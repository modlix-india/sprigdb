//! Geo commands on sorted sets: a member's score is Redis's 52-bit
//! geohash, and distances use Redis's haversine, so results match Redis.
//! Searches read the whole sorted set and keep the members inside the
//! shape (the set Redis returns after its own exact test); reading only
//! the covering geohash ranges is a later optimisation.

#![allow(clippy::similar_names)]

use bytes::Bytes;
use sprig_proto_redis::Frame;
use sprig_proto_redis::num::parse_f64;
use sprig_redis::reply;

use super::zsets::{ZEdit, cmp_pair};
use super::{Arg, Cx, Out, int, tri, upper};
use crate::store::Batch;

/// Runs a geo command; `None` when `name` isn't one.
pub fn run<B: Batch>(cx: &mut Cx<'_, B>, name: &str, a: &[Bytes]) -> Option<Out> {
    Some(match name {
        "geoadd" => geoadd(cx, a),
        "geopos" => geopos(cx, a),
        "geodist" => geodist(cx, a),
        "geohash" => geohash(cx, a),
        "geosearch" => geosearch(cx, a, false),
        "geosearchstore" => geosearch(cx, a, true),
        "georadius" | "georadius_ro" => georadius(cx, a, false, name.ends_with("_ro")),
        "georadiusbymember" | "georadiusbymember_ro" => {
            georadius(cx, a, true, name.ends_with("_ro"))
        }
        _ => return None,
    })
}

const LON_MIN: f64 = -180.0;
const LON_MAX: f64 = 180.0;
const LAT_MIN: f64 = -85.051_128_78;
const LAT_MAX: f64 = 85.051_128_78;
const STEP: u32 = 26;
/// Redis's `EARTH_RADIUS_IN_METERS`.
const EARTH_RADIUS: f64 = 6_372_797.560_856;

fn spread(x: u32) -> u64 {
    let mut x = u64::from(x);
    x = (x | (x << 16)) & 0x0000_FFFF_0000_FFFF;
    x = (x | (x << 8)) & 0x00FF_00FF_00FF_00FF;
    x = (x | (x << 4)) & 0x0F0F_0F0F_0F0F_0F0F;
    x = (x | (x << 2)) & 0x3333_3333_3333_3333;
    (x | (x << 1)) & 0x5555_5555_5555_5555
}

fn squash(x: u64) -> u32 {
    let mut x = x & 0x5555_5555_5555_5555;
    x = (x | (x >> 1)) & 0x3333_3333_3333_3333;
    x = (x | (x >> 2)) & 0x0F0F_0F0F_0F0F_0F0F;
    x = (x | (x >> 4)) & 0x00FF_00FF_00FF_00FF;
    x = (x | (x >> 8)) & 0x0000_FFFF_0000_FFFF;
    x = (x | (x >> 16)) & 0x0000_0000_FFFF_FFFF;
    u32::try_from(x).unwrap_or(u32::MAX)
}

/// The 52-bit geohash of a point over the given ranges, as Redis stores it:
/// latitude bits even, longitude bits odd.
fn encode(lon: f64, lat: f64, lat_min: f64, lat_max: f64) -> u64 {
    let scale = f64::from(1u32 << STEP);
    let lat_off = (lat - lat_min) / (lat_max - lat_min) * scale;
    let lon_off = (lon - LON_MIN) / (LON_MAX - LON_MIN) * scale;
    spread(lat_off as u32) | (spread(lon_off as u32) << 1)
}

/// The cell centre of a 52-bit hash.
fn decode(bits: u64) -> (f64, f64) {
    let ilat = f64::from(squash(bits));
    let ilon = f64::from(squash(bits >> 1));
    let scale = f64::from(1u32 << STEP);
    let lat_s = LAT_MAX - LAT_MIN;
    let lon_s = LON_MAX - LON_MIN;
    let lat_min = LAT_MIN + (ilat / scale) * lat_s;
    let lat_max = LAT_MIN + ((ilat + 1.0) / scale) * lat_s;
    let lon_min = LON_MIN + (ilon / scale) * lon_s;
    let lon_max = LON_MIN + ((ilon + 1.0) / scale) * lon_s;
    let lon = f64::midpoint(lon_min, lon_max).clamp(LON_MIN, LON_MAX);
    let lat = f64::midpoint(lat_min, lat_max).clamp(LAT_MIN, LAT_MAX);
    (lon, lat)
}

fn rad(d: f64) -> f64 {
    d * (std::f64::consts::PI / 180.0)
}

/// The haversine distance in metres, on the Earth radius Redis uses.
fn distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let (la1, lo1, la2, lo2) = (rad(lat1), rad(lon1), rad(lat2), rad(lon2));
    let u = ((la2 - la1) / 2.0).sin();
    let v = ((lo2 - lo1) / 2.0).sin();
    2.0 * EARTH_RADIUS * (u * u + la1.cos() * la2.cos() * v * v).sqrt().asin()
}

/// A distance or coordinate as Redis replies with it: 17 decimals, trailing
/// zeros dropped.
fn human(d: f64) -> Frame {
    let mut s = format!("{d:.17}");
    if s.contains('.') {
        while s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
    }
    Frame::DoubleText(Bytes::from(s))
}

fn unit(a: &[u8]) -> Arg<f64> {
    match a.to_ascii_lowercase().as_slice() {
        b"m" => Ok(1.0),
        b"km" => Ok(1000.0),
        b"ft" => Ok(0.3048),
        b"mi" => Ok(1609.34),
        _ => Err(Frame::err(
            "unsupported unit provided. please use M, KM, FT, MI",
        )),
    }
}

fn dist_frame(d: f64) -> Frame {
    Frame::bulk(format!("{d:.4}"))
}

fn lonlat(lon: &[u8], lat: &[u8]) -> Arg<(f64, f64)> {
    match (parse_f64(lon), parse_f64(lat)) {
        (Some(x), Some(y)) => Ok((x, y)),
        _ => Err(reply::not_float()),
    }
}

fn check_pair(x: f64, y: f64) -> Arg<()> {
    if !(LON_MIN..=LON_MAX).contains(&x) || !(LAT_MIN..=LAT_MAX).contains(&y) {
        return Err(Frame::err(&format!(
            "invalid longitude,latitude pair {x:.6},{y:.6}"
        )));
    }
    Ok(())
}

/// `GEOADD k [NX|XX] [CH] lon lat m ...`.
fn geoadd<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let (mut nx, mut xx, mut ch) = (false, false, false);
    let mut i = 2;
    while i < a.len() {
        match upper(&a[i]).as_str() {
            "NX" => nx = true,
            "XX" => xx = true,
            "CH" => ch = true,
            _ => break,
        }
        i += 1;
    }
    let rest = &a[i..];
    if rest.is_empty() || !rest.len().is_multiple_of(3) {
        return Ok(Frame::err(
            "syntax error. Try GEOADD key [x1] [y1] [name1] [x2] [y2] [name2] ... ",
        ));
    }
    if nx && xx {
        return Ok(reply::syntax());
    }
    let mut items = Vec::with_capacity(rest.len() / 3);
    for c in rest.chunks(3) {
        let (x, y) = tri!(lonlat(&c[0], &c[1]));
        tri!(check_pair(x, y));
        items.push((encode(x, y, LAT_MIN, LAT_MAX) as f64, c[2].clone()));
    }
    let z = tri!(cx.zset(&a[1])?);
    let mut edits = Vec::new();
    let mut seen: std::collections::HashMap<Bytes, Option<f64>> = std::collections::HashMap::new();
    for (s, m) in items {
        let old = match seen.get(&m) {
            Some(o) => *o,
            None => match &z.body {
                None => None,
                Some(b) => cx.zscore_of(b, &m)?,
            },
        };
        let go = if old.is_some() { !nx } else { !xx };
        if go {
            seen.insert(m.clone(), Some(s));
            edits.push(ZEdit::Set(s, m));
        }
    }
    let (added, changed, _) = cx.z_write(&a[1], &z, edits)?;
    Ok(Frame::Integer(if ch { added + changed } else { added }))
}

/// The scores of `members` in `key` (nothing for a missing key).
fn scores<B: Batch>(
    cx: &mut Cx<'_, B>,
    key: &[u8],
    members: &[Bytes],
) -> Result<Arg<Vec<Option<f64>>>, crate::store::StoreError> {
    let z = match cx.zset(key)? {
        Ok(z) => z,
        Err(f) => return Ok(Err(f)),
    };
    let mut out = Vec::with_capacity(members.len());
    for m in members {
        out.push(match &z.body {
            None => None,
            Some(b) => cx.zscore_of(b, m)?,
        });
    }
    Ok(Ok(out))
}

/// `GEOPOS k m...`.
fn geopos<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let s = tri!(scores(cx, &a[1], &a[2..])?);
    Ok(Frame::Array(
        s.into_iter()
            .map(|s| match s {
                None => Frame::null_array(),
                Some(s) => {
                    let (x, y) = decode(s as u64);
                    Frame::Array(vec![human(x), human(y)])
                }
            })
            .collect(),
    ))
}

/// `GEODIST k m1 m2 [unit]`.
fn geodist<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    let conv = match a.len() {
        4 => 1.0,
        5 => tri!(unit(&a[4])),
        _ => return Ok(reply::syntax()),
    };
    let s = tri!(scores(cx, &a[1], &a[2..4])?);
    let (Some(s1), Some(s2)) = (s[0], s[1]) else {
        return Ok(Frame::null());
    };
    let (x1, y1) = decode(s1 as u64);
    let (x2, y2) = decode(s2 as u64);
    Ok(dist_frame(distance(x1, y1, x2, y2) / conv))
}

/// `GEOHASH k m...`: 11 base-32 characters over latitude ±90.
fn geohash<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes]) -> Out {
    const ALPHA: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    let s = tri!(scores(cx, &a[1], &a[2..])?);
    Ok(Frame::Array(
        s.into_iter()
            .map(|s| match s {
                None => Frame::null(),
                Some(s) => {
                    let (x, y) = decode(s as u64);
                    let bits = encode(x, y, -90.0, 90.0);
                    let mut buf = Vec::with_capacity(11);
                    for j in 0..11u32 {
                        let idx = if j == 10 {
                            0
                        } else {
                            ((bits >> (52 - (j + 1) * 5)) & 0x1f) as usize
                        };
                        buf.push(ALPHA[idx]);
                    }
                    Frame::Bulk(buf.into())
                }
            })
            .collect(),
    ))
}

/// A search shape.
#[derive(Debug, Clone, Copy)]
enum Shape {
    Radius(f64),
    Box(f64, f64),
}

/// A parsed search.
struct Search {
    center: Option<(f64, f64)>,
    member: Option<Bytes>,
    shape: Option<Shape>,
    conv: f64,
    sort: Option<bool>,
    count: Option<(i64, bool)>,
    withcoord: bool,
    withdist: bool,
    withhash: bool,
    store: Option<(Bytes, bool)>,
}

fn parse_opts(a: &[Bytes], mut i: usize, s: &mut Search, geosearch: bool, ro: bool) -> Arg<()> {
    while i < a.len() {
        let left = a.len() - i - 1;
        match upper(&a[i]).as_str() {
            "WITHCOORD" => s.withcoord = true,
            "WITHDIST" => s.withdist = true,
            "WITHHASH" => s.withhash = true,
            "ASC" => s.sort = Some(true),
            "DESC" => s.sort = Some(false),
            "ANY" => match &mut s.count {
                Some((_, any)) => *any = true,
                None => {
                    return Err(Frame::err("the ANY argument requires COUNT argument"));
                }
            },
            "COUNT" if left >= 1 => {
                let n = int(&a[i + 1])?;
                if n <= 0 {
                    return Err(Frame::err("COUNT must be > 0"));
                }
                let any = i + 2 < a.len() && upper(&a[i + 2]) == "ANY";
                s.count = Some((n, any));
                i += if any { 2 } else { 1 };
            }
            "STORE" | "STOREDIST" if !geosearch && !ro && left >= 1 => {
                s.store = Some((a[i + 1].clone(), upper(&a[i]) == "STOREDIST"));
                i += 1;
            }
            "STOREDIST" if geosearch && !ro => {
                if s.store.is_none() {
                    return Err(reply::syntax());
                }
                if let Some(st) = &mut s.store {
                    st.1 = true;
                }
            }
            "FROMMEMBER" if geosearch && left >= 1 => {
                if s.center.is_some() || s.member.is_some() {
                    return Err(reply::syntax());
                }
                s.member = Some(a[i + 1].clone());
                i += 1;
            }
            "FROMLONLAT" if geosearch && left >= 2 => {
                if s.center.is_some() || s.member.is_some() {
                    return Err(reply::syntax());
                }
                let (x, y) = lonlat(&a[i + 1], &a[i + 2])?;
                check_pair(x, y)?;
                s.center = Some((x, y));
                i += 2;
            }
            "BYRADIUS" if geosearch && left >= 2 => {
                if s.shape.is_some() {
                    return Err(reply::syntax());
                }
                let Some(r) = parse_f64(&a[i + 1]).filter(|r| *r >= 0.0) else {
                    return Err(Frame::err("radius cannot be negative"));
                };
                s.conv = unit(&a[i + 2])?;
                s.shape = Some(Shape::Radius(r));
                i += 2;
            }
            "BYBOX" if geosearch && left >= 3 => {
                if s.shape.is_some() {
                    return Err(reply::syntax());
                }
                let (Some(w), Some(h)) = (parse_f64(&a[i + 1]), parse_f64(&a[i + 2])) else {
                    return Err(reply::not_float());
                };
                if w < 0.0 || h < 0.0 {
                    return Err(Frame::err("height or width cannot be negative"));
                }
                s.conv = unit(&a[i + 3])?;
                s.shape = Some(Shape::Box(w, h));
                i += 3;
            }
            _ => return Err(reply::syntax()),
        }
        i += 1;
    }
    if geosearch {
        if s.center.is_none() && s.member.is_none() {
            return Err(Frame::err(
                "exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH",
            ));
        }
        if s.shape.is_none() {
            return Err(Frame::err(
                "exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH",
            ));
        }
    }
    if s.store.is_some() && (s.withcoord || s.withdist || s.withhash) {
        return Err(Frame::err(if geosearch {
            "GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options"
        } else {
            "STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options"
        }));
    }
    Ok(())
}

/// Runs a parsed search on `src`.
fn run_search<B: Batch>(cx: &mut Cx<'_, B>, src: &Bytes, mut s: Search) -> Out {
    let z = tri!(cx.zset(src)?);
    let Some(body) = z.body else {
        if let Some((dst, _)) = &s.store {
            let raw = cx.raw(dst)?;
            if raw.is_some() {
                cx.write(dst, raw.as_ref(), None)?;
            }
            return Ok(Frame::Integer(0));
        }
        return Ok(Frame::Array(Vec::new()));
    };
    let all = cx.z_all(&body, false)?;
    if let Some(m) = &s.member {
        match all.iter().find(|(_, x)| x == m) {
            Some((sc, _)) => s.center = Some(decode(*sc as u64)),
            None => {
                return Ok(Frame::err("could not decode requested zset member"));
            }
        }
    }
    let (cx0, cy0) = s.center.unwrap_or((0.0, 0.0));
    let shape = s.shape.unwrap_or(Shape::Radius(0.0));
    if s.count.is_some_and(|(_, any)| !any) && s.sort.is_none() {
        s.sort = Some(true);
    }
    let mut hits = Vec::new();
    for (sc, m) in all {
        let (x, y) = decode(sc as u64);
        let d = match shape {
            Shape::Radius(r) => {
                let d = distance(cx0, cy0, x, y);
                if d > r * s.conv {
                    continue;
                }
                d
            }
            Shape::Box(w, h) => {
                let lat_d = EARTH_RADIUS * (rad(cy0) - rad(y)).abs();
                if lat_d > h * s.conv / 2.0 {
                    continue;
                }
                if distance(x, y, cx0, y) > w * s.conv / 2.0 {
                    continue;
                }
                distance(cx0, cy0, x, y)
            }
        };
        hits.push((m, sc, d, x, y));
    }
    match s.sort {
        Some(true) => hits.sort_by(|a, b| a.2.total_cmp(&b.2)),
        Some(false) => hits.sort_by(|a, b| b.2.total_cmp(&a.2)),
        None => {}
    }
    if let Some((n, _)) = s.count {
        hits.truncate(usize::try_from(n).unwrap_or(usize::MAX));
    }
    if let Some((dst, storedist)) = &s.store {
        let mut items: Vec<(f64, Bytes)> = hits
            .iter()
            .map(|(m, sc, d, _, _)| (if *storedist { d / s.conv } else { *sc }, m.clone()))
            .collect();
        items.sort_by(cmp_pair);
        return Ok(Frame::Integer(cx.z_store(dst, items)?));
    }
    let plain = !(s.withcoord || s.withdist || s.withhash);
    Ok(Frame::Array(
        hits.into_iter()
            .map(|(m, sc, d, x, y)| {
                if plain {
                    return Frame::Bulk(m);
                }
                let mut item = vec![Frame::Bulk(m)];
                if s.withdist {
                    item.push(dist_frame(d / s.conv));
                }
                if s.withhash {
                    item.push(Frame::Integer(sc as i64));
                }
                if s.withcoord {
                    item.push(Frame::Array(vec![human(x), human(y)]));
                }
                Frame::Array(item)
            })
            .collect(),
    ))
}

fn empty_search() -> Search {
    Search {
        center: None,
        member: None,
        shape: None,
        conv: 1.0,
        sort: None,
        count: None,
        withcoord: false,
        withdist: false,
        withhash: false,
        store: None,
    }
}

/// `GEOSEARCH k ...` and `GEOSEARCHSTORE dst src ...`.
fn geosearch<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes], store: bool) -> Out {
    let mut s = empty_search();
    let (src, start) = if store {
        s.store = Some((a[1].clone(), false));
        (a[2].clone(), 3)
    } else {
        (a[1].clone(), 2)
    };
    tri!(parse_opts(a, start, &mut s, true, false));
    run_search(cx, &src, s)
}

/// `GEORADIUS k lon lat r unit ...`, `GEORADIUSBYMEMBER k m r unit ...`
/// (and their `_RO` forms).
fn georadius<B: Batch>(cx: &mut Cx<'_, B>, a: &[Bytes], by_member: bool, ro: bool) -> Out {
    let mut s = empty_search();
    let at = if by_member {
        s.member = Some(a[2].clone());
        3
    } else {
        let (x, y) = tri!(lonlat(&a[2], &a[3]));
        tri!(check_pair(x, y));
        s.center = Some((x, y));
        4
    };
    if a.len() < at + 2 {
        return Ok(reply::syntax());
    }
    let Some(r) = parse_f64(&a[at]).filter(|r| *r >= 0.0) else {
        return Ok(Frame::err("radius cannot be negative"));
    };
    s.conv = tri!(unit(&a[at + 1]));
    s.shape = Some(Shape::Radius(r));
    tri!(parse_opts(a, at + 2, &mut s, false, ro));
    run_search(cx, &a[1], s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn geohash_round_trip_matches_redis() {
        // Redis: GEOADD Sicily 13.361389 38.115556 "Palermo" stores score
        // 3479099956230698; GEOPOS gives 13.36138933897018433 38.11555639549629859.
        let h = encode(13.361_389, 38.115_556, LAT_MIN, LAT_MAX);
        assert_eq!(h, 3_479_099_956_230_698);
        let (x, y) = decode(h);
        assert_eq!(format!("{x:.17}"), "13.36138933897018433");
        assert_eq!(format!("{y:.17}"), "38.11555639549629859");
        let c = encode(15.087_269, 37.502_669, LAT_MIN, LAT_MAX);
        let (x2, y2) = decode(c);
        // GEODIST Sicily Palermo Catania = 166274.1516 m.
        assert_eq!(format!("{:.4}", distance(x, y, x2, y2)), "166274.1516");
    }
}
