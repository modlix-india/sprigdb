//! [`WireSink`]: the [`ResultSink`] that encodes a response for the
//! client's capabilities onto its `Framed` connection.
//!
//! Packets are fed into the codec's write buffer; the buffer is flushed to
//! the socket whenever it passes the backpressure boundary (the connection
//! is built with `gateway.relay_buffer_kb`), and every feed waits until it
//! has, so at most that much waits for a slow client.

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt as _};
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::buf::put_lenenc_int;
use sprig_proto_mysql::packets::{EofPacket, OkPacket};
use sprig_proto_mysql::resultset::{
    Terminator, encode_binary_row_into, encode_column_count, encode_text_row_into,
};
use sprig_proto_mysql::{Caps, ColumnDef, ErrPacket, PacketCodec, Persona, StatusFlags};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

use crate::pipeline::{EndOfRows, ResultSink, RowFormat, RowValues, Wire};

/// Flushes a client connection's write buffer.
pub(crate) async fn flush<S>(framed: &mut Framed<S, PacketCodec>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    <Framed<S, PacketCodec> as SinkExt<Bytes>>::flush(framed)
        .await
        .map_err(SprigError::from)
}

/// Puts one packet whose payload `f` writes straight into the connection's
/// write buffer: no payload allocation or copy while the buffer is below its
/// backpressure boundary and framing is plain (the common case). Otherwise,
/// and for payloads that need splitting, the payload goes through the codec
/// as before.
async fn put<S>(
    framed: &mut Framed<S, PacketCodec>,
    f: impl FnOnce(&mut BytesMut) -> Result<()> + Send,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let plain = !framed.codec().framing().is_compressed();
    if plain && framed.write_buffer().len() < framed.backpressure_boundary() {
        let seq = framed.codec().next_seq();
        let buf = framed.write_buffer_mut();
        let start = buf.len();
        buf.extend_from_slice(&[0, 0, 0, seq]);
        if let Err(e) = f(buf) {
            buf.truncate(start);
            return Err(e);
        }
        let len = buf.len() - start - 4;
        if len < 0xFF_FFFF {
            let l = u32::try_from(len).unwrap_or(0).to_le_bytes();
            buf[start..start + 3].copy_from_slice(&l[..3]);
            framed.codec_mut().set_next_seq(seq.wrapping_add(1));
            return Ok(());
        }
        let payload = buf.split_off(start + 4).freeze();
        buf.truncate(start);
        return framed.feed(payload).await.map_err(SprigError::from);
    }
    let mut b = BytesMut::new();
    f(&mut b)?;
    framed.feed(b.freeze()).await.map_err(SprigError::from)
}

/// Where a response is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Nothing sent, or between results.
    Idle,
    /// Rows of a resultset.
    Rows(RowFormat),
    /// Waiting for the client's `LOCAL INFILE` packets.
    Infile,
    /// The final packet went out.
    Finished,
}

/// The column definitions a client last received for one prepared
/// statement (`MARIADB_CLIENT_CACHE_METADATA`): those of its `PREPARE_OK`,
/// then of each execute that sent them.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SentMeta {
    /// Definitions the gateway encoded.
    Typed(Vec<ColumnDef>),
    /// Definitions relayed as the backend sent them.
    Raw(Vec<Bytes>),
}

/// The sink of one request.
pub(crate) struct WireSink<'a, S> {
    framed: &'a mut Framed<S, PacketCodec>,
    persona: Persona,
    caps: Caps,
    columns: Vec<ColumnDef>,
    state: State,
    /// The status of the last OK, EOF or terminator sent.
    pub(crate) last_status: Option<StatusFlags>,
    /// A `COM_STMT_EXECUTE` under `MARIADB_CLIENT_CACHE_METADATA`: what the
    /// client already has for the statement. Binary resultsets then send
    /// the definitions only when they changed, after a one-byte flag.
    pub(crate) meta_cache: Option<&'a mut Option<SentMeta>>,
    /// A resultset of this request already began: only an execute's first
    /// resultset may leave its definitions out.
    began: bool,
}

/// A column count packet with the metadata flag
/// (`MARIADB_CLIENT_CACHE_METADATA`).
fn count_with_flag(n: usize, follows: bool) -> Bytes {
    let c = encode_column_count(n as u64);
    let mut b = Vec::with_capacity(c.len() + 1);
    b.extend_from_slice(&c);
    b.push(u8::from(follows));
    Bytes::from(b)
}

impl<'a, S> WireSink<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub(crate) fn new(
        framed: &'a mut Framed<S, PacketCodec>,
        persona: Persona,
        caps: Caps,
    ) -> Self {
        Self {
            framed,
            persona,
            caps,
            columns: Vec::new(),
            state: State::Idle,
            last_status: None,
            meta_cache: None,
            began: false,
        }
    }

    /// Under `MARIADB_CLIENT_CACHE_METADATA`, whether the definitions must
    /// follow the count (and records them as sent); `None` without the
    /// capability. `now` builds the definitions about to be sent; it is
    /// called only when there is a statement's slot to compare them with.
    /// Only the first resultset of an execute may leave them out: a later
    /// one (a procedure's second resultset) always carries its own.
    fn cached(&mut self, format: RowFormat, now: impl FnOnce() -> SentMeta) -> Option<bool> {
        let first = !std::mem::replace(&mut self.began, true);
        if !self.caps.contains(Caps::MARIADB_CLIENT_CACHE_METADATA) {
            return None;
        }
        if format == RowFormat::Text {
            return Some(true);
        }
        let Some(slot) = self.meta_cache.as_deref_mut() else {
            return Some(true);
        };
        let now = now();
        if first && slot.as_ref() == Some(&now) {
            return Some(false);
        }
        *slot = Some(now);
        Some(true)
    }

    /// The EOF that ends a resultset's column definitions for a client
    /// without `DEPRECATE_EOF`. MariaDB sends it even when the definitions
    /// themselves were left out (`MARIADB_CLIENT_CACHE_METADATA`).
    async fn end_of_definitions(&mut self) -> Result<()> {
        if self.caps.contains(Caps::DEPRECATE_EOF) {
            return Ok(());
        }
        let eof = EofPacket {
            warnings: 0,
            status: StatusFlags::AUTOCOMMIT,
        };
        self.feed(eof.encode(self.caps)).await
    }

    async fn feed(&mut self, payload: Bytes) -> Result<()> {
        self.framed.feed(payload).await.map_err(SprigError::from)
    }

    fn finish_with(&mut self, status: StatusFlags) {
        self.last_status = Some(status);
        self.state = if status.contains(StatusFlags::MORE_RESULTS_EXISTS) {
            State::Idle
        } else {
            State::Finished
        };
    }
}

#[async_trait]
impl<S> ResultSink for WireSink<'_, S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    fn wire(&self) -> Wire {
        Wire {
            persona: self.persona,
            caps: self.caps,
        }
    }

    async fn begin_rows(&mut self, columns: &[ColumnDef], format: RowFormat) -> Result<()> {
        let follows = self.cached(format, || SentMeta::Typed(columns.to_vec()));
        let n = columns.len() as u64;
        put(self.framed, |b| {
            put_lenenc_int(b, n);
            if let Some(f) = follows {
                b.extend_from_slice(&[u8::from(f)]);
            }
            Ok(())
        })
        .await?;
        if follows != Some(false) {
            let caps = self.caps;
            for c in columns {
                put(self.framed, |b| {
                    c.encode_into(caps, b);
                    Ok(())
                })
                .await?;
            }
        }
        self.end_of_definitions().await?;
        self.columns.clear();
        self.columns.extend_from_slice(columns);
        self.state = State::Rows(format);
        Ok(())
    }

    async fn begin_rows_raw(&mut self, defs: &[Bytes], format: RowFormat) -> Result<()> {
        let follows = self.cached(format, || SentMeta::Raw(defs.to_vec()));
        match follows {
            None => self.feed(encode_column_count(defs.len() as u64)).await?,
            Some(f) => self.feed(count_with_flag(defs.len(), f)).await?,
        }
        if follows != Some(false) {
            for d in defs {
                self.feed(d.clone()).await?;
            }
        }
        self.end_of_definitions().await?;
        // Rows are relayed raw: nothing to encode them with.
        self.columns.clear();
        self.state = State::Rows(format);
        Ok(())
    }

    async fn cursor_opened(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()> {
        match self.cached(RowFormat::Binary, || SentMeta::Typed(columns.to_vec())) {
            None => self.feed(encode_column_count(columns.len() as u64)).await?,
            Some(f) => {
                self.feed(count_with_flag(columns.len(), f)).await?;
                if f {
                    for c in columns {
                        self.feed(c.encode(self.caps)).await?;
                    }
                }
            }
        }
        if !self.caps.contains(Caps::MARIADB_CLIENT_CACHE_METADATA) {
            for c in columns {
                self.feed(c.encode(self.caps)).await?;
            }
        }
        // The one packet after the definitions carries CURSOR_EXISTS: an
        // EOF, or under DEPRECATE_EOF the OK-form terminator.
        let t = if self.caps.contains(Caps::DEPRECATE_EOF) {
            Terminator::Ok(OkPacket {
                header: 0xFE,
                status: end.status,
                warnings: end.warnings,
                ..OkPacket::default()
            })
        } else {
            Terminator::Eof(EofPacket {
                warnings: end.warnings,
                status: end.status,
            })
        };
        self.feed(t.encode(self.caps)).await?;
        self.finish_with(end.status);
        Ok(())
    }

    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        let columns = &self.columns;
        put(self.framed, |b| match values {
            RowValues::Text(v) => {
                encode_text_row_into(v.iter().copied(), b);
                Ok(())
            }
            RowValues::Binary(v) => encode_binary_row_into(v, columns, b).map_err(SprigError::from),
        })
        .await
    }

    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        self.feed(payload).await
    }

    fn try_raw_row(&mut self, payload: Bytes) -> std::result::Result<Result<()>, Bytes> {
        use futures::Sink as _;
        // `feed` without its wait: below the backpressure boundary the
        // framed writer is ready, and the row only goes into its buffer.
        if self.framed.write_buffer().len() >= self.framed.backpressure_boundary() {
            return Err(payload);
        }
        Ok(std::pin::Pin::new(&mut *self.framed)
            .start_send(payload)
            .map_err(SprigError::from))
    }

    async fn end_rows(&mut self, end: EndOfRows) -> Result<()> {
        if self.caps.contains(Caps::DEPRECATE_EOF) {
            let ok = OkPacket {
                header: 0xFE,
                status: end.status,
                warnings: end.warnings,
                ..OkPacket::default()
            };
            let caps = self.caps;
            put(self.framed, |b| {
                ok.encode_into(caps, b);
                Ok(())
            })
            .await?;
            self.finish_with(end.status);
            return Ok(());
        }
        let t = Terminator::Ok(OkPacket {
            header: 0xFE,
            status: end.status,
            warnings: end.warnings,
            ..OkPacket::default()
        });
        self.feed(t.encode(self.caps)).await?;
        self.finish_with(end.status);
        Ok(())
    }

    async fn ok(&mut self, ok: OkPacket) -> Result<()> {
        let status = ok.status;
        let ok = OkPacket { header: 0, ..ok };
        let caps = self.caps;
        put(self.framed, |b| {
            ok.encode_into(caps, b);
            Ok(())
        })
        .await?;
        self.finish_with(status);
        Ok(())
    }

    async fn error(&mut self, err: &SprigError) -> Result<()> {
        // 3306: MariaDB-only errors as MySQL 8.4's.
        let mapped = sprig_sql::compat::errmap::for_persona(self.persona, err);
        let p = ErrPacket::from_error(self.persona, mapped.as_ref().unwrap_or(err));
        self.feed(p.encode(self.caps)).await?;
        self.state = State::Finished;
        Ok(())
    }

    async fn column_list(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()> {
        for c in columns {
            self.feed(c.encode(self.caps)).await?;
        }
        self.state = State::Rows(RowFormat::Text);
        self.end_rows(end).await
    }

    async fn local_infile_request(&mut self, file_name: &[u8]) -> Result<()> {
        let mut p = Vec::with_capacity(1 + file_name.len());
        p.push(0xFB);
        p.extend_from_slice(file_name);
        self.feed(p.into()).await?;
        flush(self.framed).await?;
        self.state = State::Infile;
        Ok(())
    }

    async fn local_infile_read(&mut self) -> Result<Bytes> {
        if self.state != State::Infile {
            return Err(SprigError::internal("LOCAL INFILE read without a request"));
        }
        match self.framed.next().await {
            Some(Ok(p)) => {
                if p.payload.is_empty() {
                    self.state = State::Idle;
                }
                Ok(p.payload)
            }
            Some(Err(e)) => Err(e.into()),
            None => Err(SprigError::standard(
                1158,
                *b"08S01",
                "Got an error reading communication packets",
            )),
        }
    }

    async fn flush(&mut self) -> Result<()> {
        flush(self.framed).await
    }

    fn finished(&self) -> bool {
        self.state == State::Finished
    }
}
