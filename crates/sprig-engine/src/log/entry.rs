//! The log entry raft-engine stores.
//!
//! raft-engine stores entries as protobuf-2 messages. `LogEntry` writes the
//! same wire format as raft-rs's `eraftpb::Entry` (term = field 2, index =
//! field 3, data = field 4), so when replication arrives the same log files
//! can hold raft-rs entries without a format change. Only the wire methods
//! are implemented; raft-engine never uses reflection.

use std::sync::LazyLock;

use protobuf::wire_format::WireType;
use protobuf::{
    CachedSize, Clear, CodedInputStream, CodedOutputStream, Message, ProtobufResult, UnknownFields,
};
use raft_engine::MessageExt;

/// One committed proposal.
#[derive(Debug, Clone, Default)]
pub struct LogEntry {
    /// The Raft term (always 1 with one voter).
    pub term: u64,
    /// The entry's index in its unit's log.
    pub index: u64,
    /// The payload ([`super::Payload`]).
    pub data: Vec<u8>,
    unknown: UnknownFields,
    cached: CachedSize,
}

impl PartialEq for LogEntry {
    fn eq(&self, o: &Self) -> bool {
        self.term == o.term && self.index == o.index && self.data == o.data
    }
}

impl LogEntry {
    /// An entry.
    #[must_use]
    pub fn new(term: u64, index: u64, data: Vec<u8>) -> Self {
        Self {
            term,
            index,
            data,
            ..Self::default()
        }
    }
}

impl Clear for LogEntry {
    fn clear(&mut self) {
        self.term = 0;
        self.index = 0;
        self.data.clear();
        self.unknown.clear();
    }
}

static DEFAULT: LazyLock<LogEntry> = LazyLock::new(LogEntry::default);

impl Message for LogEntry {
    fn descriptor(&self) -> &'static protobuf::reflect::MessageDescriptor {
        // raft-engine only encodes and decodes entries; nothing reflects on
        // them. A call here is a programming error.
        unreachable!("LogEntry has no protobuf descriptor")
    }

    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut CodedInputStream<'_>) -> ProtobufResult<()> {
        while !is.eof()? {
            let (field, wt) = is.read_tag_unpack()?;
            match field {
                2 | 3 => {
                    if wt != WireType::WireTypeVarint {
                        return Err(protobuf::rt::unexpected_wire_type(wt));
                    }
                    let v = is.read_uint64()?;
                    if field == 2 {
                        self.term = v;
                    } else {
                        self.index = v;
                    }
                }
                4 => {
                    if wt != WireType::WireTypeLengthDelimited {
                        return Err(protobuf::rt::unexpected_wire_type(wt));
                    }
                    self.data = is.read_bytes()?;
                }
                _ => protobuf::rt::read_unknown_or_skip_group(field, wt, is, &mut self.unknown)?,
            }
        }
        Ok(())
    }

    fn write_to_with_cached_sizes(&self, os: &mut CodedOutputStream<'_>) -> ProtobufResult<()> {
        if self.term != 0 {
            os.write_uint64(2, self.term)?;
        }
        if self.index != 0 {
            os.write_uint64(3, self.index)?;
        }
        if !self.data.is_empty() {
            os.write_bytes(4, &self.data)?;
        }
        os.write_unknown_fields(&self.unknown)?;
        Ok(())
    }

    fn compute_size(&self) -> u32 {
        let mut n = 0;
        if self.term != 0 {
            n += protobuf::rt::value_size(2, self.term, WireType::WireTypeVarint);
        }
        if self.index != 0 {
            n += protobuf::rt::value_size(3, self.index, WireType::WireTypeVarint);
        }
        if !self.data.is_empty() {
            n += protobuf::rt::bytes_size(4, &self.data);
        }
        n += protobuf::rt::unknown_fields_size(&self.unknown);
        self.cached.set(n);
        n
    }

    fn get_cached_size(&self) -> u32 {
        self.cached.get()
    }

    fn get_unknown_fields(&self) -> &UnknownFields {
        &self.unknown
    }

    fn mut_unknown_fields(&mut self) -> &mut UnknownFields {
        &mut self.unknown
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn new() -> Self {
        Self::default()
    }

    fn default_instance() -> &'static Self {
        &DEFAULT
    }
}

/// raft-engine's handle on the entry type.
#[derive(Debug, Clone, Copy)]
pub struct EntryExt;

impl MessageExt for EntryExt {
    type Entry = LogEntry;

    fn index(e: &LogEntry) -> u64 {
        e.index
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let e = LogEntry::new(1, 42, b"payload".to_vec());
        let b = e.write_to_bytes().unwrap();
        let d = LogEntry::parse_from_bytes(&b).unwrap();
        assert_eq!(d, e);
        // eraftpb::Entry's encoding of term 1, index 42, data "payload".
        assert_eq!(b, [&[0x10, 1, 0x18, 42, 0x22, 7][..], b"payload"].concat());
    }
}
