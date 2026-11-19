// Records: what a producer sends, what a consumer gets back, and record
// batches (message format v2), with message sets v0/v1 decoded for topics
// that still hold them.
//
// A batch is:
//
//     base_offset int64, length int32 (the bytes after it),
//     partition_leader_epoch int32, magic int8 (2), crc uint32 (CRC-32C of
//     everything after it), attributes int16, last_offset_delta int32,
//     base_timestamp int64, max_timestamp int64, producer_id int64,
//     producer_epoch int16, base_sequence int32, count int32, records
//
// and a record is a varint length, then attributes int8, timestamp delta
// varlong, offset delta varint, key and value (varint length, -1 for
// null), and headers (a varint count, then key and value each). When the
// batch is compressed, everything after count is compressed as one block.

pub let CODEC_NONE = 0;
pub let CODEC_GZIP = 1;
pub let CODEC_SNAPPY = 2;
pub let CODEC_LZ4 = 3;
pub let CODEC_ZSTD = 4;

let ATTR_CODEC = 7;
let ATTR_LOG_APPEND_TIME = 8;
let ATTR_TRANSACTIONAL = 16;
let ATTR_CONTROL = 32;

// The batch header's size up to and including count, and the offset of
// attributes (where the CRC starts) within it.
let BATCH_HEADER = 61;
let CRC_FROM = 21;

pub gc struct Header {
    key: str,
    // none is a null value, which Kafka keeps distinct from empty
    value: opt[bytes],
}

// A record to produce. partition -1 lets the partitioner choose; a
// timestamp of -1 is the time the producer batches it (ms since the epoch).
pub gc struct Record {
    topic: str,
    partition: int,
    // none: no key (spread across partitions); a key always goes to the
    // partition its hash picks
    key: opt[bytes],
    // none is a tombstone: it deletes the key from a compacted topic
    value: opt[bytes],
    headers: [Header],
    timestamp: int,
}

// A keyed record.
pub fn record(topic: str, key: bytes, value: bytes) -> Record {
    return Record { topic: topic, partition: -1, key: some(key), value: some(value), headers: [], timestamp: -1 };
}

// A record without a key.
pub fn value_record(topic: str, value: bytes) -> Record {
    return Record { topic: topic, partition: -1, key: none, value: some(value), headers: [], timestamp: -1 };
}

pub fn add_header(r: Record, key: str, value: bytes) {
    push(r.headers, Header { key: key, value: some(value) });
}

// A consumed record.
pub gc struct Message {
    topic: str,
    partition: int,
    offset: int,
    // ms since the epoch: the producer's time, or the broker's when the
    // topic uses LogAppendTime (log_append_time)
    timestamp: int,
    log_append_time: bool,
    key: opt[bytes],
    value: opt[bytes],
    headers: [Header],
    // the leader epoch the batch was written in (-1 when unknown)
    leader_epoch: int,
}

// The first header with this key.
pub fn header(m: Message, key: str) -> opt[bytes] {
    for h in m.headers {
        if h.key == key {
            return h.value;
        }
    }
    return none;
}

// ---- encoding ----------------------------------------------------------------

// What a batch says about itself, beyond its records.
pub gc struct BatchSpec {
    base_offset: int,
    leader_epoch: int,
    // -1: the first record's timestamp
    base_timestamp: int,
    // -1: the largest record timestamp
    max_timestamp: int,
    producer_id: int,
    producer_epoch: int,
    base_sequence: int,
    transactional: bool,
    log_append_time: bool,
    control: bool,
    codec: int,
}

// A producer's batch: offsets from 0, no producer id.
pub fn batch_spec() -> BatchSpec {
    return BatchSpec { base_offset: 0, leader_epoch: -1, base_timestamp: -1, max_timestamp: -1, producer_id: -1,
                       producer_epoch: -1, base_sequence: -1, transactional: false, log_append_time: false,
                       control: false, codec: CODEC_NONE };
}

pub fn uvarint_size(v: int) -> int {
    let u = v as u64;
    let n = 1;
    while u >= 128 {
        u = u >> 7;
        n += 1;
    }
    return n;
}

fn varint_size(v: int) -> int {
    return uvarint_size(((v << 1) ^ (v >> 31)) & 0xffffffff);
}

fn varlong_size(v: int) -> int {
    return uvarint_size((v << 1) ^ (v >> 63));
}

fn nfield_size(b: opt[bytes]) -> int {
    if let v = b {
        return varint_size(len(v)) + len(v);
    }
    return 1;
}

fn wr_nfield(w: Buf, b: opt[bytes]) {
    if let v = b {
        wr_varint(w, len(v));
        wr_raw(w, v);
    } else {
        wr_varint(w, -1);
    }
}

// The records of recs[from..to], uncompressed, offsets counted from 0.
fn encode_records(w: Buf, recs: [Record], from: int, to: int, base_ts: int) {
    for i in from..to {
        let rec = recs[i];
        let ts_delta = rec.timestamp - base_ts;
        let delta = i - from;
        let size = 1 + varlong_size(ts_delta) + varint_size(delta) + nfield_size(rec.key) + nfield_size(rec.value) +
                   varint_size(len(rec.headers));
        for h in rec.headers {
            size += varint_size(len(h.key)) + len(h.key) + nfield_size(h.value);
        }
        wr_varint(w, size);
        wr_u8(w, 0);
        wr_varlong(w, ts_delta);
        wr_varint(w, delta);
        wr_nfield(w, rec.key);
        wr_nfield(w, rec.value);
        wr_varint(w, len(rec.headers));
        for h in rec.headers {
            wr_varint(w, len(h.key));
            wr_text(w, h.key);
            wr_nfield(w, h.value);
        }
    }
}

// A batch of recs[from..to]. Every record needs a timestamp (not -1)
// by now; a producer fills them in when it accepts the record.
pub fn encode_batch(t: Crc, s: BatchSpec, recs: [Record], from: int, to: int) -> result[Buf, str] {
    let count = to - from;
    if count <= 0 {
        return err("kafka: a batch needs at least one record");
    }
    let base_ts = s.base_timestamp;
    if base_ts < 0 {
        base_ts = recs[from].timestamp;
    }
    let max_ts = s.max_timestamp;
    if max_ts < 0 {
        max_ts = base_ts;
        for i in from..to {
            if recs[i].timestamp > max_ts {
                max_ts = recs[i].timestamp;
            }
        }
    }
    let body = new_buf();
    encode_records(body, recs, from, to, base_ts);
    let attrs = s.codec & ATTR_CODEC;
    if s.log_append_time {
        attrs = attrs | ATTR_LOG_APPEND_TIME;
    }
    if s.transactional {
        attrs = attrs | ATTR_TRANSACTIONAL;
    }
    if s.control {
        attrs = attrs | ATTR_CONTROL;
    }
    let tail = new_buf();
    wr_i16(tail, attrs);
    wr_i32(tail, count - 1);
    wr_i64(tail, base_ts);
    wr_i64(tail, max_ts);
    wr_i64(tail, s.producer_id);
    wr_i16(tail, s.producer_epoch);
    wr_i32(tail, s.base_sequence);
    wr_i32(tail, count);
    if s.codec == CODEC_NONE {
        wr_buf(tail, body);
    } else {
        let cr = compress(s.codec, body.finish());
        guard let packed = cr else let e = err_of(cr) { return err(e); }
        wr_raw(tail, packed);
    }
    let crc = 0;
    for p in tail.pieces() {
        crc = crc32c_update(t, crc, p, 0, len(p));
    }
    let out = new_buf();
    wr_i64(out, s.base_offset);
    wr_i32(out, tail.size() + 9);
    wr_i32(out, s.leader_epoch);
    wr_u8(out, 2);
    wr_i32(out, crc);
    wr_buf(out, tail);
    return ok(out);
}

// ---- decoding ----------------------------------------------------------------

// A batch's header, and where its records are: src[from..to], compressed
// when codec() is not CODEC_NONE. A v0/v1 message is a batch of magic 0
// or 1 whose src[from..to] is the message from its attributes on.
pub gc struct Batch {
    base_offset: int,
    length: int,
    leader_epoch: int,
    magic: int,
    crc: int,
    attributes: int,
    last_offset_delta: int,
    base_timestamp: int,
    max_timestamp: int,
    producer_id: int,
    producer_epoch: int,
    base_sequence: int,
    count: int,
    src: bytes,
    from: int,
    to: int,
}

impl Batch {
    pub fn codec(self: Batch) -> int {
        return self.attributes & ATTR_CODEC;
    }

    pub fn is_transactional(self: Batch) -> bool {
        return (self.attributes & ATTR_TRANSACTIONAL) != 0;
    }

    pub fn is_control(self: Batch) -> bool {
        return self.magic >= 2 && (self.attributes & ATTR_CONTROL) != 0;
    }

    pub fn log_append_time(self: Batch) -> bool {
        return (self.attributes & ATTR_LOG_APPEND_TIME) != 0;
    }

    pub fn last_offset(self: Batch) -> int {
        return self.base_offset + self.last_offset_delta;
    }

    // The offset after this batch.
    pub fn next_offset(self: Batch) -> int {
        return self.base_offset + self.last_offset_delta + 1;
    }
}

// The batch at r's position, or none when only part of one is left (a
// fetch response may end in the middle of a batch: the next fetch starts
// there). verify checks the CRC.
pub fn read_batch(r: Reader, t: Crc, verify: bool) -> result[opt[Batch], str] {
    if r.failed != "" {
        return err(r.failed);
    }
    let start = r.pos;
    if r.end - start < 17 {
        return ok(none);
    }
    let base = rd_i64(r);
    let length = rd_i32(r);
    if length < 0 {
        return err("kafka: a batch with a length of " + to_str(length));
    }
    if r.end - r.pos < length {
        r.pos = start;
        return ok(none);
    }
    let end = r.pos + length;
    let magic = r.b[start + 16];
    if magic < 2 {
        return legacy_batch(r, t, verify, start, base, length, magic);
    }
    if magic != 2 {
        return err("kafka: a batch with magic " + to_str(magic));
    }
    if length < BATCH_HEADER - 12 {
        return err("kafka: a batch of " + to_str(length) + " bytes, shorter than its header");
    }
    let epoch = rd_i32(r);
    rd_u8(r);
    let crc = rd_u32(r);
    if verify {
        let got = crc32c_update(t, 0, r.b, start + CRC_FROM, end);
        if got != crc {
            return err("kafka: a batch at offset " + to_str(base) + " fails its CRC (corrupt in transit or on disk)");
        }
    }
    let b = Batch { base_offset: base, length: length, leader_epoch: epoch, magic: 2, crc: crc,
                    attributes: rd_i16(r), last_offset_delta: rd_i32(r), base_timestamp: rd_i64(r),
                    max_timestamp: rd_i64(r), producer_id: rd_i64(r), producer_epoch: rd_i16(r),
                    base_sequence: rd_i32(r), count: rd_i32(r), src: r.b, from: start + BATCH_HEADER, to: end };
    if b.count < 0 || b.last_offset_delta < 0 {
        return err("kafka: a batch with a record count of " + to_str(b.count));
    }
    r.pos = end;
    return ok(some(b));
}

// A v0/v1 message: offset, size, crc, magic, attributes, [timestamp],
// key, value. size is the bytes after it.
fn legacy_batch(r: Reader, t: Crc, verify: bool, start: int, base: int, size: int, magic: int)
                -> result[opt[Batch], str] {
    let end = start + 12 + size;
    let min = 14;
    if magic == 1 {
        min = 22;
    }
    if size < min {
        return err("kafka: a v" + to_str(magic) + " message of " + to_str(size) + " bytes");
    }
    let crc = rd_u32(r);
    if verify {
        let got = crc32_ieee(t, r.b, start + 16, end);
        if got != crc {
            return err("kafka: a message at offset " + to_str(base) + " fails its CRC");
        }
    }
    rd_u8(r);
    let attrs = rd_u8(r);
    let ts = -1;
    if magic == 1 {
        ts = rd_i64(r);
    }
    let b = Batch { base_offset: base, length: size, leader_epoch: -1, magic: magic, crc: crc, attributes: attrs,
                    last_offset_delta: 0, base_timestamp: ts, max_timestamp: ts, producer_id: -1,
                    producer_epoch: -1, base_sequence: -1, count: 1, src: r.b, from: r.pos, to: end };
    r.pos = end;
    return ok(some(b));
}

// The records of a batch, decompressed (up to max_bytes) and decoded. A
// control batch's records are markers, not data: see control_type.
pub fn batch_messages(t: Crc, b: Batch, topic: str, partition: int, max_bytes: int) -> result[[Message], str] {
    let out: [Message] = [];
    if b.magic < 2 {
        let lr = legacy_messages(t, b, topic, partition, max_bytes, out, 0);
        guard let _l = lr else let e = err_of(lr) { return err(e); }
        return ok(out);
    }
    let r = reader(b.src, b.from, b.to);
    if b.codec() != CODEC_NONE {
        let dr = decompress(b.codec(), b.src[b.from..b.to], max_bytes);
        guard let data = dr else let e = err_of(dr) { return err(e); }
        r = reader(data, 0, len(data));
    }
    // a record takes at least 7 bytes
    if b.count > r.left() / 7 + 1 {
        return err("kafka: a batch claims " + to_str(b.count) + " records in " + to_str(r.left()) + " bytes");
    }
    let lat = b.log_append_time();
    for k in 0..b.count {
        let size = rd_varint(r);
        let rec_end = r.pos + size;
        if size < 0 || rec_end > r.end {
            return err("kafka: a record of " + to_str(size) + " bytes runs past its batch");
        }
        rd_u8(r);
        let ts_delta = rd_varlong(r);
        let off_delta = rd_varint(r);
        let key = rd_vfield(r);
        let value = rd_vfield(r);
        let nh = rd_varint(r);
        if nh < 0 || nh > rec_end - r.pos {
            return err("kafka: a record with " + to_str(nh) + " headers");
        }
        let headers: [Header] = [];
        for h in 0..nh {
            let kl = rd_varint(r);
            if kl < 0 {
                return err("kafka: a header with a null key");
            }
            let hk = rd_text(r, kl);
            push(headers, Header { key: hk, value: rd_vfield(r) });
        }
        if r.failed != "" {
            return err(r.failed);
        }
        if r.pos != rec_end {
            return err("kafka: a record's fields and its length disagree");
        }
        let ts = b.base_timestamp + ts_delta;
        if lat {
            ts = b.max_timestamp;
        }
        push(out, Message { topic: topic, partition: partition, offset: b.base_offset + off_delta, timestamp: ts,
                            log_append_time: lat, key: key, value: value, headers: headers,
                            leader_epoch: b.leader_epoch });
    }
    return ok(out);
}

fn rd_vfield(r: Reader) -> opt[bytes] {
    let n = rd_varint(r);
    if n < 0 {
        return none;
    }
    return some(rd_raw(r, n));
}

// v0/v1 messages: one, or for a compressed wrapper the inner message set
// (inner offsets are relative to the wrapper in v1, absolute in v0).
fn legacy_messages(t: Crc, b: Batch, topic: str, partition: int, max_bytes: int, out: [Message], depth: int)
                   -> result[bool, str] {
    let r = reader(b.src, b.from, b.to);
    let key = rd_nbytes(r);
    let value = rd_nbytes(r);
    if r.failed != "" {
        return err(r.failed);
    }
    let lat = (b.attributes & ATTR_LOG_APPEND_TIME) != 0;
    if b.codec() == CODEC_NONE {
        push(out, Message { topic: topic, partition: partition, offset: b.base_offset, timestamp: b.base_timestamp,
                            log_append_time: lat, key: key, value: value, headers: [], leader_epoch: -1 });
        return ok(true);
    }
    if depth > 0 {
        return err("kafka: a compressed message inside a compressed message");
    }
    guard let packed = value else { return err("kafka: a compressed message without a value"); }
    let dr = decompress(b.codec(), packed, max_bytes);
    guard let data = dr else let e = err_of(dr) { return err(e); }
    let inner: [Message] = [];
    let ir = reader(data, 0, len(data));
    while true {
        let br = read_batch(ir, t, true);
        guard let got = br else let e = err_of(br) { return err(e); }
        guard let ib = got else { break; }
        if ib.magic >= 2 {
            return err("kafka: a v2 batch inside a v" + to_str(b.magic) + " message");
        }
        let lr = legacy_messages(t, ib, topic, partition, max_bytes, inner, depth + 1);
        guard let _l = lr else let e = err_of(lr) { return err(e); }
    }
    if len(inner) == 0 {
        return ok(true);
    }
    let shift = 0;
    if b.magic == 1 {
        shift = b.base_offset - inner[len(inner) - 1].offset;
    }
    for m in inner {
        m.offset += shift;
        if lat {
            m.timestamp = b.base_timestamp;
            m.log_append_time = true;
        }
        push(out, m);
    }
    return ok(true);
}

// A control batch's marker: CONTROL_ABORT or CONTROL_COMMIT, read from its
// first record's key (version int16, type int16).
pub let CONTROL_ABORT = 0;
pub let CONTROL_COMMIT = 1;

pub fn control_type(t: Crc, b: Batch) -> result[int, str] {
    let mr = batch_messages(t, b, "", 0, 1048576);
    guard let ms = mr else let e = err_of(mr) { return err(e); }
    if len(ms) == 0 {
        return err("kafka: a control batch without records");
    }
    let key = ms[0].key ?? b"";
    if len(key) < 4 {
        return err("kafka: a control record with a " + to_str(len(key)) + "-byte key");
    }
    return ok((key[2] << 8) | key[3]);
}
