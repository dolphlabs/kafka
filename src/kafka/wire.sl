// Kafka wire types, big-endian: a Buf that writers append to, and a
// Reader whose first failure sticks (every later read returns zero, so a
// decoder reads a whole structure and checks once).
//
// A Buf writes small fields into a chunk in place and keeps a large
// value (a record's key or value) as a part of its own, not a copy:
// finish() joins the parts once, with one native copy per byte. Lengths
// that come before what they measure are known before writing (a nested
// structure is built in its own Buf and appended), so nothing is
// back-patched across parts.
//
// The classic forms carry fixed-width lengths (int16 strings, int32 bytes
// and arrays, -1 for null); the compact forms of flexible versions carry
// an unsigned varint of length + 1 (0 for null) and end each structure
// with tagged fields.

import "strings";

let CHUNK_MIN = 512;
let CHUNK_MAX = 65536;
// A value at least this long is kept as its own part instead of copied.
let BLOB_MIN = 128;

pub gc struct Buf {
    parts: [bytes],
    cur: bytes,
    // cur[start..n] is written and not yet sealed into parts
    start: int,
    n: int,
    sealed: int,
}

pub fn new_buf() -> Buf {
    return Buf { parts: [], cur: b"", start: 0, n: 0, sealed: 0 };
}

impl Buf {
    // Bytes written so far.
    pub fn size(self: Buf) -> int {
        return self.sealed + self.n - self.start;
    }

    // Everything written, in one allocation (none when it is one part).
    pub fn finish(self: Buf) -> bytes {
        seal(self);
        if len(self.parts) == 0 {
            return b"";
        }
        if len(self.parts) == 1 {
            return self.parts[0];
        }
        let all = strings.join_bytes(self.parts, b"");
        self.parts = [all];
        return all;
    }

    // The written bytes as parts, for a writer that sends them in turn or
    // a checksum that runs over them.
    pub fn pieces(self: Buf) -> [bytes] {
        seal(self);
        return self.parts;
    }
}

fn seal(w: Buf) {
    if w.n == w.start {
        return;
    }
    if w.start == 0 && w.n == len(w.cur) {
        push(w.parts, w.cur);
        w.cur = b"";
        w.n = 0;
    } else {
        push(w.parts, w.cur[w.start..w.n]);
    }
    w.sealed += w.n - w.start;
    w.start = w.n;
}

// Room for k more bytes in the chunk.
fn room(w: Buf, k: int) {
    if w.n + k <= len(w.cur) {
        return;
    }
    seal(w);
    let c = w.sealed;
    if c < CHUNK_MIN {
        c = CHUNK_MIN;
    }
    if c > CHUNK_MAX {
        c = CHUNK_MAX;
    }
    if c < k {
        c = k;
    }
    w.cur = strings.bytes_zero(c);
    w.start = 0;
    w.n = 0;
}

// Appends b's parts to w, without copying them.
pub fn wr_buf(w: Buf, b: Buf) {
    seal(w);
    for p in b.pieces() {
        push(w.parts, p);
        w.sealed += len(p);
    }
}

pub fn wr_u8(w: Buf, v: int) {
    room(w, 1);
    w.cur[w.n] = v & 255;
    w.n += 1;
}

pub fn wr_bool(w: Buf, v: bool) {
    if v {
        wr_u8(w, 1);
    } else {
        wr_u8(w, 0);
    }
}

pub fn wr_i16(w: Buf, v: int) {
    room(w, 2);
    w.cur[w.n] = (v >> 8) & 255;
    w.cur[w.n + 1] = v & 255;
    w.n += 2;
}

pub fn wr_i32(w: Buf, v: int) {
    room(w, 4);
    let n = w.n;
    w.cur[n] = (v >> 24) & 255;
    w.cur[n + 1] = (v >> 16) & 255;
    w.cur[n + 2] = (v >> 8) & 255;
    w.cur[n + 3] = v & 255;
    w.n = n + 4;
}

pub fn wr_i64(w: Buf, v: int) {
    room(w, 8);
    let n = w.n;
    let shift = 56;
    for k in 0..8 {
        w.cur[n + k] = (v >> shift) & 255;
        shift -= 8;
    }
    w.n = n + 8;
}

// An unsigned varint (7 bits per byte, low first). v is treated as an
// unsigned 64-bit number.
pub fn wr_uvarint(w: Buf, v: int) {
    room(w, 10);
    let u = v as u64;
    while u >= 128 {
        w.cur[w.n] = ((u & 127) | 128) as int;
        w.n += 1;
        u = u >> 7;
    }
    w.cur[w.n] = u as int;
    w.n += 1;
}

// A zigzag varint: a signed 32-bit value (record fields).
pub fn wr_varint(w: Buf, v: int) {
    wr_uvarint(w, ((v << 1) ^ (v >> 31)) & 0xffffffff);
}

// A zigzag varlong: a signed 64-bit value (record timestamps).
pub fn wr_varlong(w: Buf, v: int) {
    wr_uvarint(w, (v << 1) ^ (v >> 63));
}

// A copy of b, or b itself as a part when it is long.
pub fn wr_raw(w: Buf, b: bytes) {
    let k = len(b);
    if k >= BLOB_MIN {
        seal(w);
        push(w.parts, b);
        w.sealed += k;
        return;
    }
    room(w, k);
    let n = w.n;
    for i in 0..k {
        w.cur[n + i] = b[i];
    }
    w.n = n + k;
}

fn wr_text(w: Buf, s: str) {
    let k = len(s);
    if k == 0 {
        return;
    }
    wr_raw(w, to_bytes(s));
}

// STRING: int16 length, then UTF-8.
pub fn wr_str(w: Buf, s: str) {
    wr_i16(w, len(s));
    wr_text(w, s);
}

// NULLABLE_STRING: -1 is null.
pub fn wr_nstr(w: Buf, s: opt[str]) {
    if let v = s {
        wr_str(w, v);
    } else {
        wr_i16(w, -1);
    }
}

// COMPACT_STRING: uvarint length + 1.
pub fn wr_cstr(w: Buf, s: str) {
    wr_uvarint(w, len(s) + 1);
    wr_text(w, s);
}

// COMPACT_NULLABLE_STRING: 0 is null.
pub fn wr_cnstr(w: Buf, s: opt[str]) {
    if let v = s {
        wr_cstr(w, v);
    } else {
        wr_uvarint(w, 0);
    }
}

// BYTES: int32 length.
pub fn wr_bytes(w: Buf, b: bytes) {
    wr_i32(w, len(b));
    wr_raw(w, b);
}

pub fn wr_nbytes(w: Buf, b: opt[bytes]) {
    if let v = b {
        wr_bytes(w, v);
    } else {
        wr_i32(w, -1);
    }
}

pub fn wr_cbytes(w: Buf, b: bytes) {
    wr_uvarint(w, len(b) + 1);
    wr_raw(w, b);
}

pub fn wr_cnbytes(w: Buf, b: opt[bytes]) {
    if let v = b {
        wr_cbytes(w, v);
    } else {
        wr_uvarint(w, 0);
    }
}

// An ARRAY's length (int32; -1 is null).
pub fn wr_len(w: Buf, n: int) {
    wr_i32(w, n);
}

// A COMPACT_ARRAY's length (uvarint n + 1; 0 is null).
pub fn wr_clen(w: Buf, n: int) {
    wr_uvarint(w, n + 1);
}

// No tagged fields.
pub fn wr_tags(w: Buf) {
    wr_u8(w, 0);
}

// A UUID: 16 bytes (a zero UUID when b is empty).
pub fn wr_uuid(w: Buf, b: bytes) {
    if len(b) == 0 {
        wr_i64(w, 0);
        wr_i64(w, 0);
        return;
    }
    wr_raw(w, b);
}

// ---- reading ----------------------------------------------------------------

pub gc struct Reader {
    b: bytes,
    pos: int,
    end: int,
    failed: str,
}

// A reader over b[from..to].
pub fn reader(b: bytes, from: int, to: int) -> Reader {
    return Reader { b: b, pos: from, end: to, failed: "" };
}

impl Reader {
    pub fn good(self: Reader) -> bool {
        return self.failed == "";
    }

    pub fn left(self: Reader) -> int {
        return self.end - self.pos;
    }
}

fn need(r: Reader, n: int) -> bool {
    if r.failed != "" {
        return false;
    }
    if n < 0 || n > r.end - r.pos {
        r.failed = "kafka: truncated: " + to_str(n) + " bytes needed at offset " + to_str(r.pos) + " of " +
                   to_str(r.end);
        return false;
    }
    return true;
}

pub fn fail_read(r: Reader, msg: str) {
    if r.failed == "" {
        r.failed = "kafka: " + msg;
    }
}

pub fn rd_u8(r: Reader) -> int {
    if !need(r, 1) {
        return 0;
    }
    let v = r.b[r.pos];
    r.pos += 1;
    return v;
}

pub fn rd_i8(r: Reader) -> int {
    let v = rd_u8(r);
    if v >= 128 {
        return v - 256;
    }
    return v;
}

pub fn rd_bool(r: Reader) -> bool {
    return rd_u8(r) != 0;
}

pub fn rd_i16(r: Reader) -> int {
    if !need(r, 2) {
        return 0;
    }
    let v = (r.b[r.pos] << 8) | r.b[r.pos + 1];
    r.pos += 2;
    if v >= 32768 {
        return v - 65536;
    }
    return v;
}

pub fn rd_u16(r: Reader) -> int {
    if !need(r, 2) {
        return 0;
    }
    let v = (r.b[r.pos] << 8) | r.b[r.pos + 1];
    r.pos += 2;
    return v;
}

pub fn rd_u32(r: Reader) -> int {
    if !need(r, 4) {
        return 0;
    }
    let p = r.pos;
    let v = (r.b[p] << 24) | (r.b[p + 1] << 16) | (r.b[p + 2] << 8) | r.b[p + 3];
    r.pos = p + 4;
    return v;
}

pub fn rd_i32(r: Reader) -> int {
    let v = rd_u32(r);
    if v >= 2147483648 {
        return v - 4294967296;
    }
    return v;
}

pub fn rd_i64(r: Reader) -> int {
    if !need(r, 8) {
        return 0;
    }
    let v = 0;
    let p = r.pos;
    for k in 0..8 {
        v = (v << 8) | r.b[p + k];
    }
    r.pos = p + 8;
    return v;
}

// An unsigned varint of at most max_bytes bytes (5 for 32 bits, 10 for 64).
fn rd_uvar(r: Reader, max_bytes: int) -> int {
    let v = 0;
    let shift = 0;
    for k in 0..max_bytes {
        if !need(r, 1) {
            return 0;
        }
        let c = r.b[r.pos];
        r.pos += 1;
        v = v | ((c & 127) << shift);
        if c < 128 {
            return v;
        }
        shift += 7;
    }
    fail_read(r, "a varint longer than " + to_str(max_bytes) + " bytes");
    return 0;
}

// UNSIGNED_VARINT (32 bits).
pub fn rd_uvarint(r: Reader) -> int {
    return rd_uvar(r, 5) & 0xffffffff;
}

// VARINT: zigzag, 32 bits.
pub fn rd_varint(r: Reader) -> int {
    let u = rd_uvar(r, 5) & 0xffffffff;
    let v = (u >> 1) ^ (0 - (u & 1));
    // back to a signed 32-bit value
    if v >= 2147483648 {
        return v - 4294967296;
    }
    if v < -2147483648 {
        return v + 4294967296;
    }
    return v;
}

// VARLONG: zigzag, 64 bits.
pub fn rd_varlong(r: Reader) -> int {
    let u = rd_uvar(r, 10);
    return (((u as u64) >> 1) as int) ^ (0 - (u & 1));
}

// n bytes, copied out.
pub fn rd_raw(r: Reader, n: int) -> bytes {
    if !need(r, n) {
        return b"";
    }
    if n == 0 {
        return b"";
    }
    let out = r.b[r.pos..r.pos + n];
    r.pos += n;
    return out;
}

fn rd_text(r: Reader, n: int) -> str {
    if !need(r, n) {
        return "";
    }
    if n == 0 {
        return "";
    }
    let s = strings.from_bytes(r.b, r.pos, r.pos + n);
    r.pos += n;
    return s;
}

pub fn rd_skip(r: Reader, n: int) {
    if need(r, n) {
        r.pos += n;
    }
}

// STRING; a null reads as "".
pub fn rd_str(r: Reader) -> str {
    let n = rd_i16(r);
    if n < 0 {
        if n != -1 {
            fail_read(r, "a string length of " + to_str(n));
        }
        return "";
    }
    return rd_text(r, n);
}

pub fn rd_nstr(r: Reader) -> opt[str] {
    let n = rd_i16(r);
    if n == -1 {
        return none;
    }
    if n < 0 {
        fail_read(r, "a string length of " + to_str(n));
        return none;
    }
    return some(rd_text(r, n));
}

// COMPACT_STRING; a null reads as "".
pub fn rd_cstr(r: Reader) -> str {
    let n = rd_uvarint(r);
    if n == 0 {
        return "";
    }
    return rd_text(r, n - 1);
}

pub fn rd_cnstr(r: Reader) -> opt[str] {
    let n = rd_uvarint(r);
    if n == 0 {
        return none;
    }
    return some(rd_text(r, n - 1));
}

pub fn rd_bytes(r: Reader) -> bytes {
    let n = rd_i32(r);
    if n < 0 {
        return b"";
    }
    return rd_raw(r, n);
}

pub fn rd_nbytes(r: Reader) -> opt[bytes] {
    let n = rd_i32(r);
    if n < 0 {
        return none;
    }
    return some(rd_raw(r, n));
}

pub fn rd_cbytes(r: Reader) -> bytes {
    let n = rd_uvarint(r);
    if n == 0 {
        return b"";
    }
    return rd_raw(r, n - 1);
}

pub fn rd_cnbytes(r: Reader) -> opt[bytes] {
    let n = rd_uvarint(r);
    if n == 0 {
        return none;
    }
    return some(rd_raw(r, n - 1));
}

// An ARRAY's length; null (-1) reads as 0. Each element takes at least
// one byte, so a length past what remains is malformed: refused before
// anything is allocated for it.
pub fn rd_len(r: Reader) -> int {
    let n = rd_i32(r);
    if n < 0 {
        return 0;
    }
    if n > r.end - r.pos {
        fail_read(r, "an array of " + to_str(n) + " elements in " + to_str(r.end - r.pos) + " bytes");
        return 0;
    }
    return n;
}

// A COMPACT_ARRAY's length; null reads as 0.
pub fn rd_clen(r: Reader) -> int {
    let n = rd_uvarint(r);
    if n == 0 {
        return 0;
    }
    n -= 1;
    if n > r.end - r.pos {
        fail_read(r, "an array of " + to_str(n) + " elements in " + to_str(r.end - r.pos) + " bytes");
        return 0;
    }
    return n;
}

pub fn rd_uuid(r: Reader) -> bytes {
    return rd_raw(r, 16);
}

// Skips a structure's tagged fields (none of the ones this package reads
// are needed: they carry hints a later request learns anyway).
pub fn rd_tags(r: Reader) {
    let n = rd_uvarint(r);
    for k in 0..n {
        if r.failed != "" {
            return;
        }
        let _tag = rd_uvarint(r);
        let size = rd_uvarint(r);
        rd_skip(r, size);
    }
}

// A tagged field: its tag and raw value.
pub gc struct Tag {
    tag: int,
    data: bytes,
}

pub fn rd_tag_list(r: Reader) -> [Tag] {
    let out: [Tag] = [];
    let n = rd_uvarint(r);
    for k in 0..n {
        if r.failed != "" {
            break;
        }
        let tag = rd_uvarint(r);
        let size = rd_uvarint(r);
        push(out, Tag { tag: tag, data: rd_raw(r, size) });
    }
    return out;
}
