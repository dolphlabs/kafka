// Snappy: the raw block format, and the xerial framing that Java's
// producer writes (a 16-byte header, then length-prefixed raw blocks).
// Brokers and every client read both; this package writes the framing,
// as the Java client does.
//
// A raw block is the uncompressed length (uvarint), then elements whose
// tag byte's low two bits say: 0 a literal, 1 a copy with an 11-bit
// offset, 2 a copy with a 16-bit offset, 3 a copy with a 32-bit offset.

import "strings";

let XERIAL_CHUNK = 32768;
let SNAPPY_BLOCK = 65536;

// A growable output buffer for the codecs.
gc struct Out {
    b: bytes,
    n: int,
}

fn new_out(cap: int) -> Out {
    let c = cap;
    if c < 64 {
        c = 64;
    }
    return Out { b: strings.bytes_zero(c), n: 0 };
}

fn out_room(o: Out, k: int) {
    if o.n + k <= len(o.b) {
        return;
    }
    let c = len(o.b) * 2;
    while c < o.n + k {
        c = c * 2;
    }
    o.b = o.b[0..o.n] + strings.bytes_zero(c - o.n);
}

fn out_byte(o: Out, v: int) {
    out_room(o, 1);
    o.b[o.n] = v & 255;
    o.n += 1;
}

fn out_copy(o: Out, src: bytes, from: int, to: int) {
    let k = to - from;
    out_room(o, k);
    let n = o.n;
    let ob = o.b;
    for i in 0..k {
        ob[n + i] = src[from + i];
    }
    o.n = n + k;
}

fn out_bytes(o: Out) -> bytes {
    if o.n == len(o.b) {
        return o.b;
    }
    return o.b[0..o.n];
}

fn le32_at(b: bytes, i: int) -> int {
    return b[i] | (b[i + 1] << 8) | (b[i + 2] << 16) | (b[i + 3] << 24);
}

// ---- decoding ----------------------------------------------------------------

// Snappy data, raw or xerial-framed, refusing more than max bytes out.
pub fn snappy_decode(src: bytes, max: int) -> result[bytes, str] {
    if is_xerial(src) {
        return xerial_decode(src, max);
    }
    return snappy_block_decode(src, 0, len(src), max);
}

fn is_xerial(src: bytes) -> bool {
    if len(src) < 16 {
        return false;
    }
    return src[0] == 130 && src[1] == 83 && src[2] == 78 && src[3] == 65 && src[4] == 80 && src[5] == 80 &&
           src[6] == 89 && src[7] == 0;
}

fn xerial_decode(src: bytes, max: int) -> result[bytes, str] {
    let parts: [bytes] = [];
    let total = 0;
    let pos = 16;
    while pos < len(src) {
        if pos + 4 > len(src) {
            return err("kafka: snappy: a truncated xerial chunk length");
        }
        let n = (src[pos] << 24) | (src[pos + 1] << 16) | (src[pos + 2] << 8) | src[pos + 3];
        pos += 4;
        if n > len(src) - pos {
            return err("kafka: snappy: a xerial chunk runs past the data");
        }
        let dr = snappy_block_decode(src, pos, pos + n, max - total);
        guard let d = dr else let e = err_of(dr) { return err(e); }
        total += len(d);
        push(parts, d);
        pos += n;
    }
    return ok(strings.join_bytes(parts, b""));
}

// The raw block in src[from..to].
fn snappy_block_decode(src: bytes, from: int, to: int, max: int) -> result[bytes, str] {
    let pos = from;
    let n = 0;
    let shift = 0;
    while true {
        if pos >= to || shift > 28 {
            return err("kafka: snappy: a bad length preamble");
        }
        let c = src[pos];
        pos += 1;
        n = n | ((c & 127) << shift);
        if c < 128 {
            break;
        }
        shift += 7;
    }
    if n > max {
        return err("kafka: snappy: " + to_str(n) + " bytes decompressed, over the limit of " + to_str(max));
    }
    let out = strings.bytes_zero(n);
    let d = 0;
    while pos < to {
        let tag = src[pos];
        pos += 1;
        let kind = tag & 3;
        if kind == 0 {
            let k = tag >> 2;
            if k >= 60 {
                let nb = k - 59;
                if pos + nb > to {
                    return err("kafka: snappy: a truncated literal length");
                }
                k = 0;
                for i in 0..nb {
                    k = k | (src[pos + i] << (8 * i));
                }
                pos += nb;
            }
            k += 1;
            if k > to - pos || k > n - d {
                return err("kafka: snappy: a literal runs past the data");
            }
            for i in 0..k {
                out[d + i] = src[pos + i];
            }
            d += k;
            pos += k;
            continue;
        }
        let k = 0;
        let off = 0;
        if kind == 1 {
            if pos >= to {
                return err("kafka: snappy: a truncated copy");
            }
            k = 4 + ((tag >> 2) & 7);
            off = ((tag >> 5) << 8) | src[pos];
            pos += 1;
        } else if kind == 2 {
            if pos + 2 > to {
                return err("kafka: snappy: a truncated copy");
            }
            k = 1 + (tag >> 2);
            off = src[pos] | (src[pos + 1] << 8);
            pos += 2;
        } else {
            if pos + 4 > to {
                return err("kafka: snappy: a truncated copy");
            }
            k = 1 + (tag >> 2);
            off = le32_at(src, pos);
            pos += 4;
        }
        if off == 0 || off > d {
            return err("kafka: snappy: a copy from before the start");
        }
        if k > n - d {
            return err("kafka: snappy: a copy runs past the length");
        }
        let s = d - off;
        for i in 0..k {
            out[d + i] = out[s + i];
        }
        d += k;
    }
    if d != n {
        return err("kafka: snappy: " + to_str(d) + " bytes decoded of " + to_str(n));
    }
    return ok(out);
}

// ---- encoding ----------------------------------------------------------------

// data as xerial-framed snappy (32 KB chunks, as Java writes).
pub fn snappy_encode(data: bytes) -> bytes {
    let o = new_out(len(data) + len(data) / 6 + 64);
    // 0x82 "SNAPPY" 0, version 1, compatible version 1
    for c in [130, 83, 78, 65, 80, 80, 89, 0, 0, 0, 0, 1, 0, 0, 0, 1] {
        out_byte(o, c);
    }
    let tab = snappy_table();
    let pos = 0;
    while pos < len(data) {
        let end = pos + XERIAL_CHUNK;
        if end > len(data) {
            end = len(data);
        }
        out_room(o, 4);
        let at = o.n;
        o.n += 4;
        snappy_block_encode(o, data, pos, end, tab);
        let n = o.n - at - 4;
        o.b[at] = (n >> 24) & 255;
        o.b[at + 1] = (n >> 16) & 255;
        o.b[at + 2] = (n >> 8) & 255;
        o.b[at + 3] = n & 255;
        pos = end;
    }
    return out_bytes(o);
}

// data as one raw snappy block.
pub fn snappy_encode_raw(data: bytes) -> bytes {
    let o = new_out(len(data) + len(data) / 6 + 32);
    snappy_block_encode(o, data, 0, len(data), snappy_table());
    return out_bytes(o);
}

let TABLE_BITS = 14;

fn snappy_table() -> [int] {
    let t: [int] = [];
    for i in 0..(1 << TABLE_BITS) {
        push(t, -1);
    }
    return t;
}

fn hash4(v: int, bits: int) -> int {
    // a multiplicative hash over the low 32 bits
    return (((v * 0x1e35a7bd) & 0xffffffff) >> (32 - bits));
}

fn emit_literal(o: Out, src: bytes, from: int, to: int) {
    let k = to - from;
    if k <= 0 {
        return;
    }
    let n = k - 1;
    if n < 60 {
        out_byte(o, n << 2);
    } else if n < 256 {
        out_byte(o, 60 << 2);
        out_byte(o, n);
    } else if n < 65536 {
        out_byte(o, 61 << 2);
        out_byte(o, n);
        out_byte(o, n >> 8);
    } else if n < 16777216 {
        out_byte(o, 62 << 2);
        out_byte(o, n);
        out_byte(o, n >> 8);
        out_byte(o, n >> 16);
    } else {
        out_byte(o, 63 << 2);
        out_byte(o, n);
        out_byte(o, n >> 8);
        out_byte(o, n >> 16);
        out_byte(o, n >> 24);
    }
    out_copy(o, src, from, to);
}

// A copy of k bytes from off back (off < 65536 within a block).
fn emit_copy(o: Out, off: int, k: int) {
    let left = k;
    // 64 at a time; keep at least 4 for the last one
    while left >= 68 {
        out_byte(o, (63 << 2) | 2);
        out_byte(o, off);
        out_byte(o, off >> 8);
        left -= 64;
    }
    if left > 64 {
        out_byte(o, (59 << 2) | 2);
        out_byte(o, off);
        out_byte(o, off >> 8);
        left -= 60;
    }
    if left >= 4 && left < 12 && off < 2048 {
        out_byte(o, ((off >> 8) << 5) | ((left - 4) << 2) | 1);
        out_byte(o, off);
        return;
    }
    out_byte(o, ((left - 1) << 2) | 2);
    out_byte(o, off);
    out_byte(o, off >> 8);
}

// The raw block for src[from..to]: its length, then 64 KB pieces
// matched within themselves (offsets fit 16 bits).
fn snappy_block_encode(o: Out, src: bytes, from: int, to: int, tab: [int]) {
    let n = to - from;
    let u = n;
    while u >= 128 {
        out_byte(o, (u & 127) | 128);
        u = u >> 7;
    }
    out_byte(o, u);
    let start = from;
    while start < to {
        let end = start + SNAPPY_BLOCK;
        if end > to {
            end = to;
        }
        snappy_piece(o, src, start, end, tab);
        start = end;
    }
}

fn snappy_piece(o: Out, src: bytes, start: int, end: int, tab: [int]) {
    let n = end - start;
    if n < 17 {
        emit_literal(o, src, start, end);
        return;
    }
    let bits = 8;
    while bits < TABLE_BITS && (1 << bits) < n {
        bits += 1;
    }
    for i in 0..(1 << bits) {
        tab[i] = -1;
    }
    // the last 15 bytes are never the start of a match (as the reference)
    let limit = end - 15;
    let lit = start;
    let i = start;
    while i < limit {
        let v = le32_at(src, i);
        let h = hash4(v, bits);
        let cand = tab[h];
        tab[h] = i;
        if cand >= start && le32_at(src, cand) == v {
            emit_literal(o, src, lit, i);
            let k = 4;
            while i + k < end && src[cand + k] == src[i + k] {
                k += 1;
            }
            emit_copy(o, i - cand, k);
            i += k;
            lit = i;
            if i < limit {
                tab[hash4(le32_at(src, i - 1), bits)] = i - 1;
            }
            continue;
        }
        // skip faster through data that does not match
        i += 1 + ((i - lit) >> 5);
    }
    emit_literal(o, src, lit, end);
}
