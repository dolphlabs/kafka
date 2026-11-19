// LZ4: the frame format (Kafka's lz4 codec) over the block format, and
// XXH32, which the frame's header and optional checksums use.
//
// A frame is the magic 0x184D2204, a descriptor (flags, block size,
// optional content size and dictionary id, a header checksum), blocks
// (a 4-byte size, high bit set for a stored block, optionally followed by
// a block checksum), a zero end mark, and an optional content checksum.
// Kafka's v0 messages computed the header checksum over the magic too
// (KAFKA-3160); both forms are accepted.
//
// A block is sequences: a token (literal length high nibble, match length
// - 4 low nibble, 15 meaning more bytes follow), literals, a 2-byte
// little-endian offset and the extra match length. The last sequence is
// literals only.

import "strings";

let LZ4_MAGIC = 0x184d2204;
let LZ4_BLOCK = 65536;
let P1 = 2654435761;
let P2 = 2246822519;
let P3 = 3266489917;
let P4 = 668265263;
let P5 = 374761393;

// ---- XXH32 -------------------------------------------------------------------

// a * b mod 2^32, without overflowing 64 bits.
fn mul32(a: int, b: int) -> int {
    return ((a * (b & 0xffff)) + (((a * (b >> 16)) & 0xffff) << 16)) & 0xffffffff;
}

fn rotl32(x: int, r: int) -> int {
    return ((x << r) | (x >> (32 - r))) & 0xffffffff;
}

fn xxh_round(acc: int, input: int) -> int {
    return mul32(rotl32((acc + mul32(input, P2)) & 0xffffffff, 13), P1);
}

// XXH32 of b[from..to] with seed 0.
pub fn xxh32(b: bytes, from: int, to: int) -> int {
    let n = to - from;
    let i = from;
    let h = 0;
    if n >= 16 {
        let v1 = (P1 + P2) & 0xffffffff;
        let v2 = P2;
        let v3 = 0;
        let v4 = (0 - P1) & 0xffffffff;
        while i + 16 <= to {
            v1 = xxh_round(v1, le32_at(b, i));
            v2 = xxh_round(v2, le32_at(b, i + 4));
            v3 = xxh_round(v3, le32_at(b, i + 8));
            v4 = xxh_round(v4, le32_at(b, i + 12));
            i += 16;
        }
        h = (rotl32(v1, 1) + rotl32(v2, 7) + rotl32(v3, 12) + rotl32(v4, 18)) & 0xffffffff;
    } else {
        h = P5;
    }
    h = (h + n) & 0xffffffff;
    while i + 4 <= to {
        h = mul32(rotl32((h + mul32(le32_at(b, i), P3)) & 0xffffffff, 17), P4);
        i += 4;
    }
    while i < to {
        h = mul32(rotl32((h + mul32(b[i], P5)) & 0xffffffff, 11), P1);
        i += 1;
    }
    h = h ^ (h >> 15);
    h = mul32(h, P2);
    h = h ^ (h >> 13);
    h = mul32(h, P3);
    h = h ^ (h >> 16);
    return h;
}

// ---- decoding ----------------------------------------------------------------

// One or more LZ4 frames (skippable frames are skipped), refusing more
// than max bytes out.
pub fn lz4_decode(src: bytes, max: int) -> result[bytes, str] {
    let o = new_out(len(src) * 3);
    let pos = 0;
    while pos < len(src) {
        if pos + 4 > len(src) {
            return err("kafka: lz4: trailing bytes after a frame");
        }
        let magic = le32_at(src, pos);
        if (magic & 0xfffffff0) == 0x184d2a50 {
            if pos + 8 > len(src) {
                return err("kafka: lz4: a truncated skippable frame");
            }
            let n = le32_at(src, pos + 4);
            if n > len(src) - pos - 8 {
                return err("kafka: lz4: a skippable frame runs past the data");
            }
            pos += 8 + n;
            continue;
        }
        if magic != LZ4_MAGIC {
            return err("kafka: lz4: not an LZ4 frame");
        }
        let fr = lz4_frame(src, pos, o, max);
        guard let next = fr else let e = err_of(fr) { return err(e); }
        pos = next;
    }
    return ok(out_bytes(o));
}

// Decodes the frame at src[at..] onto o; returns where it ended.
fn lz4_frame(src: bytes, at: int, o: Out, max: int) -> result[int, str] {
    let pos = at + 4;
    if pos + 3 > len(src) {
        return err("kafka: lz4: a truncated frame descriptor");
    }
    let flg = src[pos];
    let bd = src[pos + 1];
    if (flg >> 6) != 1 {
        return err("kafka: lz4: frame version " + to_str(flg >> 6));
    }
    if (flg & 2) != 0 || (bd & 0x8f) != 0 {
        return err("kafka: lz4: reserved descriptor bits are set");
    }
    let block_max = 1 << (8 + 2 * ((bd >> 4) & 7));
    if ((bd >> 4) & 7) < 4 {
        return err("kafka: lz4: a block size id of " + to_str((bd >> 4) & 7));
    }
    let block_sum = (flg & 16) != 0;
    let content_size = (flg & 8) != 0;
    let content_sum = (flg & 4) != 0;
    let dict = (flg & 1) != 0;
    let desc_end = pos + 2;
    if content_size {
        desc_end += 8;
    }
    if dict {
        desc_end += 4;
    }
    if desc_end + 1 > len(src) {
        return err("kafka: lz4: a truncated frame descriptor");
    }
    if dict {
        return err("kafka: lz4: frames with a dictionary are not supported");
    }
    let hc = src[desc_end];
    if hc != ((xxh32(src, pos, desc_end) >> 8) & 255) && hc != ((xxh32(src, at, desc_end) >> 8) & 255) {
        return err("kafka: lz4: the frame descriptor fails its checksum");
    }
    pos = desc_end + 1;
    let start = o.n;
    while true {
        if pos + 4 > len(src) {
            return err("kafka: lz4: a truncated block size");
        }
        let size = le32_at(src, pos);
        pos += 4;
        if size == 0 {
            break;
        }
        let stored = (size & 0x80000000) != 0;
        size = size & 0x7fffffff;
        if size > block_max || size > len(src) - pos {
            return err("kafka: lz4: a block of " + to_str(size) + " bytes runs past the data or the block size");
        }
        if block_sum {
            if pos + size + 4 > len(src) {
                return err("kafka: lz4: a truncated block checksum");
            }
            if le32_at(src, pos + size) != xxh32(src, pos, pos + size) {
                return err("kafka: lz4: a block fails its checksum");
            }
        }
        if stored {
            if o.n + size - start > max {
                return err("kafka: lz4: more than " + to_str(max) + " bytes decompressed");
            }
            out_copy(o, src, pos, pos + size);
        } else {
            let br = lz4_block_decode(o, src, pos, pos + size, start, max);
            guard let _b = br else let e = err_of(br) { return err(e); }
        }
        pos += size;
        if block_sum {
            pos += 4;
        }
    }
    if content_sum {
        if pos + 4 > len(src) {
            return err("kafka: lz4: a truncated content checksum");
        }
        if le32_at(src, pos) != xxh32(o.b, start, o.n) {
            return err("kafka: lz4: the content fails its checksum");
        }
        pos += 4;
    }
    if content_size {
        let want = 0;
        for k in 0..8 {
            want = want | (src[at + 6 + k] << (8 * k));
        }
        if want != o.n - start {
            return err("kafka: lz4: the frame says " + to_str(want) + " bytes and holds " + to_str(o.n - start));
        }
    }
    return ok(pos);
}

// The block src[from..to] onto o. Matches may reach back to start (the
// frame's first byte), so linked blocks decode too.
fn lz4_block_decode(o: Out, src: bytes, from: int, to: int, start: int, max: int) -> result[bool, str] {
    let pos = from;
    while pos < to {
        let token = src[pos];
        pos += 1;
        let lit = token >> 4;
        if lit == 15 {
            while true {
                if pos >= to {
                    return err("kafka: lz4: a truncated literal length");
                }
                let c = src[pos];
                pos += 1;
                lit += c;
                if c != 255 {
                    break;
                }
            }
        }
        if lit > to - pos {
            return err("kafka: lz4: literals run past the block");
        }
        if o.n + lit - start > max {
            return err("kafka: lz4: more than " + to_str(max) + " bytes decompressed");
        }
        out_copy(o, src, pos, pos + lit);
        pos += lit;
        if pos == to {
            break;
        }
        if pos + 2 > to {
            return err("kafka: lz4: a truncated match offset");
        }
        let off = src[pos] | (src[pos + 1] << 8);
        pos += 2;
        if off == 0 || off > o.n - start {
            return err("kafka: lz4: a match from before the start");
        }
        let ml = token & 15;
        if ml == 15 {
            while true {
                if pos >= to {
                    return err("kafka: lz4: a truncated match length");
                }
                let c = src[pos];
                pos += 1;
                ml += c;
                if c != 255 {
                    break;
                }
            }
        }
        ml += 4;
        if o.n + ml - start > max {
            return err("kafka: lz4: more than " + to_str(max) + " bytes decompressed");
        }
        out_room(o, ml);
        // forward, so a match that overlaps itself repeats (LZ77)
        o.n = strings.copy_into(o.b, o.n, o.b, o.n - off, o.n - off + ml);
    }
    return ok(true);
}

// ---- encoding ----------------------------------------------------------------

// data as one LZ4 frame, as Kafka's Java client writes it: independent
// 64 KB blocks, no checksums but the header's.
pub fn lz4_encode(data: bytes) -> bytes {
    let o = new_out(len(data) + len(data) / 255 + 64);
    out_byte(o, 0x04);
    out_byte(o, 0x22);
    out_byte(o, 0x4d);
    out_byte(o, 0x18);
    // version 01, independent blocks; 64 KB blocks
    out_byte(o, 0x60);
    out_byte(o, 0x40);
    out_byte(o, (xxh32(o.b, 4, 6) >> 8) & 255);
    let tab = lz4_table();
    let pos = 0;
    while pos < len(data) {
        let end = pos + LZ4_BLOCK;
        if end > len(data) {
            end = len(data);
        }
        out_room(o, 4);
        let at = o.n;
        o.n += 4;
        lz4_block_encode(o, data, pos, end, tab);
        let size = o.n - at - 4;
        if size >= end - pos {
            // incompressible: store it
            o.n = at + 4;
            out_copy(o, data, pos, end);
            size = (end - pos) | 0x80000000;
        }
        o.b[at] = size & 255;
        o.b[at + 1] = (size >> 8) & 255;
        o.b[at + 2] = (size >> 16) & 255;
        o.b[at + 3] = (size >> 24) & 255;
        pos = end;
    }
    for k in 0..4 {
        out_byte(o, 0);
    }
    return out_bytes(o);
}

let LZ4_HASH_BITS = 14;

fn lz4_table() -> [int] {
    let t: [int] = [];
    for i in 0..(1 << LZ4_HASH_BITS) {
        push(t, -1);
    }
    return t;
}

fn lz4_hash(v: int, bits: int) -> int {
    return ((v * 2654435761) & 0xffffffff) >> (32 - bits);
}

fn lz4_len_bytes(o: Out, n: int) {
    let rest = n;
    while rest >= 255 {
        out_byte(o, 255);
        rest -= 255;
    }
    out_byte(o, rest);
}

fn lz4_sequence(o: Out, src: bytes, lit_from: int, lit_to: int, off: int, ml: int) {
    let lit = lit_to - lit_from;
    let tl = lit;
    if tl > 15 {
        tl = 15;
    }
    let tm = ml - 4;
    if tm > 15 {
        tm = 15;
    }
    out_byte(o, (tl << 4) | tm);
    if lit >= 15 {
        lz4_len_bytes(o, lit - 15);
    }
    out_copy(o, src, lit_from, lit_to);
    out_byte(o, off);
    out_byte(o, off >> 8);
    if ml - 4 >= 15 {
        lz4_len_bytes(o, ml - 4 - 15);
    }
}

// One block of src[from..to] (at most 64 KB, so offsets fit 16 bits).
fn lz4_block_encode(o: Out, src: bytes, from: int, to: int, tab: [int]) {
    let n = to - from;
    let anchor = from;
    if n >= 13 {
        let bits = 8;
        while bits < LZ4_HASH_BITS && (1 << bits) < n {
            bits += 1;
        }
        for i in 0..(1 << bits) {
            tab[i] = -1;
        }
        // a match starts at least 12 bytes before the end and ends at
        // least 5 before it
        let last_start = to - 12;
        let match_end = to - 5;
        let i = from;
        while i <= last_start {
            let v = le32_at(src, i);
            let h = lz4_hash(v, bits);
            let cand = tab[h];
            tab[h] = i;
            if cand >= from && le32_at(src, cand) == v {
                let s = i;
                let c = cand;
                while s > anchor && c > from && src[s - 1] == src[c - 1] {
                    s -= 1;
                    c -= 1;
                }
                let e = i + 4;
                while e < match_end && src[e] == src[e - i + cand] {
                    e += 1;
                }
                lz4_sequence(o, src, anchor, s, s - c, e - s);
                anchor = e;
                i = e;
                if i - 2 >= from && i - 2 <= last_start {
                    tab[lz4_hash(le32_at(src, i - 2), bits)] = i - 2;
                }
                continue;
            }
            i += 1 + ((i - anchor) >> 6);
        }
    }
    // the last literals
    let lit = to - anchor;
    if lit >= 15 {
        out_byte(o, 15 << 4);
        lz4_len_bytes(o, lit - 15);
    } else {
        out_byte(o, lit << 4);
    }
    out_copy(o, src, anchor, to);
}
