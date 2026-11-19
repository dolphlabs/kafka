// Zstandard (RFC 8878): a complete frame decoder, and an encoder that
// finds matches greedily and writes them with the predefined FSE tables.
//
// A frame is the magic 0xFD2FB528, a header (window or content size,
// optional dictionary id and checksum flag), blocks (raw, RLE or
// compressed, at most 128 KB each), and an optional checksum (the low 32
// bits of XXH64). A compressed block is a literals section (raw, RLE or
// Huffman-coded in one or four streams, possibly reusing the previous
// block's Huffman table) and a sequences section: (literal length, match
// length, offset) triples, FSE-coded with predefined, RLE, described or
// repeated tables, read backwards from the block's end.

import "strings";

let ZSTD_MAGIC = 0xfd2fb528;
let ZSTD_BLOCK_MAX = 131072;

// ---- static tables -----------------------------------------------------------

gc struct ZTabs {
    ll_base: [int],
    ll_bits: [int],
    ml_base: [int],
    ml_bits: [int],
    ll_norm: [int],
    ml_norm: [int],
    of_norm: [int],
}

fn ztabs() -> ZTabs {
    let ll_base: [int] = [];
    let ll_bits: [int] = [];
    for i in 0..16 {
        push(ll_base, i);
        push(ll_bits, 0);
    }
    for v in [16, 18, 20, 22, 24, 28, 32, 40, 48, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536] {
        push(ll_base, v);
    }
    for v in [1, 1, 1, 1, 2, 2, 3, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] {
        push(ll_bits, v);
    }
    let ml_base: [int] = [];
    let ml_bits: [int] = [];
    for i in 0..32 {
        push(ml_base, i + 3);
        push(ml_bits, 0);
    }
    for v in [35, 37, 39, 41, 43, 47, 51, 59, 67, 83, 99, 131, 259, 515, 1027, 2051, 4099, 8195, 16387, 32771, 65539] {
        push(ml_base, v);
    }
    for v in [1, 1, 1, 1, 2, 2, 3, 3, 4, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] {
        push(ml_bits, v);
    }
    return ZTabs {
        ll_base: ll_base, ll_bits: ll_bits, ml_base: ml_base, ml_bits: ml_bits,
        ll_norm: [4, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 2, 1, 1, 1, 1, 1, -1,
                  -1, -1, -1],
        ml_norm: [1, 4, 3, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, -1, -1, -1, -1, -1, -1, -1],
        of_norm: [1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, -1, -1, -1, -1, -1]
    };
}

fn highbit(v: int) -> int {
    let n = -1;
    let x = v;
    while x > 0 {
        x = x >> 1;
        n += 1;
    }
    return n;
}

// ---- bit readers ---------------------------------------------------------------

// n bits (n <= 31) of b[from..to] starting at bit pos (little-endian bit
// order); bits past to read as 0.
fn bits_at(b: bytes, from: int, to: int, pos: int, n: int) -> int {
    if n == 0 {
        return 0;
    }
    let i = from + (pos >> 3);
    let sh = pos & 7;
    let need = (sh + n + 7) >> 3;
    let v = 0;
    for k in 0..need {
        if i + k < to {
            v = v | (b[i + k] << (8 * k));
        }
    }
    return (v >> sh) & ((1 << n) - 1);
}

// A stream read backwards from its last byte, whose highest set bit
// marks where the data begins. cont holds the nb bits just below pos;
// bytes below them are loaded as reads need them.
gc struct Back {
    b: bytes,
    from: int,
    // the next byte to load is b[next - 1]
    next: int,
    cont: int,
    nb: int,
    // bits not yet read; negative once reads ran past the start
    pos: int,
}

fn back_reader(b: bytes, from: int, to: int) -> result[Back, str] {
    if to <= from {
        return err("kafka: zstd: an empty bitstream");
    }
    let last = b[to - 1];
    if last == 0 {
        return err("kafka: zstd: a bitstream without its end mark");
    }
    let hb = highbit(last);
    return ok(Back { b: b, from: from, next: to - 1, cont: last & ((1 << hb) - 1), nb: hb,
                     pos: (to - from - 1) * 8 + hb });
}

fn back_fill(r: Back) {
    let b = r.b;
    let c = r.cont;
    let nb = r.nb;
    let next = r.next;
    while nb <= 48 && next > r.from {
        next -= 1;
        c = (c << 8) | b[next];
        nb += 8;
    }
    r.cont = c;
    r.nb = nb;
    r.next = next;
}

// n bits; past the start they read as zeros and pos goes negative (an
// overflow the caller checks).
fn back_read(r: Back, n: int) -> int {
    if n == 0 {
        return 0;
    }
    if r.nb < n {
        back_fill(r);
    }
    r.pos -= n;
    if r.nb >= n {
        let nb = r.nb - n;
        let v = (r.cont >> nb) & ((1 << n) - 1);
        r.cont = r.cont & ((1 << nb) - 1);
        r.nb = nb;
        return v;
    }
    let v = (r.cont << (n - r.nb)) & ((1 << n) - 1);
    r.cont = 0;
    r.nb = 0;
    return v;
}

fn back_peek(r: Back, n: int) -> int {
    if r.nb < n {
        back_fill(r);
    }
    if r.nb >= n {
        return (r.cont >> (r.nb - n)) & ((1 << n) - 1);
    }
    return (r.cont << (n - r.nb)) & ((1 << n) - 1);
}

// Consumes n bits a peek looked at.
fn back_skip(r: Back, n: int) {
    r.pos -= n;
    if r.nb >= n {
        r.nb -= n;
        r.cont = r.cont & ((1 << r.nb) - 1);
    } else {
        r.cont = 0;
        r.nb = 0;
    }
}

// ---- FSE ------------------------------------------------------------------------

gc struct Fse {
    log: int,
    sym: [int],
    nbits: [int],
    base: [int],
}

// The decoding table for normalized counts (-1: "less than 1").
fn fse_build(norm: [int], log: int) -> result[Fse, str] {
    let size = 1 << log;
    let sym: [int] = [];
    let nbits: [int] = [];
    let base: [int] = [];
    for i in 0..size {
        push(sym, 0);
        push(nbits, 0);
        push(base, 0);
    }
    let next: [int] = [];
    let high = size - 1;
    for s in 0..len(norm) {
        if norm[s] == -1 {
            sym[high] = s;
            high -= 1;
            push(next, 1);
        } else {
            push(next, norm[s]);
        }
    }
    let step = (size >> 1) + (size >> 3) + 3;
    let mask = size - 1;
    let pos = 0;
    for s in 0..len(norm) {
        for k in 0..norm[s] {
            sym[pos] = s;
            pos = (pos + step) & mask;
            while pos > high {
                pos = (pos + step) & mask;
            }
        }
    }
    if pos != 0 {
        return err("kafka: zstd: an FSE table whose counts do not fill it");
    }
    for u in 0..size {
        let s = sym[u];
        let ns = next[s];
        next[s] = ns + 1;
        let nb = log - highbit(ns);
        nbits[u] = nb;
        base[u] = (ns << nb) - size;
    }
    return ok(Fse { log: log, sym: sym, nbits: nbits, base: base });
}

// A table that always gives sym (RLE mode).
fn fse_rle(sym: int) -> Fse {
    return Fse { log: 0, sym: [sym], nbits: [0], base: [0] };
}

// An FSE table description at b[pos..to]; returns the table and where
// the description ended.
gc struct FseRead {
    table: Fse,
    next: int,
}

fn fse_read(b: bytes, pos: int, to: int, max_sym: int, max_log: int) -> result[FseRead, str] {
    let bit = 0;
    let log = bits_at(b, pos, to, bit, 4) + 5;
    bit += 4;
    if log > max_log {
        return err("kafka: zstd: an FSE accuracy of " + to_str(log) + " bits, over " + to_str(max_log));
    }
    let remaining = (1 << log) + 1;
    let threshold = 1 << log;
    let nb = log + 1;
    let norm: [int] = [];
    while remaining > 1 && len(norm) <= max_sym {
        if pos + (bit >> 3) >= to {
            return err("kafka: zstd: a truncated FSE table description");
        }
        let maxv = 2 * threshold - 1 - remaining;
        let v = 0;
        let low = bits_at(b, pos, to, bit, nb - 1);
        if low < maxv {
            v = low;
            bit += nb - 1;
        } else {
            v = bits_at(b, pos, to, bit, nb);
            if v >= threshold {
                v -= maxv;
            }
            bit += nb;
        }
        let count = v - 1;
        if count < 0 {
            remaining -= 1;
        } else {
            remaining -= count;
        }
        push(norm, count);
        if count == 0 {
            while true {
                let rep = bits_at(b, pos, to, bit, 2);
                bit += 2;
                for k in 0..rep {
                    push(norm, 0);
                }
                if rep != 3 {
                    break;
                }
                if len(norm) > max_sym + 1 {
                    break;
                }
            }
        }
        while remaining < threshold && threshold > 1 {
            nb -= 1;
            threshold = threshold >> 1;
        }
    }
    if remaining != 1 || len(norm) > max_sym + 1 {
        return err("kafka: zstd: a malformed FSE table description");
    }
    let next = pos + ((bit + 7) >> 3);
    if next > to {
        return err("kafka: zstd: a truncated FSE table description");
    }
    let tr = fse_build(norm, log);
    guard let t = tr else let e = err_of(tr) { return err(e); }
    return ok(FseRead { table: t, next: next });
}

// ---- Huffman ----------------------------------------------------------------------

gc struct Huf {
    max_bits: int,
    sym: [int],
    nbits: [int],
}

gc struct HufRead {
    table: Huf,
    next: int,
}

// A Huffman tree description at b[pos..to].
fn huf_read(b: bytes, pos: int, to: int) -> result[HufRead, str] {
    if pos >= to {
        return err("kafka: zstd: a truncated Huffman tree description");
    }
    let hb = b[pos];
    let weights: [int] = [];
    let next = 0;
    if hb >= 128 {
        let n = hb - 127;
        let nbytes = (n + 1) / 2;
        if pos + 1 + nbytes > to {
            return err("kafka: zstd: a truncated Huffman weight list");
        }
        for i in 0..n {
            let byte = b[pos + 1 + i / 2];
            if i % 2 == 0 {
                push(weights, byte >> 4);
            } else {
                push(weights, byte & 15);
            }
        }
        next = pos + 1 + nbytes;
    } else {
        let end = pos + 1 + hb;
        if end > to {
            return err("kafka: zstd: a truncated Huffman weight stream");
        }
        let fr = fse_read(b, pos + 1, end, 255, 6);
        guard let f = fr else let e = err_of(fr) { return err(e); }
        let br = back_reader(b, f.next, end);
        guard let r = br else let e = err_of(br) { return err(e); }
        let t = f.table;
        let s1 = back_read(r, t.log);
        let s2 = back_read(r, t.log);
        while true {
            if len(weights) > 254 {
                return err("kafka: zstd: too many Huffman weights");
            }
            push(weights, t.sym[s1]);
            s1 = t.base[s1] + back_read(r, t.nbits[s1]);
            if r.pos < 0 {
                push(weights, t.sym[s2]);
                break;
            }
            push(weights, t.sym[s2]);
            s2 = t.base[s2] + back_read(r, t.nbits[s2]);
            if r.pos < 0 {
                push(weights, t.sym[s1]);
                break;
            }
        }
        next = end;
    }
    let total = 0;
    for w in weights {
        if w > 11 {
            return err("kafka: zstd: a Huffman weight of " + to_str(w));
        }
        if w > 0 {
            total += 1 << (w - 1);
        }
    }
    if total == 0 {
        return err("kafka: zstd: a Huffman tree with no symbols");
    }
    let max_bits = highbit(total) + 1;
    let rest = (1 << max_bits) - total;
    if max_bits > 11 || (rest & (rest - 1)) != 0 {
        return err("kafka: zstd: a Huffman tree whose weights do not sum to a power of two");
    }
    push(weights, highbit(rest) + 1);
    let size = 1 << max_bits;
    let sym: [int] = [];
    let nbits: [int] = [];
    for i in 0..size {
        push(sym, 0);
        push(nbits, 0);
    }
    let at = 0;
    for w in 1..(max_bits + 1) {
        let span = 1 << (w - 1);
        for s in 0..len(weights) {
            if weights[s] != w {
                continue;
            }
            for k in 0..span {
                sym[at + k] = s;
                nbits[at + k] = max_bits + 1 - w;
            }
            at += span;
        }
    }
    if at != size {
        return err("kafka: zstd: a malformed Huffman tree");
    }
    return ok(HufRead { table: Huf { max_bits: max_bits, sym: sym, nbits: nbits }, next: next });
}

// One Huffman stream b[from..to] into out[at..at+n].
fn huf_stream(h: Huf, b: bytes, from: int, to: int, out: bytes, at: int, n: int) -> result[bool, str] {
    let br = back_reader(b, from, to);
    guard let r = br else let e = err_of(br) { return err(e); }
    let mb = h.max_bits;
    for i in 0..n {
        let idx = back_peek(r, mb);
        out[at + i] = h.sym[idx];
        back_skip(r, h.nbits[idx]);
    }
    if r.pos != 0 {
        return err("kafka: zstd: a Huffman stream that does not end where its literals do");
    }
    return ok(true);
}

// ---- frames ---------------------------------------------------------------------

// What carries from one block of a frame to the next.
gc struct ZState {
    tabs: ZTabs,
    huf: opt[Huf],
    ll: opt[Fse],
    of: opt[Fse],
    ml: opt[Fse],
    rep1: int,
    rep2: int,
    rep3: int,
    start: int,
    window: int,
}

// One or more zstd frames (skippable frames are skipped), refusing more
// than max bytes out.
pub fn zstd_decode(src: bytes, max: int) -> result[bytes, str] {
    let o = new_out(len(src) * 4);
    let tabs = ztabs();
    let pos = 0;
    while pos < len(src) {
        if pos + 4 > len(src) {
            return err("kafka: zstd: trailing bytes after a frame");
        }
        let magic = le32_at(src, pos);
        if (magic & 0xfffffff0) == 0x184d2a50 {
            if pos + 8 > len(src) {
                return err("kafka: zstd: a truncated skippable frame");
            }
            let n = le32_at(src, pos + 4);
            if n > len(src) - pos - 8 {
                return err("kafka: zstd: a skippable frame runs past the data");
            }
            pos += 8 + n;
            continue;
        }
        if magic != ZSTD_MAGIC {
            return err("kafka: zstd: not a zstd frame");
        }
        let fr = zstd_frame(src, pos + 4, o, max, tabs);
        guard let next = fr else let e = err_of(fr) { return err(e); }
        pos = next;
    }
    return ok(out_bytes(o));
}

fn zstd_frame(src: bytes, at: int, o: Out, max: int, tabs: ZTabs) -> result[int, str] {
    let pos = at;
    if pos >= len(src) {
        return err("kafka: zstd: a truncated frame header");
    }
    let fhd = src[pos];
    pos += 1;
    let fcs_flag = fhd >> 6;
    let single = (fhd & 32) != 0;
    let checksum = (fhd & 4) != 0;
    let dict_flag = fhd & 3;
    if (fhd & 8) != 0 {
        return err("kafka: zstd: a reserved frame header bit is set");
    }
    let window = 0;
    if !single {
        if pos >= len(src) {
            return err("kafka: zstd: a truncated frame header");
        }
        let wd = src[pos];
        pos += 1;
        let wlog = 10 + (wd >> 3);
        let wbase = 1 << wlog;
        window = wbase + (wbase / 8) * (wd & 7);
    }
    let dict_size = switch dict_flag {
        case 0 { 0 }
        case 1 { 1 }
        case 2 { 2 }
        default { 4 }
    };
    if pos + dict_size > len(src) {
        return err("kafka: zstd: a truncated frame header");
    }
    let dict = 0;
    for k in 0..dict_size {
        dict = dict | (src[pos + k] << (8 * k));
    }
    pos += dict_size;
    if dict != 0 {
        return err("kafka: zstd: frames with a dictionary are not supported");
    }
    let fcs_size = switch fcs_flag {
        case 0 { 0 }
        case 1 { 2 }
        case 2 { 4 }
        default { 8 }
    };
    if fcs_flag == 0 && single {
        fcs_size = 1;
    }
    if pos + fcs_size > len(src) {
        return err("kafka: zstd: a truncated frame header");
    }
    let fcs = -1;
    if fcs_size > 0 {
        fcs = 0;
        for k in 0..fcs_size {
            fcs = fcs | (src[pos + k] << (8 * k));
        }
        if fcs_size == 2 {
            fcs += 256;
        }
        pos += fcs_size;
        if fcs < 0 || fcs > max {
            return err("kafka: zstd: a frame of " + to_str(fcs) + " bytes, over the limit of " + to_str(max));
        }
        out_room(o, fcs);
    }
    if single {
        window = fcs;
    }
    let st = ZState { tabs: tabs, huf: none, ll: none, of: none, ml: none, rep1: 1, rep2: 4, rep3: 8, start: o.n,
                      window: window };
    let block_max = ZSTD_BLOCK_MAX;
    if window > 0 && window < block_max {
        block_max = window;
    }
    while true {
        if pos + 3 > len(src) {
            return err("kafka: zstd: a truncated block header");
        }
        let bh = src[pos] | (src[pos + 1] << 8) | (src[pos + 2] << 16);
        pos += 3;
        let last = (bh & 1) != 0;
        let kind = (bh >> 1) & 3;
        let size = bh >> 3;
        if kind == 1 {
            if pos >= len(src) {
                return err("kafka: zstd: a truncated RLE block");
            }
            if size > block_max || o.n + size - st.start > max {
                return err("kafka: zstd: a block over the size limit");
            }
            out_room(o, size);
            let v = src[pos];
            for k in 0..size {
                o.b[o.n + k] = v;
            }
            o.n += size;
            pos += 1;
        } else {
            if size > len(src) - pos {
                return err("kafka: zstd: a block runs past the data");
            }
            if kind == 0 {
                if size > block_max || o.n + size - st.start > max {
                    return err("kafka: zstd: a block over the size limit");
                }
                out_copy(o, src, pos, pos + size);
            } else if kind == 2 {
                if size > block_max {
                    return err("kafka: zstd: a compressed block over the block size");
                }
                let br = zstd_block(st, src, pos, pos + size, o, max);
                guard let _b = br else let e = err_of(br) { return err(e); }
            } else {
                return err("kafka: zstd: a reserved block type");
            }
            pos += size;
        }
        if last {
            break;
        }
    }
    if fcs >= 0 && o.n - st.start != fcs {
        return err("kafka: zstd: the frame says " + to_str(fcs) + " bytes and holds " + to_str(o.n - st.start));
    }
    if checksum {
        if pos + 4 > len(src) {
            return err("kafka: zstd: a truncated content checksum");
        }
        let want = le32_at(src, pos);
        let got = xxh64(o.b, st.start, o.n) & 0xffffffff;
        if want != got {
            return err("kafka: zstd: the content fails its checksum");
        }
        pos += 4;
    }
    return ok(pos);
}

// A compressed block src[from..to] onto o.
fn zstd_block(st: ZState, src: bytes, from: int, to: int, o: Out, max: int) -> result[bool, str] {
    // literals
    let pos = from;
    let b0 = src[pos];
    let lt = b0 & 3;
    let sf = (b0 >> 2) & 3;
    let regen = 0;
    let csize = 0;
    let hsize = 0;
    let streams = 1;
    if lt == 0 || lt == 1 {
        if sf == 0 || sf == 2 {
            regen = b0 >> 3;
            hsize = 1;
        } else if sf == 1 {
            if pos + 2 > to {
                return err("kafka: zstd: a truncated literals header");
            }
            regen = (b0 >> 4) + (src[pos + 1] << 4);
            hsize = 2;
        } else {
            if pos + 3 > to {
                return err("kafka: zstd: a truncated literals header");
            }
            regen = (b0 >> 4) + (src[pos + 1] << 4) + (src[pos + 2] << 12);
            hsize = 3;
        }
    } else {
        let bits = 10;
        hsize = 3;
        if sf == 0 {
            streams = 1;
        } else {
            streams = 4;
        }
        if sf == 2 {
            bits = 14;
            hsize = 4;
        } else if sf == 3 {
            bits = 18;
            hsize = 5;
        }
        if pos + hsize > to {
            return err("kafka: zstd: a truncated literals header");
        }
        let v = 0;
        for k in 0..hsize {
            v = v | (src[pos + k] << (8 * k));
        }
        regen = (v >> 4) & ((1 << bits) - 1);
        csize = (v >> (4 + bits)) & ((1 << bits) - 1);
    }
    if regen > ZSTD_BLOCK_MAX {
        return err("kafka: zstd: " + to_str(regen) + " literals in one block");
    }
    pos += hsize;
    let lits = b"";
    if lt == 0 {
        if pos + regen > to {
            return err("kafka: zstd: raw literals run past the block");
        }
        lits = src[pos..pos + regen];
        pos += regen;
    } else if lt == 1 {
        if pos >= to {
            return err("kafka: zstd: a truncated RLE literal");
        }
        lits = strings_fill(regen, src[pos]);
        pos += 1;
    } else {
        if pos + csize > to {
            return err("kafka: zstd: compressed literals run past the block");
        }
        let hfrom = pos;
        if lt == 2 {
            let hr = huf_read(src, pos, pos + csize);
            guard let h = hr else let e = err_of(hr) { return err(e); }
            st.huf = some(h.table);
            hfrom = h.next;
        }
        guard let huf = st.huf else { return err("kafka: zstd: treeless literals without an earlier tree"); }
        let out = strings_fill(regen, 0);
        let end = pos + csize;
        if streams == 1 {
            let sr = huf_stream(huf, src, hfrom, end, out, 0, regen);
            guard let _s = sr else let e = err_of(sr) { return err(e); }
        } else {
            if hfrom + 6 > end {
                return err("kafka: zstd: a truncated jump table");
            }
            let s1 = src[hfrom] | (src[hfrom + 1] << 8);
            let s2 = src[hfrom + 2] | (src[hfrom + 3] << 8);
            let s3 = src[hfrom + 4] | (src[hfrom + 5] << 8);
            let a = hfrom + 6;
            let bb = a + s1;
            let c = bb + s2;
            let d = c + s3;
            if d > end {
                return err("kafka: zstd: a jump table past the literals");
            }
            let seg = (regen + 3) / 4;
            if 3 * seg > regen {
                return err("kafka: zstd: too few literals for four streams");
            }
            let r1 = huf_stream(huf, src, a, bb, out, 0, seg);
            guard let _1 = r1 else let e = err_of(r1) { return err(e); }
            let r2 = huf_stream(huf, src, bb, c, out, seg, seg);
            guard let _2 = r2 else let e = err_of(r2) { return err(e); }
            let r3 = huf_stream(huf, src, c, d, out, 2 * seg, seg);
            guard let _3 = r3 else let e = err_of(r3) { return err(e); }
            let r4 = huf_stream(huf, src, d, end, out, 3 * seg, regen - 3 * seg);
            guard let _4 = r4 else let e = err_of(r4) { return err(e); }
        }
        lits = out;
        pos = end;
    }
    // sequences
    if pos >= to {
        return err("kafka: zstd: a block without its sequences section");
    }
    let nseq = src[pos];
    pos += 1;
    if nseq >= 128 {
        if nseq < 255 {
            if pos >= to {
                return err("kafka: zstd: a truncated sequence count");
            }
            nseq = ((nseq - 128) << 8) + src[pos];
            pos += 1;
        } else {
            if pos + 2 > to {
                return err("kafka: zstd: a truncated sequence count");
            }
            nseq = src[pos] + (src[pos + 1] << 8) + 0x7f00;
            pos += 2;
        }
    }
    if nseq == 0 {
        if pos != to {
            return err("kafka: zstd: bytes after a block's literals");
        }
        if o.n + len(lits) - st.start > max {
            return err("kafka: zstd: more than " + to_str(max) + " bytes decompressed");
        }
        out_copy(o, lits, 0, len(lits));
        return ok(true);
    }
    if pos >= to {
        return err("kafka: zstd: a truncated sequences header");
    }
    let modes = src[pos];
    pos += 1;
    if (modes & 3) != 0 {
        return err("kafka: zstd: reserved sequence mode bits are set");
    }
    let tl = seq_table(st, src, pos, to, modes >> 6, 0);
    guard let lt2 = tl else let e = err_of(tl) { return err(e); }
    st.ll = some(lt2.table);
    pos = lt2.next;
    let to2 = seq_table(st, src, pos, to, (modes >> 4) & 3, 1);
    guard let ot = to2 else let e = err_of(to2) { return err(e); }
    st.of = some(ot.table);
    pos = ot.next;
    let tm = seq_table(st, src, pos, to, (modes >> 2) & 3, 2);
    guard let mt = tm else let e = err_of(tm) { return err(e); }
    st.ml = some(mt.table);
    pos = mt.next;
    return zstd_sequences(st, src, pos, to, nseq, lits, lt2.table, ot.table, mt.table, o, max);
}

fn strings_fill(n: int, v: int) -> bytes {
    let out = strings.bytes_zero(n);
    if v != 0 {
        for i in 0..n {
            out[i] = v;
        }
    }
    return out;
}

// The table for one of literal lengths (which 0), offsets (1) and match
// lengths (2), in mode 0 predefined, 1 RLE, 2 described, 3 repeated.
fn seq_table(st: ZState, src: bytes, pos: int, to: int, mode: int, which: int) -> result[FseRead, str] {
    let max_sym = 35;
    let max_log = 9;
    if which == 1 {
        max_sym = 31;
        max_log = 8;
    } else if which == 2 {
        max_sym = 52;
    }
    if mode == 0 {
        let norm = st.tabs.ll_norm;
        let log = 6;
        if which == 1 {
            norm = st.tabs.of_norm;
            log = 5;
        } else if which == 2 {
            norm = st.tabs.ml_norm;
        }
        let fr = fse_build(norm, log);
        guard let f = fr else let e = err_of(fr) { return err(e); }
        return ok(FseRead { table: f, next: pos });
    }
    if mode == 1 {
        if pos >= to {
            return err("kafka: zstd: a truncated RLE sequence symbol");
        }
        let s = src[pos];
        if s > max_sym {
            return err("kafka: zstd: an RLE sequence symbol of " + to_str(s));
        }
        return ok(FseRead { table: fse_rle(s), next: pos + 1 });
    }
    if mode == 2 {
        return fse_read(src, pos, to, max_sym, max_log);
    }
    let prev: opt[Fse] = st.ll;
    if which == 1 {
        prev = st.of;
    } else if which == 2 {
        prev = st.ml;
    }
    guard let p = prev else { return err("kafka: zstd: a repeated sequence table with none before it"); }
    return ok(FseRead { table: p, next: pos });
}

fn zstd_sequences(st: ZState, src: bytes, pos: int, to: int, nseq: int, lits: bytes, llt: Fse, oft: Fse, mlt: Fse,
                  o: Out, max: int) -> result[bool, str] {
    let br = back_reader(src, pos, to);
    guard let r = br else let e = err_of(br) { return err(e); }
    let tabs = st.tabs;
    let lls = back_read(r, llt.log);
    let ofs = back_read(r, oft.log);
    let mls = back_read(r, mlt.log);
    let lit_pos = 0;
    for k in 0..nseq {
        let ofc = oft.sym[ofs];
        let mlc = mlt.sym[mls];
        let llc = llt.sym[lls];
        if ofc > 31 || mlc > 52 || llc > 35 {
            return err("kafka: zstd: a sequence code out of range");
        }
        let ofv = (1 << ofc) + back_read(r, ofc);
        let ml = tabs.ml_base[mlc] + back_read(r, tabs.ml_bits[mlc]);
        let ll = tabs.ll_base[llc] + back_read(r, tabs.ll_bits[llc]);
        let off = 0;
        if ofv > 3 {
            off = ofv - 3;
            st.rep3 = st.rep2;
            st.rep2 = st.rep1;
            st.rep1 = off;
        } else {
            let idx = ofv - 1;
            if ll == 0 {
                idx += 1;
            }
            if idx == 0 {
                off = st.rep1;
            } else {
                if idx == 1 {
                    off = st.rep2;
                } else if idx == 2 {
                    off = st.rep3;
                } else {
                    off = st.rep1 - 1;
                }
                if idx > 1 {
                    st.rep3 = st.rep2;
                }
                st.rep2 = st.rep1;
                st.rep1 = off;
            }
        }
        if k + 1 < nseq {
            lls = llt.base[lls] + back_read(r, llt.nbits[lls]);
            mls = mlt.base[mls] + back_read(r, mlt.nbits[mls]);
            ofs = oft.base[ofs] + back_read(r, oft.nbits[ofs]);
        }
        if r.pos < 0 {
            return err("kafka: zstd: a sequence bitstream read past its start");
        }
        if ll > len(lits) - lit_pos {
            return err("kafka: zstd: a sequence takes more literals than the block has");
        }
        if o.n + ll + ml - st.start > max {
            return err("kafka: zstd: more than " + to_str(max) + " bytes decompressed");
        }
        out_copy(o, lits, lit_pos, lit_pos + ll);
        lit_pos += ll;
        if off <= 0 || off > o.n - st.start {
            return err("kafka: zstd: a match from before the start");
        }
        out_room(o, ml);
        let d = o.n;
        let s = d - off;
        let b = o.b;
        for i in 0..ml {
            b[d + i] = b[s + i];
        }
        o.n = d + ml;
    }
    if r.pos != 0 {
        return err("kafka: zstd: a sequence bitstream with bits left over");
    }
    if o.n + len(lits) - lit_pos - st.start > max {
        return err("kafka: zstd: more than " + to_str(max) + " bytes decompressed");
    }
    out_copy(o, lits, lit_pos, len(lits));
    return ok(true);
}

// ---- XXH64 ----------------------------------------------------------------------

fn rotl64(x: u64, r: int) -> u64 {
    return (x << r) | (x >> (64 - r));
}

fn le64u(b: bytes, i: int) -> u64 {
    let v = 0 as u64;
    for k in 0..8 {
        v = v | ((b[i + k] as u64) << (8 * k));
    }
    return v;
}

fn xxh64_round(acc: u64, input: u64) -> u64 {
    let q1 = 11400714785074694791;
    let q2 = 14029467366897019727;
    return rotl64(acc + input * q2, 31) * q1;
}

fn xxh64_merge(acc: u64, v: u64) -> u64 {
    let q1 = 11400714785074694791;
    let q4 = 9650029242287828579 as u64;
    return (acc ^ xxh64_round(0 as u64, v)) * q1 + q4;
}

// XXH64 of b[from..to] with seed 0, as an int (two's complement).
pub fn xxh64(b: bytes, from: int, to: int) -> int {
    let q1 = 11400714785074694791;
    let q2 = 14029467366897019727;
    let q3 = 1609587929392839161 as u64;
    let q4 = 9650029242287828579 as u64;
    let q5 = 2870177450012600261 as u64;
    let n = to - from;
    let i = from;
    let h = 0 as u64;
    if n >= 32 {
        let v1 = q1 + q2;
        let v2 = q2;
        let v3 = 0 as u64;
        let v4 = (0 as u64) - q1;
        while i + 32 <= to {
            v1 = xxh64_round(v1, le64u(b, i));
            v2 = xxh64_round(v2, le64u(b, i + 8));
            v3 = xxh64_round(v3, le64u(b, i + 16));
            v4 = xxh64_round(v4, le64u(b, i + 24));
            i += 32;
        }
        h = rotl64(v1, 1) + rotl64(v2, 7) + rotl64(v3, 12) + rotl64(v4, 18);
        h = xxh64_merge(h, v1);
        h = xxh64_merge(h, v2);
        h = xxh64_merge(h, v3);
        h = xxh64_merge(h, v4);
    } else {
        h = q5;
    }
    h = h + (n as u64);
    while i + 8 <= to {
        h = rotl64(h ^ xxh64_round(0 as u64, le64u(b, i)), 27) * q1 + q4;
        i += 8;
    }
    if i + 4 <= to {
        h = rotl64(h ^ ((le32_at(b, i) as u64) * q1), 23) * q2 + q3;
        i += 4;
    }
    while i < to {
        h = rotl64(h ^ ((b[i] as u64) * q5), 11) * q1;
        i += 1;
    }
    h = h ^ (h >> 33);
    h = h * q2;
    h = h ^ (h >> 29);
    h = h * q3;
    h = h ^ (h >> 32);
    return h as int;
}

// ---- encoding ----------------------------------------------------------------------

// The encoding table for normalized counts, as the reference builds it.
gc struct FseEnc {
    log: int,
    state: [int],
    delta_nb: [int],
    delta_find: [int],
}

fn fse_enc(norm: [int], log: int) -> FseEnc {
    let size = 1 << log;
    let high = size - 1;
    let sym: [int] = [];
    for i in 0..size {
        push(sym, 0);
    }
    let cumul: [int] = [0];
    for s in 0..len(norm) {
        if norm[s] == -1 {
            push(cumul, cumul[s] + 1);
            sym[high] = s;
            high -= 1;
        } else {
            push(cumul, cumul[s] + norm[s]);
        }
    }
    let step = (size >> 1) + (size >> 3) + 3;
    let mask = size - 1;
    let pos = 0;
    for s in 0..len(norm) {
        for k in 0..norm[s] {
            sym[pos] = s;
            pos = (pos + step) & mask;
            while pos > high {
                pos = (pos + step) & mask;
            }
        }
    }
    let state: [int] = [];
    for i in 0..size {
        push(state, 0);
    }
    for u in 0..size {
        let s = sym[u];
        state[cumul[s]] = size + u;
        cumul[s] = cumul[s] + 1;
    }
    let dnb: [int] = [];
    let dfs: [int] = [];
    let total = 0;
    for s in 0..len(norm) {
        let c = norm[s];
        if c == 0 {
            push(dnb, ((log + 1) << 16) - size);
            push(dfs, 0);
        } else if c == -1 || c == 1 {
            push(dnb, (log << 16) - size);
            push(dfs, total - 1);
            total += 1;
        } else {
            let mbo = log - highbit(c - 1);
            push(dnb, (mbo << 16) - (c << mbo));
            push(dfs, total - c);
            total += c;
        }
    }
    return FseEnc { log: log, state: state, delta_nb: dnb, delta_find: dfs };
}

// A forward bit writer: bits go in low first, bytes out as they fill.
gc struct BitW {
    o: Out,
    acc: int,
    n: int,
}

fn bw_add(w: BitW, v: int, n: int) {
    if n == 0 {
        return;
    }
    w.acc = w.acc | ((v & ((1 << n) - 1)) << w.n);
    w.n += n;
    while w.n >= 8 {
        out_byte(w.o, w.acc & 255);
        w.acc = w.acc >> 8;
        w.n -= 8;
    }
}

// The end mark, then the last partial byte.
fn bw_close(w: BitW) {
    bw_add(w, 1, 1);
    if w.n > 0 {
        out_byte(w.o, w.acc & 255);
        w.acc = 0;
        w.n = 0;
    }
}

fn fse_init_state(t: FseEnc, s: int) -> int {
    let nbo = (t.delta_nb[s] + 32768) >> 16;
    let value = (nbo << 16) - t.delta_nb[s];
    return t.state[(value >> nbo) + t.delta_find[s]];
}

fn fse_put(w: BitW, t: FseEnc, state: int, s: int) -> int {
    let nbo = (state + t.delta_nb[s]) >> 16;
    bw_add(w, state, nbo);
    return t.state[(state >> nbo) + t.delta_find[s]];
}

fn ll_code(t: ZTabs, ll: int) -> int {
    if ll < 16 {
        return ll;
    }
    let c = 35;
    while t.ll_base[c] > ll {
        c -= 1;
    }
    return c;
}

fn ml_code(t: ZTabs, ml: int) -> int {
    if ml < 35 {
        return ml - 3;
    }
    let c = 52;
    while t.ml_base[c] > ml {
        c -= 1;
    }
    return c;
}

// The encoder's per-call state: tables built once per frame.
gc struct ZEnc {
    tabs: ZTabs,
    ll: FseEnc,
    of: FseEnc,
    ml: FseEnc,
    hash: [int],
}

let ZHASH_BITS = 16;

// data as one zstd frame: single segment, the content size in the
// header, no checksum (a Kafka batch has its own CRC).
pub fn zstd_encode(data: bytes) -> bytes {
    let n = len(data);
    let o = new_out(n + n / 128 + 32);
    out_byte(o, 0x28);
    out_byte(o, 0xb5);
    out_byte(o, 0x2f);
    out_byte(o, 0xfd);
    if n < 256 {
        out_byte(o, 0x20);
        out_byte(o, n);
    } else if n < 65536 + 256 {
        out_byte(o, 0x60);
        out_byte(o, (n - 256) & 255);
        out_byte(o, (n - 256) >> 8);
    } else if n <= 0xffffffff {
        out_byte(o, 0xa0);
        for k in 0..4 {
            out_byte(o, n >> (8 * k));
        }
    } else {
        out_byte(o, 0xe0);
        for k in 0..8 {
            out_byte(o, n >> (8 * k));
        }
    }
    let tabs = ztabs();
    let hash: [int] = [];
    for i in 0..(1 << ZHASH_BITS) {
        push(hash, -1);
    }
    let e = ZEnc { tabs: tabs, ll: fse_enc(tabs.ll_norm, 6), of: fse_enc(tabs.of_norm, 5), ml: fse_enc(tabs.ml_norm, 6),
                   hash: hash };
    if n == 0 {
        // one empty raw block, last
        out_byte(o, 1);
        out_byte(o, 0);
        out_byte(o, 0);
        return out_bytes(o);
    }
    let pos = 0;
    while pos < n {
        let end = pos + ZSTD_BLOCK_MAX;
        if end > n {
            end = n;
        }
        zstd_put_block(e, o, data, pos, end, end == n);
        pos = end;
    }
    return out_bytes(o);
}

fn block_header(o: Out, at: int, last: bool, kind: int, size: int) {
    let v = (size << 3) | (kind << 1);
    if last {
        v = v | 1;
    }
    o.b[at] = v & 255;
    o.b[at + 1] = (v >> 8) & 255;
    o.b[at + 2] = (v >> 16) & 255;
}

fn zstd_put_block(e: ZEnc, o: Out, src: bytes, from: int, to: int, last: bool) {
    let n = to - from;
    // a run of one byte: an RLE block
    let same = true;
    for i in (from + 1)..to {
        if src[i] != src[from] {
            same = false;
            break;
        }
    }
    out_room(o, 3);
    let at = o.n;
    o.n += 3;
    if same {
        out_byte(o, src[from]);
        block_header(o, at, last, 1, n);
        return;
    }
    let body = o.n;
    zstd_compress_block(e, o, src, from, to);
    let size = o.n - body;
    if size >= n {
        o.n = body;
        out_copy(o, src, from, to);
        block_header(o, at, last, 0, n);
        return;
    }
    block_header(o, at, last, 2, size);
}

fn hash4z(v: int) -> int {
    return ((v * 2654435761) & 0xffffffff) >> (32 - ZHASH_BITS);
}

// Greedy matching over src (matches may reach back into earlier blocks
// of the frame), raw literals, and the predefined FSE tables.
fn zstd_compress_block(e: ZEnc, o: Out, src: bytes, from: int, to: int) {
    let lits = new_out(to - from);
    let seq_ll: [int] = [];
    let seq_ml: [int] = [];
    let seq_of: [int] = [];
    let anchor = from;
    let i = from;
    let limit = to - 8;
    let h = e.hash;
    while i < limit {
        let v = le32_at(src, i);
        let hk = hash4z(v);
        let cand = h[hk];
        h[hk] = i;
        if cand >= 0 && i - cand < 0x1000000 && le32_at(src, cand) == v {
            let s = i;
            let c = cand;
            while s > anchor && c > 0 && src[s - 1] == src[c - 1] {
                s -= 1;
                c -= 1;
            }
            let m = i + 4;
            while m < to && src[m] == src[m - i + cand] {
                m += 1;
            }
            let ml = m - s;
            out_copy(lits, src, anchor, s);
            push(seq_ll, s - anchor);
            push(seq_ml, ml);
            push(seq_of, s - c + 3);
            anchor = m;
            i = m;
            if i - 2 > from && i + 4 <= to {
                h[hash4z(le32_at(src, i - 2))] = i - 2;
            }
            continue;
        }
        i += 1 + ((i - anchor) >> 6);
    }
    out_copy(lits, src, anchor, to);
    // literals section: raw
    let ln = lits.n;
    if ln < 32 {
        out_byte(o, ln << 3);
    } else if ln < 4096 {
        out_byte(o, 4 | ((ln & 15) << 4));
        out_byte(o, ln >> 4);
    } else {
        out_byte(o, 12 | ((ln & 15) << 4));
        out_byte(o, (ln >> 4) & 255);
        out_byte(o, ln >> 12);
    }
    out_copy(o, lits.b, 0, ln);
    // sequences section
    let ns = len(seq_ll);
    if ns < 128 {
        out_byte(o, ns);
    } else if ns < 0x7f00 {
        out_byte(o, (ns >> 8) + 128);
        out_byte(o, ns & 255);
    } else {
        out_byte(o, 255);
        out_byte(o, (ns - 0x7f00) & 255);
        out_byte(o, (ns - 0x7f00) >> 8);
    }
    if ns == 0 {
        return;
    }
    // all three tables predefined
    out_byte(o, 0);
    let t = e.tabs;
    let w = BitW { o: o, acc: 0, n: 0 };
    let k = ns - 1;
    let llc = ll_code(t, seq_ll[k]);
    let mlc = ml_code(t, seq_ml[k]);
    let ofc = highbit(seq_of[k]);
    let sml = fse_init_state(e.ml, mlc);
    let sof = fse_init_state(e.of, ofc);
    let sll = fse_init_state(e.ll, llc);
    bw_add(w, seq_ll[k] - t.ll_base[llc], t.ll_bits[llc]);
    bw_add(w, seq_ml[k] - t.ml_base[mlc], t.ml_bits[mlc]);
    bw_add(w, seq_of[k], ofc);
    k -= 1;
    while k >= 0 {
        llc = ll_code(t, seq_ll[k]);
        mlc = ml_code(t, seq_ml[k]);
        ofc = highbit(seq_of[k]);
        sof = fse_put(w, e.of, sof, ofc);
        sml = fse_put(w, e.ml, sml, mlc);
        sll = fse_put(w, e.ll, sll, llc);
        bw_add(w, seq_ll[k] - t.ll_base[llc], t.ll_bits[llc]);
        bw_add(w, seq_ml[k] - t.ml_base[mlc], t.ml_bits[mlc]);
        bw_add(w, seq_of[k], ofc);
        k -= 1;
    }
    bw_add(w, sml, e.ml.log);
    bw_add(w, sof, e.of.log);
    bw_add(w, sll, e.ll.log);
    bw_close(w);
}
