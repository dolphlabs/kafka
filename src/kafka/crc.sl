// CRC-32C (Castagnoli), which covers a record batch from its attributes
// to its end, and CRC-32 (IEEE), which covers a v0/v1 message.
//
// Package globals cannot be lists, so the tables live in a Crc that a
// client builds once and passes down. CRC-32C reads eight bytes per step
// through eight tables ("slicing by 8"), about four times the speed of a
// table lookup per byte.

pub gc struct Crc {
    // eight 256-entry tables, back to back
    c: [int],
    // CRC-32 (IEEE), one table
    ieee: [int],
}

fn table_for(poly: int) -> [int] {
    let t: [int] = [];
    for i in 0..256 {
        let c = i;
        for k in 0..8 {
            if (c & 1) != 0 {
                c = (c >> 1) ^ poly;
            } else {
                c = c >> 1;
            }
        }
        push(t, c);
    }
    return t;
}

pub fn crc_tables() -> Crc {
    let base = table_for(0x82f63b78);
    let c: [int] = [];
    for v in base {
        push(c, v);
    }
    for k in 1..8 {
        for i in 0..256 {
            let prev = c[(k - 1) * 256 + i];
            push(c, (prev >> 8) ^ base[prev & 255]);
        }
    }
    return Crc { c: c, ieee: table_for(0xedb88320) };
}

// CRC-32C of b[from..to], continuing from crc (0 to start).
pub fn crc32c_update(t: Crc, crc: int, b: bytes, from: int, to: int) -> int {
    let tab = t.c;
    let c = crc ^ 0xffffffff;
    let i = from;
    while i + 8 <= to {
        c = c ^ (b[i] | (b[i + 1] << 8) | (b[i + 2] << 16) | (b[i + 3] << 24));
        c = tab[1792 + (c & 255)] ^ tab[1536 + ((c >> 8) & 255)] ^ tab[1280 + ((c >> 16) & 255)] ^
            tab[1024 + (c >> 24)] ^ tab[768 + b[i + 4]] ^ tab[512 + b[i + 5]] ^ tab[256 + b[i + 6]] ^
            tab[b[i + 7]];
        i += 8;
    }
    while i < to {
        c = tab[(c ^ b[i]) & 255] ^ (c >> 8);
        i += 1;
    }
    return c ^ 0xffffffff;
}

pub fn crc32c(t: Crc, b: bytes) -> int {
    return crc32c_update(t, 0, b, 0, len(b));
}

// CRC-32 (IEEE) of b[from..to].
pub fn crc32_ieee(t: Crc, b: bytes, from: int, to: int) -> int {
    let tab = t.ieee;
    let c = 0xffffffff;
    for i in from..to {
        c = tab[(c ^ b[i]) & 255] ^ (c >> 8);
    }
    return c ^ 0xffffffff;
}
