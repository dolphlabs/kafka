// Partitioning: murmur2 of the key, as the Java client (and franz-go,
// librdkafka's murmur2_random) computes it, so a keyed record lands on
// the same partition whichever client produced it.

// Java's Utils.murmur2: 32-bit, seed 0x9747b28c, as a signed int.
pub fn murmur2(data: bytes) -> int {
    let m = 0x5bd1e995;
    let n = len(data);
    let h = (0x9747b28c ^ n) & 0xffffffff;
    let i = 0;
    while i + 4 <= n {
        let k = le32_at(data, i);
        k = mul32(k, m);
        k = k ^ (k >> 24);
        k = mul32(k, m);
        h = mul32(h, m);
        h = h ^ k;
        i += 4;
    }
    let rest = n - i;
    if rest == 3 {
        h = h ^ (data[i + 2] << 16);
    }
    if rest >= 2 {
        h = h ^ (data[i + 1] << 8);
    }
    if rest >= 1 {
        h = h ^ data[i];
        h = mul32(h, m);
    }
    h = h ^ (h >> 13);
    h = mul32(h, m);
    h = h ^ (h >> 15);
    if h >= 2147483648 {
        return h - 4294967296;
    }
    return h;
}

// The partition a key goes to among n.
pub fn key_partition(key: bytes, n: int) -> int {
    return (murmur2(key) & 0x7fffffff) % n;
}
