fn test_murmur2_matches_java() {
    // Kafka's UtilsTest.testMurmur2
    assert(murmur2(to_bytes("21")) == -973932308);
    assert(murmur2(to_bytes("foobar")) == -790332482);
    assert(murmur2(to_bytes("a-little-bit-long-string")) == -985981536);
    assert(murmur2(to_bytes("a-little-bit-longer-string")) == -1486304829);
    assert(murmur2(to_bytes("lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8")) == -58897971);
    assert(murmur2(b"abc") == 479470107);
}

fn test_key_partition_is_stable() {
    assert(key_partition(to_bytes("order-17"), 12) == key_partition(to_bytes("order-17"), 12));
    for n in [1, 3, 7, 100] {
        let k = key_partition(to_bytes("x"), n);
        assert(k >= 0 && k < n);
    }
}
