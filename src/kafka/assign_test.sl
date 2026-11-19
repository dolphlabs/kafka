fn sub(id: str, topics: [str], owned: [TopicPartition]) -> Subscription {
    return Subscription { member_id: id, instance_id: none, topics: topics, owned: owned };
}

fn show(tps: [TopicPartition]) -> str {
    let s = "";
    for x in tps {
        s = s + x.topic + to_str(x.partition) + " ";
    }
    return s;
}

fn test_range_matches_java() {
    let counts: map[str]int = {"t": 7, "u": 2};
    let plan = run_assignor("range", [sub("b", ["t", "u"], []), sub("a", ["t", "u"], []), sub("c", ["t"], [])], counts) ??
               {};
    assert(show(plan["a"]) == "t0 t1 t2 u0 ", show(plan["a"]));
    assert(show(plan["b"]) == "t3 t4 u1 ", show(plan["b"]));
    assert(show(plan["c"]) == "t5 t6 ", show(plan["c"]));
}

fn test_roundrobin_skips_members_not_subscribed() {
    let counts: map[str]int = {"t": 3, "u": 3};
    let plan = run_assignor("roundrobin", [sub("a", ["t", "u"], []), sub("b", ["t"], [])], counts) ?? {};
    assert(show(plan["a"]) == "t0 t2 u0 u1 u2 ", show(plan["a"]));
    assert(show(plan["b"]) == "t1 ", show(plan["b"]));
}

fn test_sticky_keeps_what_members_own() {
    let counts: map[str]int = {"t": 6};
    let owned_a = [tp("t", 0), tp("t", 1), tp("t", 2)];
    let owned_b = [tp("t", 3), tp("t", 4), tp("t", 5)];
    // c joins: a and b keep two each, c gets the two they gave up
    let plan = run_assignor("sticky", [sub("a", ["t"], owned_a), sub("b", ["t"], owned_b), sub("c", ["t"], [])], counts)
               ?? {};
    assert(show(plan["a"]) == "t0 t1 ", show(plan["a"]));
    assert(show(plan["b"]) == "t3 t4 ", show(plan["b"]));
    assert(show(plan["c"]) == "t2 t5 ", show(plan["c"]));
    // a partition two members claim goes to one of them
    let both = run_assignor("sticky", [sub("a", ["t"], [tp("t", 0)]), sub("b", ["t"], [tp("t", 0)])], counts) ?? {};
    assert(len(both["a"]) == 3 && len(both["b"]) == 3);
}

fn test_unknown_assignor() {
    let counts: map[str]int = {};
    assert(err_of(run_assignor("cooperative-sticky", [sub("a", ["t"], [])], counts)) ==
           "kafka: the group chose assignor cooperative-sticky, which this client does not have");
}

fn test_subscription_and_assignment_round_trip() {
    let b = encode_subscription(["t", "u"], [tp("t", 1), tp("u", 0), tp("t", 2)]);
    let s = decode_subscription("m", some("i"), b) ?? sub("", [], []);
    assert(len(s.topics) == 2 && s.topics[1] == "u");
    assert(show(s.owned) == "t1 t2 u0 ", show(s.owned));
    let a = decode_assignment(encode_assignment([tp("t", 3), tp("t", 4)])) ?? [];
    assert(show(a) == "t3 t4 ");
    assert(len(decode_assignment(b"") ?? [tp("x", 0)]) == 0);
    assert(err_of(decode_subscription("m", none, b"\x00\x01\x00\x00\x00\x05")) != "");
}
