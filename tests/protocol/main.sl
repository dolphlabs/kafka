// Every request encoded as franz-go's kmsg encodes it (byte for byte),
// and every response kmsg encoded decoded to the values gen/ wrote into
// expected.txt. Run with tests/run.sh.

import "encoding";
import "kafka";
import "strings";

fn unhex(s: str) -> bytes {
    let r = encoding.hex_decode(s);
    guard let b = r else { panic("bad hex"); }
    return b;
}

fn check_req(name: str, w: kafka.Buf) {
    let got = w.finish();
    let reqs = request_vectors();
    let want = unhex(reqs[name]);
    if got == want {
        println("req " + name + " same");
    } else {
        println("req " + name + " DIFF");
        println("  want " + reqs[name]);
        println("  got  " + encoding.hex_encode(got));
    }
}

fn rdr(name: str) -> kafka.Reader {
    println("resp " + name);
    let b = unhex(response_vectors()[name]);
    return kafka.reader(b, 0, len(b));
}

fn ints(xs: [int]) -> str {
    let parts: [str] = [];
    for x in xs {
        push(parts, to_str(x));
    }
    return "[" + strings.join(parts, " ") + "]";
}

fn ns(s: opt[str]) -> str {
    return s ?? "null";
}

fn line(s: str) {
    println("  " + s);
}

fn fail(e: str) {
    line("ERROR " + e);
}

fn commits() -> [kafka.OffsetToCommit] {
    return [kafka.OffsetToCommit { topic: "t1", partition: 0, offset: 100, leader_epoch: 2, metadata: none },
            kafka.OffsetToCommit { topic: "t1", partition: 1, offset: 200, leader_epoch: -1, metadata: some("x") },
            kafka.OffsetToCommit { topic: "t2", partition: 0, offset: 5, leader_epoch: 1, metadata: none }];
}

fn requests() {
    let w = kafka.new_buf();
    kafka.enc_api_versions(w, "slang-kafka", "0.1.0");
    check_req("api_versions", w);
    w = kafka.new_buf();
    kafka.enc_metadata(w, some(["a", "bb"]), false);
    check_req("metadata_topics", w);
    w = kafka.new_buf();
    kafka.enc_metadata(w, none, true);
    check_req("metadata_all", w);
    w = kafka.new_buf();
    kafka.enc_find_coordinator(w, "group-1", kafka.COORDINATOR_GROUP);
    check_req("find_coordinator_group", w);
    w = kafka.new_buf();
    kafka.enc_find_coordinator(w, "txn-1", kafka.COORDINATOR_TRANSACTION);
    check_req("find_coordinator_txn", w);
    w = kafka.new_buf();
    kafka.enc_sasl_handshake(w, "SCRAM-SHA-512");
    check_req("sasl_handshake", w);
    w = kafka.new_buf();
    kafka.enc_sasl_authenticate(w, to_bytes("n,,n=user,r=abc"));
    check_req("sasl_authenticate", w);

    let parts: [kafka.ProducePart] = [];
    for p in [0, 3] {
        let rb = kafka.new_buf();
        kafka.wr_raw(rb, b"\x01\x02\x03");
        kafka.wr_u8(rb, p);
        push(parts, kafka.ProducePart { partition: p, records: rb });
    }
    let rb2 = kafka.new_buf();
    kafka.wr_raw(rb2, to_bytes("batch"));
    let t2 = kafka.ProduceTopic { topic: "t2", parts: [kafka.ProducePart { partition: 1, records: rb2 }] };
    w = kafka.new_buf();
    kafka.enc_produce(w, some("tx"), -1, 30000, [kafka.ProduceTopic { topic: "t1", parts: parts }, t2]);
    check_req("produce", w);
    w = kafka.new_buf();
    kafka.enc_produce(w, none, 1, 1500, [t2]);
    check_req("produce_no_txn", w);

    let fps: [kafka.FetchPart] = [];
    for p in [0, 2] {
        push(fps, kafka.FetchPart { partition: p, leader_epoch: 5, offset: 100 + p, last_fetched_epoch: -1,
                                    max_bytes: 1048576 });
    }
    let f = kafka.FetchReq { max_wait_ms: 500, min_bytes: 1, max_bytes: 52428800, isolation: kafka.READ_COMMITTED,
                             session_id: 7, session_epoch: 3, topics: [kafka.FetchTopic { topic: "t1", parts: fps }],
                             forgotten: [kafka.Forgotten { topic: "t9", partitions: [1, 2] }], rack: "rack-a" };
    w = kafka.new_buf();
    kafka.enc_fetch(w, f);
    check_req("fetch", w);

    w = kafka.new_buf();
    kafka.enc_list_offsets(w, kafka.READ_COMMITTED,
                           [kafka.OffsetQuery { topic: "t1", partition: 0, leader_epoch: 4, timestamp: -1 },
                            kafka.OffsetQuery { topic: "t2", partition: 0, leader_epoch: -1, timestamp: 1700000000000 },
                            kafka.OffsetQuery { topic: "t1", partition: 1, leader_epoch: -1, timestamp: -2 }]);
    check_req("list_offsets", w);

    w = kafka.new_buf();
    kafka.enc_join_group(w, kafka.JoinReq { group: "g", session_timeout_ms: 45000, rebalance_timeout_ms: 300000,
                                            member_id: "", instance_id: some("i-1"), protocol_type: "consumer",
                                            protocols: [kafka.GroupProtocol { name: "range", metadata: to_bytes("meta-range") },
                                                        kafka.GroupProtocol { name: "roundrobin",
                                                                              metadata: to_bytes("meta-roundrobin") }] });
    check_req("join_group", w);
    w = kafka.new_buf();
    kafka.enc_sync_group(w, "g", 3, "m", none,
                         [kafka.MemberAssignment { member_id: "m", assignment: to_bytes("assign-m") },
                          kafka.MemberAssignment { member_id: "n", assignment: to_bytes("assign-n") }]);
    check_req("sync_group", w);
    w = kafka.new_buf();
    kafka.enc_heartbeat(w, "g", 3, "m", some("i"));
    check_req("heartbeat", w);
    w = kafka.new_buf();
    kafka.enc_leave_group(w, "g", "m", none);
    check_req("leave_group", w);
    w = kafka.new_buf();
    kafka.enc_offset_commit(w, "g", 3, "m", none, commits());
    check_req("offset_commit", w);
    w = kafka.new_buf();
    kafka.enc_offset_fetch(w, "g", some([kafka.TopicParts { topic: "t1", partitions: [0, 1] }]), true);
    check_req("offset_fetch", w);
    w = kafka.new_buf();
    kafka.enc_offset_fetch(w, "g", none, false);
    check_req("offset_fetch_all", w);
    w = kafka.new_buf();
    kafka.enc_describe_groups(w, ["g", "h"]);
    check_req("describe_groups", w);
    w = kafka.new_buf();
    kafka.enc_list_groups(w);
    check_req("list_groups", w);
    w = kafka.new_buf();
    kafka.enc_delete_groups(w, ["g"]);
    check_req("delete_groups", w);

    w = kafka.new_buf();
    kafka.enc_create_topics(w, [kafka.NewTopic { name: "t", partitions: 3, replication_factor: 2, assignments: [],
                                                 configs: [kafka.ConfigEntry { name: "cleanup.policy", value: some("compact") },
                                                           kafka.ConfigEntry { name: "retention.ms", value: none }] },
                                kafka.NewTopic { name: "u", partitions: -1, replication_factor: -1,
                                                 assignments: [[1, 2], [2, 3]], configs: [] }], 30000, false);
    check_req("create_topics", w);
    w = kafka.new_buf();
    kafka.enc_delete_topics(w, ["t", "u"], 30000);
    check_req("delete_topics", w);
    w = kafka.new_buf();
    kafka.enc_delete_records(w, [kafka.PartOffset { topic: "t", partition: 0, offset: 100 },
                                 kafka.PartOffset { topic: "u", partition: 0, offset: 5 },
                                 kafka.PartOffset { topic: "t", partition: 1, offset: 200 }], 30000);
    check_req("delete_records", w);
    w = kafka.new_buf();
    kafka.enc_create_partitions(w, [kafka.NewPartitions { topic: "t", count: 6, assignments: [] },
                                    kafka.NewPartitions { topic: "u", count: 4, assignments: [[1, 2], [3, 1]] }], 30000,
                                true);
    check_req("create_partitions", w);
    w = kafka.new_buf();
    kafka.enc_describe_configs(w, [kafka.ConfigResource { kind: kafka.RESOURCE_TOPIC, name: "t", keys: some(["retention.ms"]) },
                                   kafka.ConfigResource { kind: kafka.RESOURCE_BROKER, name: "1", keys: none }]);
    check_req("describe_configs", w);
    w = kafka.new_buf();
    kafka.enc_alter_configs(w, [kafka.ConfigAlteration { kind: kafka.RESOURCE_TOPIC, name: "t",
                                                         ops: [kafka.ConfigOp { name: "retention.ms", op: kafka.CONFIG_SET,
                                                                                value: some("1000") },
                                                               kafka.ConfigOp { name: "cleanup.policy",
                                                                                op: kafka.CONFIG_DELETE, value: none }] }],
                            false);
    check_req("alter_configs", w);
    w = kafka.new_buf();
    kafka.enc_init_producer_id(w, none, 60000, -1, -1);
    check_req("init_producer_id", w);
    w = kafka.new_buf();
    kafka.enc_init_producer_id(w, some("tx"), 60000, 5, 2);
    check_req("init_producer_id_txn", w);
    w = kafka.new_buf();
    kafka.enc_add_partitions_to_txn(w, "tx", 5, 2, [kafka.TopicParts { topic: "t", partitions: [0, 1] },
                                                    kafka.TopicParts { topic: "u", partitions: [3] }]);
    check_req("add_partitions_to_txn", w);
    w = kafka.new_buf();
    kafka.enc_add_offsets_to_txn(w, "tx", 5, 2, "g");
    check_req("add_offsets_to_txn", w);
    w = kafka.new_buf();
    kafka.enc_end_txn(w, "tx", 5, 2, true);
    check_req("end_txn", w);
    w = kafka.new_buf();
    kafka.enc_txn_offset_commit(w, "tx", "g", 5, 2, 3, "m", none, commits());
    check_req("txn_offset_commit", w);

    let body = kafka.new_buf();
    kafka.enc_api_versions(body, "slang-kafka", "0.1.0");
    check_req("frame_api_versions", kafka.frame_request(kafka.API_API_VERSIONS, 7, "client", body));

    // the classic versions sent to Redpanda
    w = kafka.new_buf();
    kafka.enc_metadata_v(w, 8, some(["a", "bb"]), false);
    check_req("metadata_v8", w);
    let rb3 = kafka.new_buf();
    kafka.wr_raw(rb3, to_bytes("batch"));
    w = kafka.new_buf();
    kafka.enc_produce_v(w, 7, some("tx"), -1, 30000,
                        [kafka.ProduceTopic { topic: "t1", parts: [kafka.ProducePart { partition: 3, records: rb3 }] }]);
    check_req("produce_v7", w);
    w = kafka.new_buf();
    kafka.enc_fetch_v(w, 11, kafka.FetchReq { max_wait_ms: 500, min_bytes: 1, max_bytes: 1000,
                                              isolation: kafka.READ_UNCOMMITTED, session_id: 0, session_epoch: -1,
                                              topics: [kafka.FetchTopic { topic: "t1",
                                                                          parts: [kafka.FetchPart { partition: 2,
                                                                                                    leader_epoch: -1,
                                                                                                    offset: 77,
                                                                                                    last_fetched_epoch: -1,
                                                                                                    max_bytes: 4096 }] }],
                                              forgotten: [], rack: "r" });
    check_req("fetch_v11", w);
    w = kafka.new_buf();
    kafka.enc_list_offsets_v(w, 4, kafka.READ_UNCOMMITTED,
                             [kafka.OffsetQuery { topic: "t1", partition: 0, leader_epoch: -1, timestamp: -2 }]);
    check_req("list_offsets_v4", w);
    w = kafka.new_buf();
    kafka.enc_add_offsets_to_txn_v(w, 1, "tx", 5, 2, "g");
    check_req("add_offsets_to_txn_v1", w);
}

fn fallback_responses() {
    let mr = kafka.dec_metadata_v(rdr("metadata_v8"), 8);
    if let m = mr {
        line("cluster=" + ns(m.cluster_id) + " controller=" + to_str(m.controller));
        for b in m.brokers {
            line("broker " + to_str(b.node_id) + " " + b.host + ":" + to_str(b.port) + " rack=" + ns(b.rack));
        }
        for t in m.topics {
            line("topic " + t.name + " error=" + to_str(t.error_code) + " internal=" + to_str(t.internal));
            for p in t.partitions {
                line("  part " + to_str(p.partition) + " error=" + to_str(p.error_code) + " leader=" + to_str(p.leader) +
                     " epoch=" + to_str(p.leader_epoch) + " replicas=" + ints(p.replicas) + " isr=" + ints(p.isr) +
                     " offline=" + ints(p.offline));
            }
        }
    } else let e = err_of(mr) {
        fail(e);
    }
    let pr = kafka.dec_produce_v(rdr("produce_v7"), 7);
    if let p = pr {
        line("throttle=" + to_str(p.throttle_ms));
        for x in p.parts {
            line("part " + x.topic + "/" + to_str(x.partition) + " error=" + to_str(x.error_code) + " base=" +
                 to_str(x.base_offset) + " lat=" + to_str(x.log_append_time) + " lso=" + to_str(x.log_start_offset) +
                 " msg=" + x.error_message);
        }
    } else let e = err_of(pr) {
        fail(e);
    }
    let fr = kafka.dec_fetch_v(rdr("fetch_v11"), 11);
    if let f = fr {
        line("error=" + to_str(f.error_code) + " session=" + to_str(f.session_id));
        for p in f.parts {
            let ab = "null";
            if let list = p.aborted {
                let xs: [str] = [];
                for a in list {
                    push(xs, to_str(a.producer_id) + "@" + to_str(a.first_offset));
                }
                ab = "[" + strings.join(xs, " ") + "]";
            }
            line("part " + p.topic + "/" + to_str(p.partition) + " error=" + to_str(p.error_code) + " hw=" +
                 to_str(p.high_watermark) + " lso=" + to_str(p.last_stable_offset) + " start=" +
                 to_str(p.log_start_offset) + " aborted=" + ab + " preferred=" + to_str(p.preferred_replica) +
                 " records=" + to_str(f.frame[p.records_from..p.records_to]));
        }
    } else let e = err_of(fr) {
        fail(e);
    }
    let lr = kafka.dec_list_offsets_v(rdr("list_offsets_v4"), 4);
    for o in lr ?? [] {
        line("offset " + o.topic + "/" + to_str(o.partition) + " error=" + to_str(o.error_code) + " ts=" +
             to_str(o.timestamp) + " offset=" + to_str(o.offset) + " epoch=" + to_str(o.leader_epoch));
    }
    line("error=" + to_str(kafka.dec_error_only_v(rdr("add_offsets_to_txn_v1"), "AddOffsetsToTxn", false) ?? -999));
}

fn named(name: str) {
    let r = rdr(name);
    let nr = switch name {
        case "delete_groups" { kafka.dec_delete_groups(r) }
        case "delete_topics" { kafka.dec_delete_topics(r) }
        case "create_partitions" { kafka.dec_create_partitions(r) }
        default { kafka.dec_alter_configs(r) }
    };
    guard let xs = nr else let e = err_of(nr) { fail(e); return; }
    for x in xs {
        line("result " + x.name + " error=" + to_str(x.error_code) + " msg=" + x.message);
    }
}

fn responses() {
    for name in ["api_versions", "api_versions_v0"] {
        let ar = kafka.dec_api_versions(rdr(name));
        guard let a = ar else let e = err_of(ar) { fail(e); continue; }
        line("error=" + to_str(a.error_code) + " throttle=" + to_str(a.throttle_ms));
        for x in a.apis {
            line("api " + to_str(x.key) + " " + to_str(x.min) + ".." + to_str(x.max));
        }
    }
    let mr = kafka.dec_metadata(rdr("metadata"));
    if let m = mr {
        line("cluster=" + ns(m.cluster_id) + " controller=" + to_str(m.controller));
        for b in m.brokers {
            line("broker " + to_str(b.node_id) + " " + b.host + ":" + to_str(b.port) + " rack=" + ns(b.rack));
        }
        for t in m.topics {
            line("topic " + t.name + " error=" + to_str(t.error_code) + " internal=" + to_str(t.internal));
            for p in t.partitions {
                line("  part " + to_str(p.partition) + " error=" + to_str(p.error_code) + " leader=" + to_str(p.leader) +
                     " epoch=" + to_str(p.leader_epoch) + " replicas=" + ints(p.replicas) + " isr=" + ints(p.isr) +
                     " offline=" + ints(p.offline));
            }
        }
    } else let e = err_of(mr) {
        fail(e);
    }
    for name in ["find_coordinator_error", "find_coordinator"] {
        let cr = kafka.dec_find_coordinator(rdr(name));
        guard let c = cr else let e = err_of(cr) { fail(e); continue; }
        line("error=" + to_str(c.error_code) + " msg=" + c.error_message + " node=" + to_str(c.node_id) + " " + c.host +
             ":" + to_str(c.port));
    }
    let hr = kafka.dec_sasl_handshake(rdr("sasl_handshake"));
    if let h = hr {
        line("error=" + to_str(h.error_code) + " mechanisms=[" + strings.join(h.mechanisms, " ") + "]");
    } else let e = err_of(hr) {
        fail(e);
    }
    let sr = kafka.dec_sasl_authenticate(rdr("sasl_authenticate"));
    if let s = sr {
        line("error=" + to_str(s.error_code) + " msg=" + s.error_message + " auth=" + to_str(s.auth) + " lifetime=" +
             to_str(s.session_lifetime_ms));
    } else let e = err_of(sr) {
        fail(e);
    }
    let pr = kafka.dec_produce(rdr("produce"));
    if let p = pr {
        line("throttle=" + to_str(p.throttle_ms));
        for x in p.parts {
            line("part " + x.topic + "/" + to_str(x.partition) + " error=" + to_str(x.error_code) + " base=" +
                 to_str(x.base_offset) + " lat=" + to_str(x.log_append_time) + " lso=" + to_str(x.log_start_offset) +
                 " msg=" + x.error_message);
            for re in x.record_errors {
                line("  record " + to_str(re.batch_index) + " " + re.message);
            }
        }
    } else let e = err_of(pr) {
        fail(e);
    }
    let fr = kafka.dec_fetch(rdr("fetch"));
    if let f = fr {
        line("error=" + to_str(f.error_code) + " session=" + to_str(f.session_id));
        for p in f.parts {
            let ab = "null";
            if let list = p.aborted {
                let xs: [str] = [];
                for a in list {
                    push(xs, to_str(a.producer_id) + "@" + to_str(a.first_offset));
                }
                ab = "[" + strings.join(xs, " ") + "]";
            }
            line("part " + p.topic + "/" + to_str(p.partition) + " error=" + to_str(p.error_code) + " hw=" +
                 to_str(p.high_watermark) + " lso=" + to_str(p.last_stable_offset) + " start=" +
                 to_str(p.log_start_offset) + " aborted=" + ab + " preferred=" + to_str(p.preferred_replica) +
                 " records=" + to_str(f.frame[p.records_from..p.records_to]));
        }
    } else let e = err_of(fr) {
        fail(e);
    }
    let lr = kafka.dec_list_offsets(rdr("list_offsets"));
    if let ls = lr {
        for o in ls {
            line("offset " + o.topic + "/" + to_str(o.partition) + " error=" + to_str(o.error_code) + " ts=" +
                 to_str(o.timestamp) + " offset=" + to_str(o.offset) + " epoch=" + to_str(o.leader_epoch));
        }
    } else let e = err_of(lr) {
        fail(e);
    }
    let jr = kafka.dec_join_group(rdr("join_group"));
    if let j = jr {
        line("error=" + to_str(j.error_code) + " generation=" + to_str(j.generation) + " protocol=" + j.protocol +
             " leader=" + j.leader + " member=" + j.member_id);
        for m in j.members {
            line("member " + m.member_id + " instance=" + ns(m.instance_id) + " meta=" + to_str(m.metadata));
        }
    } else let e = err_of(jr) {
        fail(e);
    }
    let syr = kafka.dec_sync_group(rdr("sync_group"));
    if let s = syr {
        line("error=" + to_str(s.error_code) + " assignment=" + to_str(s.assignment));
    } else let e = err_of(syr) {
        fail(e);
    }
    let hbr = kafka.dec_error_only(rdr("heartbeat"), "Heartbeat");
    line("error=" + to_str(hbr ?? -999));
    let lvr = kafka.dec_leave_group(rdr("leave_group"));
    line("error=" + to_str(lvr ?? -999));
    for name in ["offset_commit"] {
        let cr = kafka.dec_part_errors(rdr(name), "OffsetCommit");
        guard let cs = cr else let e = err_of(cr) { fail(e); continue; }
        for c in cs {
            line("part " + c.topic + "/" + to_str(c.partition) + " error=" + to_str(c.error_code));
        }
    }
    let ofr = kafka.dec_offset_fetch(rdr("offset_fetch"));
    if let o = ofr {
        line("error=" + to_str(o.error_code));
        for c in o.offsets {
            line("committed " + c.topic + "/" + to_str(c.partition) + " offset=" + to_str(c.offset) + " epoch=" +
                 to_str(c.leader_epoch) + " meta=" + c.metadata + " error=" + to_str(c.error_code));
        }
    } else let e = err_of(ofr) {
        fail(e);
    }
    let dgr = kafka.dec_describe_groups(rdr("describe_groups"));
    if let gs = dgr {
        for g in gs {
            line("group " + g.group + " error=" + to_str(g.error_code) + " state=" + g.state + " type=" +
                 g.protocol_type + " protocol=" + g.protocol);
            for m in g.members {
                line("  member " + m.member_id + " instance=" + ns(m.instance_id) + " client=" + m.client_id + " host=" +
                     m.client_host + " meta=" + to_str(m.metadata) + " assignment=" + to_str(m.assignment));
            }
        }
    } else let e = err_of(dgr) {
        fail(e);
    }
    let lgr = kafka.dec_list_groups(rdr("list_groups"));
    if let l = lgr {
        line("error=" + to_str(l.error_code));
        for g in l.groups {
            line("group " + g.group + " type=" + g.protocol_type);
        }
    } else let e = err_of(lgr) {
        fail(e);
    }
    named("delete_groups");
    let ctr = kafka.dec_create_topics(rdr("create_topics"));
    if let ts = ctr {
        for t in ts {
            line("topic " + t.name + " error=" + to_str(t.error_code) + " msg=" + t.error_message + " partitions=" +
                 to_str(t.partitions) + " rf=" + to_str(t.replication_factor));
        }
    } else let e = err_of(ctr) {
        fail(e);
    }
    named("delete_topics");
    let drr = kafka.dec_delete_records(rdr("delete_records"));
    if let ds = drr {
        for d in ds {
            line("deleted " + d.topic + "/" + to_str(d.partition) + " low=" + to_str(d.low_watermark) + " error=" +
                 to_str(d.error_code));
        }
    } else let e = err_of(drr) {
        fail(e);
    }
    named("create_partitions");
    let dcr = kafka.dec_describe_configs(rdr("describe_configs"));
    if let ds = dcr {
        for d in ds {
            line("resource " + to_str(d.kind) + " " + d.name + " error=" + to_str(d.error_code) + " msg=" +
                 d.error_message);
            for c in d.configs {
                line("  config " + c.name + "=" + ns(c.value) + " ro=" + to_str(c.read_only) + " source=" +
                     to_str(c.source) + " sensitive=" + to_str(c.sensitive));
            }
        }
    } else let e = err_of(dcr) {
        fail(e);
    }
    named("alter_configs");
    let ipr = kafka.dec_init_producer_id(rdr("init_producer_id"));
    if let ip = ipr {
        line("error=" + to_str(ip.error_code) + " pid=" + to_str(ip.producer_id) + " epoch=" + to_str(ip.epoch));
    } else let e = err_of(ipr) {
        fail(e);
    }
    let apr = kafka.dec_part_errors(rdr("add_partitions_to_txn"), "AddPartitionsToTxn");
    for c in apr ?? [] {
        line("part " + c.topic + "/" + to_str(c.partition) + " error=" + to_str(c.error_code));
    }
    line("error=" + to_str(kafka.dec_error_only(rdr("add_offsets_to_txn"), "AddOffsetsToTxn") ?? -999));
    line("error=" + to_str(kafka.dec_error_only(rdr("end_txn"), "EndTxn") ?? -999));
    let tcr = kafka.dec_part_errors(rdr("txn_offset_commit"), "TxnOffsetCommit");
    for c in tcr ?? [] {
        line("part " + c.topic + "/" + to_str(c.partition) + " error=" + to_str(c.error_code));
    }
}

// "" for ok, else the error
fn why[T](r: result[T, str]) -> str {
    if let _v = r {
        return "";
    }
    return err_of(r);
}

// the decoder for a vector's response, as an error ("" when it decoded)
fn decode_any(name: str, r: kafka.Reader) -> str {
    if name == "metadata_v8" {
        return why(kafka.dec_metadata_v(r, 8));
    }
    if name == "produce_v7" {
        return why(kafka.dec_produce_v(r, 7));
    }
    if name == "fetch_v11" {
        return why(kafka.dec_fetch_v(r, 11));
    }
    if name == "list_offsets_v4" {
        return why(kafka.dec_list_offsets_v(r, 4));
    }
    if name == "add_offsets_to_txn_v1" {
        return why(kafka.dec_error_only_v(r, name, false));
    }
    if strings.has_prefix(name, "api_versions") {
        return why(kafka.dec_api_versions(r));
    }
    return switch name {
        case "metadata" { why(kafka.dec_metadata(r)) }
        case "find_coordinator", "find_coordinator_error" { why(kafka.dec_find_coordinator(r)) }
        case "sasl_handshake" { why(kafka.dec_sasl_handshake(r)) }
        case "sasl_authenticate" { why(kafka.dec_sasl_authenticate(r)) }
        case "produce" { why(kafka.dec_produce(r)) }
        case "fetch" { why(kafka.dec_fetch(r)) }
        case "list_offsets" { why(kafka.dec_list_offsets(r)) }
        case "join_group" { why(kafka.dec_join_group(r)) }
        case "sync_group" { why(kafka.dec_sync_group(r)) }
        case "leave_group" { why(kafka.dec_leave_group(r)) }
        case "offset_commit", "add_partitions_to_txn", "txn_offset_commit" { why(kafka.dec_part_errors(r, name)) }
        case "offset_fetch" { why(kafka.dec_offset_fetch(r)) }
        case "describe_groups" { why(kafka.dec_describe_groups(r)) }
        case "list_groups" { why(kafka.dec_list_groups(r)) }
        case "delete_groups" { why(kafka.dec_delete_groups(r)) }
        case "create_topics" { why(kafka.dec_create_topics(r)) }
        case "delete_topics" { why(kafka.dec_delete_topics(r)) }
        case "delete_records" { why(kafka.dec_delete_records(r)) }
        case "create_partitions" { why(kafka.dec_create_partitions(r)) }
        case "describe_configs" { why(kafka.dec_describe_configs(r)) }
        case "alter_configs" { why(kafka.dec_alter_configs(r)) }
        case "init_producer_id" { why(kafka.dec_init_producer_id(r)) }
        default { why(kafka.dec_error_only(r, name)) }
    };
}

requests();
responses();
fallback_responses();
// every response cut short (by one byte, and in half) is an error
let cut = 0;
let refused = 0;
for name, hex in response_vectors() {
    let b = unhex(hex);
    if decode_any(name, kafka.reader(b, 0, len(b))) != "" {
        println("whole " + name + " refused");
    }
    for end in [len(b) - 1, len(b) / 2] {
        cut += 1;
        if decode_any(name, kafka.reader(b, 0, end)) != "" {
            refused += 1;
        }
    }
}
println("truncated " + to_str(refused) + " of " + to_str(cut) + " responses refused");
