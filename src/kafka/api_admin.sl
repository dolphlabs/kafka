// CreateTopics v5, DeleteTopics v4, DeleteRecords v2, CreatePartitions
// v2, DescribeConfigs v4 and IncrementalAlterConfigs v1; and the
// transaction calls InitProducerId v4, AddPartitionsToTxn v3,
// AddOffsetsToTxn v3, EndTxn v3 and TxnOffsetCommit v3.

pub gc struct ConfigEntry {
    name: str,
    value: opt[str],
}

pub gc struct NewTopic {
    name: str,
    // -1: the broker's default (with no assignments)
    partitions: int,
    replication_factor: int,
    // replica brokers per partition, in partition order (empty: the
    // broker places them)
    assignments: [[int]],
    configs: [ConfigEntry],
}

pub fn enc_create_topics(w: Buf, topics: [NewTopic], timeout_ms: int, validate_only: bool) {
    wr_clen(w, len(topics));
    for t in topics {
        wr_cstr(w, t.name);
        wr_i32(w, t.partitions);
        wr_i16(w, t.replication_factor);
        wr_clen(w, len(t.assignments));
        for i in 0..len(t.assignments) {
            wr_i32(w, i);
            wr_int32s(w, t.assignments[i]);
            wr_tags(w);
        }
        wr_clen(w, len(t.configs));
        for c in t.configs {
            wr_cstr(w, c.name);
            wr_cnstr(w, c.value);
            wr_tags(w);
        }
        wr_tags(w);
    }
    wr_i32(w, timeout_ms);
    wr_bool(w, validate_only);
    wr_tags(w);
}

pub gc struct CreatedTopic {
    name: str,
    error_code: int,
    error_message: str,
    partitions: int,
    replication_factor: int,
}

pub fn dec_create_topics(r: Reader) -> result[[CreatedTopic], str] {
    rd_i32(r);
    let out: [CreatedTopic] = [];
    let n = rd_clen(r);
    for k in 0..n {
        let t = CreatedTopic { name: rd_cstr(r), error_code: rd_i16(r), error_message: rd_cnstr(r) ?? "",
                               partitions: rd_i32(r), replication_factor: rd_i16(r) };
        let nc = rd_uvarint(r);
        if nc > 0 {
            for c in 0..(nc - 1) {
                if r.failed != "" {
                    break;
                }
                rd_cstr(r);
                rd_cnstr(r);
                rd_bool(r);
                rd_u8(r);
                rd_bool(r);
                rd_tags(r);
            }
        }
        rd_tags(r);
        push(out, t);
    }
    rd_tags(r);
    let dr = done(r, "CreateTopics");
    guard let _d = dr else let e = err_of(dr) { return err(e); }
    return ok(out);
}

pub fn enc_delete_topics(w: Buf, names: [str], timeout_ms: int) {
    wr_cstrs(w, names);
    wr_i32(w, timeout_ms);
    wr_tags(w);
}

pub fn dec_delete_topics(r: Reader) -> result[[NamedError], str] {
    rd_i32(r);
    let out: [NamedError] = [];
    let n = rd_clen(r);
    for k in 0..n {
        push(out, NamedError { name: rd_cstr(r), error_code: rd_i16(r), message: "" });
        rd_tags(r);
    }
    rd_tags(r);
    let dr = done(r, "DeleteTopics");
    guard let _d = dr else let e = err_of(dr) { return err(e); }
    return ok(out);
}

pub gc struct PartOffset {
    topic: str,
    partition: int,
    offset: int,
}

fn group_part_offsets(ps: [PartOffset]) -> [[PartOffset]] {
    let out: [[PartOffset]] = [];
    let at: map[str]int = {};
    for p in ps {
        if !has(at, p.topic) {
            at[p.topic] = len(out);
            let fresh: [PartOffset] = [];
            push(out, fresh);
        }
        push(out[at[p.topic]], p);
    }
    return out;
}

// Deletes records before each offset.
pub fn enc_delete_records(w: Buf, ps: [PartOffset], timeout_ms: int) {
    let topics = group_part_offsets(ps);
    wr_clen(w, len(topics));
    for t in topics {
        wr_cstr(w, t[0].topic);
        wr_clen(w, len(t));
        for p in t {
            wr_i32(w, p.partition);
            wr_i64(w, p.offset);
            wr_tags(w);
        }
        wr_tags(w);
    }
    wr_i32(w, timeout_ms);
    wr_tags(w);
}

pub gc struct DeletedRecords {
    topic: str,
    partition: int,
    low_watermark: int,
    error_code: int,
}

pub fn dec_delete_records(r: Reader) -> result[[DeletedRecords], str] {
    rd_i32(r);
    let out: [DeletedRecords] = [];
    let nt = rd_clen(r);
    for k in 0..nt {
        let topic = rd_cstr(r);
        let np = rd_clen(r);
        for j in 0..np {
            push(out, DeletedRecords { topic: topic, partition: rd_i32(r), low_watermark: rd_i64(r),
                                       error_code: rd_i16(r) });
            rd_tags(r);
        }
        rd_tags(r);
    }
    rd_tags(r);
    let dr = done(r, "DeleteRecords");
    guard let _d = dr else let e = err_of(dr) { return err(e); }
    return ok(out);
}

pub gc struct NewPartitions {
    topic: str,
    // the new total
    count: int,
    // replica brokers for each added partition (empty: the broker places
    // them)
    assignments: [[int]],
}

pub fn enc_create_partitions(w: Buf, ps: [NewPartitions], timeout_ms: int, validate_only: bool) {
    wr_clen(w, len(ps));
    for p in ps {
        wr_cstr(w, p.topic);
        wr_i32(w, p.count);
        if len(p.assignments) == 0 {
            wr_uvarint(w, 0);
        } else {
            wr_clen(w, len(p.assignments));
            for a in p.assignments {
                wr_int32s(w, a);
                wr_tags(w);
            }
        }
        wr_tags(w);
    }
    wr_i32(w, timeout_ms);
    wr_bool(w, validate_only);
    wr_tags(w);
}

// CreatePartitions' and IncrementalAlterConfigs' results: a name, a code
// and a message each.
pub fn dec_create_partitions(r: Reader) -> result[[NamedError], str] {
    rd_i32(r);
    let out: [NamedError] = [];
    let n = rd_clen(r);
    for k in 0..n {
        push(out, NamedError { name: rd_cstr(r), error_code: rd_i16(r), message: rd_cnstr(r) ?? "" });
        rd_tags(r);
    }
    rd_tags(r);
    let dr = done(r, "CreatePartitions");
    guard let _d = dr else let e = err_of(dr) { return err(e); }
    return ok(out);
}

// ---- configs ---------------------------------------------------------------------

pub let RESOURCE_TOPIC = 2;
pub let RESOURCE_GROUP = 3;
pub let RESOURCE_BROKER = 4;

pub gc struct ConfigResource {
    kind: int,
    name: str,
    // none: every key
    keys: opt[[str]],
}

pub fn enc_describe_configs(w: Buf, rs: [ConfigResource]) {
    wr_clen(w, len(rs));
    for r in rs {
        wr_u8(w, r.kind);
        wr_cstr(w, r.name);
        if let ks = r.keys {
            wr_cstrs(w, ks);
        } else {
            wr_uvarint(w, 0);
        }
        wr_tags(w);
    }
    wr_bool(w, false);
    wr_bool(w, false);
    wr_tags(w);
}

pub gc struct ConfigValue {
    name: str,
    value: opt[str],
    read_only: bool,
    source: int,
    sensitive: bool,
}

pub gc struct DescribedConfigs {
    error_code: int,
    error_message: str,
    kind: int,
    name: str,
    configs: [ConfigValue],
}

pub fn dec_describe_configs(r: Reader) -> result[[DescribedConfigs], str] {
    rd_i32(r);
    let out: [DescribedConfigs] = [];
    let n = rd_clen(r);
    for k in 0..n {
        let d = DescribedConfigs { error_code: rd_i16(r), error_message: rd_cnstr(r) ?? "", kind: rd_u8(r),
                                   name: rd_cstr(r), configs: [] };
        let nc = rd_clen(r);
        for j in 0..nc {
            let c = ConfigValue { name: rd_cstr(r), value: rd_cnstr(r), read_only: rd_bool(r), source: rd_i8(r),
                                  sensitive: rd_bool(r) };
            let ns = rd_clen(r);
            for s in 0..ns {
                rd_cstr(r);
                rd_cnstr(r);
                rd_u8(r);
                rd_tags(r);
            }
            rd_u8(r);
            rd_cnstr(r);
            rd_tags(r);
            push(d.configs, c);
        }
        rd_tags(r);
        push(out, d);
    }
    rd_tags(r);
    let dr = done(r, "DescribeConfigs");
    guard let _d = dr else let e = err_of(dr) { return err(e); }
    return ok(out);
}

pub let CONFIG_SET = 0;
pub let CONFIG_DELETE = 1;
pub let CONFIG_APPEND = 2;
pub let CONFIG_SUBTRACT = 3;

pub gc struct ConfigOp {
    name: str,
    op: int,
    value: opt[str],
}

pub gc struct ConfigAlteration {
    kind: int,
    name: str,
    ops: [ConfigOp],
}

pub fn enc_alter_configs(w: Buf, alts: [ConfigAlteration], validate_only: bool) {
    wr_clen(w, len(alts));
    for a in alts {
        wr_u8(w, a.kind);
        wr_cstr(w, a.name);
        wr_clen(w, len(a.ops));
        for o in a.ops {
            wr_cstr(w, o.name);
            wr_u8(w, o.op);
            wr_cnstr(w, o.value);
            wr_tags(w);
        }
        wr_tags(w);
    }
    wr_bool(w, validate_only);
    wr_tags(w);
}

pub fn dec_alter_configs(r: Reader) -> result[[NamedError], str] {
    rd_i32(r);
    let out: [NamedError] = [];
    let n = rd_clen(r);
    for k in 0..n {
        let code = rd_i16(r);
        let msg = rd_cnstr(r) ?? "";
        rd_u8(r);
        push(out, NamedError { name: rd_cstr(r), error_code: code, message: msg });
        rd_tags(r);
    }
    rd_tags(r);
    let dr = done(r, "IncrementalAlterConfigs");
    guard let _d = dr else let e = err_of(dr) { return err(e); }
    return ok(out);
}

// ---- transactions -------------------------------------------------------------------

pub fn enc_init_producer_id(w: Buf, txn_id: opt[str], txn_timeout_ms: int, producer_id: int, epoch: int) {
    wr_cnstr(w, txn_id);
    wr_i32(w, txn_timeout_ms);
    wr_i64(w, producer_id);
    wr_i16(w, epoch);
    wr_tags(w);
}

pub gc struct ProducerIdResp {
    throttle_ms: int,
    error_code: int,
    producer_id: int,
    epoch: int,
}

pub fn dec_init_producer_id(r: Reader) -> result[ProducerIdResp, str] {
    let out = ProducerIdResp { throttle_ms: rd_i32(r), error_code: rd_i16(r), producer_id: rd_i64(r),
                               epoch: rd_i16(r) };
    rd_tags(r);
    let dr = done(r, "InitProducerId");
    guard let _d = dr else let e = err_of(dr) { return err(e); }
    return ok(out);
}

pub fn enc_add_partitions_to_txn(w: Buf, txn_id: str, producer_id: int, epoch: int, parts: [TopicParts]) {
    wr_cstr(w, txn_id);
    wr_i64(w, producer_id);
    wr_i16(w, epoch);
    wr_clen(w, len(parts));
    for t in parts {
        wr_cstr(w, t.topic);
        wr_int32s(w, t.partitions);
        wr_tags(w);
    }
    wr_tags(w);
}

pub fn enc_add_offsets_to_txn(w: Buf, txn_id: str, producer_id: int, epoch: int, group: str) {
    wr_cstr(w, txn_id);
    wr_i64(w, producer_id);
    wr_i16(w, epoch);
    wr_cstr(w, group);
    wr_tags(w);
}

pub fn enc_end_txn(w: Buf, txn_id: str, producer_id: int, epoch: int, commit: bool) {
    wr_cstr(w, txn_id);
    wr_i64(w, producer_id);
    wr_i16(w, epoch);
    wr_bool(w, commit);
    wr_tags(w);
}

pub fn enc_txn_offset_commit(w: Buf, txn_id: str, group: str, producer_id: int, epoch: int, generation: int,
                             member_id: str, instance_id: opt[str], cs: [OffsetToCommit]) {
    wr_cstr(w, txn_id);
    wr_cstr(w, group);
    wr_i64(w, producer_id);
    wr_i16(w, epoch);
    wr_i32(w, generation);
    wr_cstr(w, member_id);
    wr_cnstr(w, instance_id);
    wr_commits(w, cs);
    wr_tags(w);
}
