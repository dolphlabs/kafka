// JoinGroup v6, SyncGroup v4, Heartbeat v4, LeaveGroup v4, OffsetCommit
// v8, OffsetFetch v7, DescribeGroups v5, ListGroups v3 and DeleteGroups v2.

pub gc struct GroupProtocol {
    name: str,
    metadata: bytes,
}

pub gc struct JoinReq {
    group: str,
    session_timeout_ms: int,
    rebalance_timeout_ms: int,
    member_id: str,
    instance_id: opt[str],
    protocol_type: str,
    protocols: [GroupProtocol],
}

pub fn enc_join_group(w: Buf, j: JoinReq) {
    wr_cstr(w, j.group);
    wr_i32(w, j.session_timeout_ms);
    wr_i32(w, j.rebalance_timeout_ms);
    wr_cstr(w, j.member_id);
    wr_cnstr(w, j.instance_id);
    wr_cstr(w, j.protocol_type);
    wr_clen(w, len(j.protocols));
    for p in j.protocols {
        wr_cstr(w, p.name);
        wr_cbytes(w, p.metadata);
        wr_tags(w);
    }
    wr_tags(w);
}

pub gc struct JoinMember {
    member_id: str,
    instance_id: opt[str],
    metadata: bytes,
}

pub gc struct JoinResp {
    throttle_ms: int,
    error_code: int,
    generation: int,
    protocol: str,
    leader: str,
    member_id: str,
    members: [JoinMember],
}

pub fn dec_join_group(r: Reader) -> result[JoinResp, str] {
    let out = JoinResp { throttle_ms: rd_i32(r), error_code: rd_i16(r), generation: rd_i32(r), protocol: rd_cstr(r),
                         leader: rd_cstr(r), member_id: rd_cstr(r), members: [] };
    let n = rd_clen(r);
    for k in 0..n {
        push(out.members, JoinMember { member_id: rd_cstr(r), instance_id: rd_cnstr(r), metadata: rd_cbytes(r) });
        rd_tags(r);
    }
    rd_tags(r);
    let dr = done(r, "JoinGroup");
    guard let _d = dr else let e = err_of(dr) { return err(e); }
    return ok(out);
}

pub gc struct MemberAssignment {
    member_id: str,
    assignment: bytes,
}

pub fn enc_sync_group(w: Buf, group: str, generation: int, member_id: str, instance_id: opt[str],
                      assignments: [MemberAssignment]) {
    wr_cstr(w, group);
    wr_i32(w, generation);
    wr_cstr(w, member_id);
    wr_cnstr(w, instance_id);
    wr_clen(w, len(assignments));
    for a in assignments {
        wr_cstr(w, a.member_id);
        wr_cbytes(w, a.assignment);
        wr_tags(w);
    }
    wr_tags(w);
}

pub gc struct SyncResp {
    throttle_ms: int,
    error_code: int,
    assignment: bytes,
}

pub fn dec_sync_group(r: Reader) -> result[SyncResp, str] {
    let out = SyncResp { throttle_ms: rd_i32(r), error_code: rd_i16(r), assignment: rd_cbytes(r) };
    rd_tags(r);
    let dr = done(r, "SyncGroup");
    guard let _d = dr else let e = err_of(dr) { return err(e); }
    return ok(out);
}

pub fn enc_heartbeat(w: Buf, group: str, generation: int, member_id: str, instance_id: opt[str]) {
    wr_cstr(w, group);
    wr_i32(w, generation);
    wr_cstr(w, member_id);
    wr_cnstr(w, instance_id);
    wr_tags(w);
}

// The error code of a response that is throttle time, error code and
// tagged fields (Heartbeat, AddOffsetsToTxn, EndTxn).
pub fn dec_error_only(r: Reader, what: str) -> result[int, str] {
    rd_i32(r);
    let code = rd_i16(r);
    rd_tags(r);
    let dr = done(r, what);
    guard let _d = dr else let e = err_of(dr) { return err(e); }
    return ok(code);
}

pub fn enc_leave_group(w: Buf, group: str, member_id: str, instance_id: opt[str]) {
    wr_cstr(w, group);
    wr_clen(w, 1);
    wr_cstr(w, member_id);
    wr_cnstr(w, instance_id);
    wr_tags(w);
    wr_tags(w);
}

// The group's error code, or the member's when the group's is 0.
pub fn dec_leave_group(r: Reader) -> result[int, str] {
    rd_i32(r);
    let code = rd_i16(r);
    let n = rd_clen(r);
    for k in 0..n {
        rd_cstr(r);
        rd_cnstr(r);
        let mc = rd_i16(r);
        if code == 0 {
            code = mc;
        }
        rd_tags(r);
    }
    rd_tags(r);
    let dr = done(r, "LeaveGroup");
    guard let _d = dr else let e = err_of(dr) { return err(e); }
    return ok(code);
}

// ---- offsets ------------------------------------------------------------------------

pub gc struct OffsetToCommit {
    topic: str,
    partition: int,
    offset: int,
    leader_epoch: int,
    metadata: opt[str],
}

fn group_commits(cs: [OffsetToCommit]) -> [[OffsetToCommit]] {
    let out: [[OffsetToCommit]] = [];
    let at: map[str]int = {};
    for c in cs {
        if !has(at, c.topic) {
            at[c.topic] = len(out);
            let fresh: [OffsetToCommit] = [];
            push(out, fresh);
        }
        push(out[at[c.topic]], c);
    }
    return out;
}

fn wr_commits(w: Buf, cs: [OffsetToCommit]) {
    let topics = group_commits(cs);
    wr_clen(w, len(topics));
    for t in topics {
        wr_cstr(w, t[0].topic);
        wr_clen(w, len(t));
        for c in t {
            wr_i32(w, c.partition);
            wr_i64(w, c.offset);
            wr_i32(w, c.leader_epoch);
            wr_cnstr(w, c.metadata);
            wr_tags(w);
        }
        wr_tags(w);
    }
}

pub fn enc_offset_commit(w: Buf, group: str, generation: int, member_id: str, instance_id: opt[str],
                         cs: [OffsetToCommit]) {
    wr_cstr(w, group);
    wr_i32(w, generation);
    wr_cstr(w, member_id);
    wr_cnstr(w, instance_id);
    wr_commits(w, cs);
    wr_tags(w);
}

pub gc struct PartError {
    topic: str,
    partition: int,
    error_code: int,
}

// OffsetCommit's and TxnOffsetCommit's response: per-partition codes.
pub fn dec_part_errors(r: Reader, what: str) -> result[[PartError], str] {
    rd_i32(r);
    let out: [PartError] = [];
    let nt = rd_clen(r);
    for k in 0..nt {
        let topic = rd_cstr(r);
        let np = rd_clen(r);
        for j in 0..np {
            push(out, PartError { topic: topic, partition: rd_i32(r), error_code: rd_i16(r) });
            rd_tags(r);
        }
        rd_tags(r);
    }
    rd_tags(r);
    let dr = done(r, what);
    guard let _d = dr else let e = err_of(dr) { return err(e); }
    return ok(out);
}

pub gc struct TopicParts {
    topic: str,
    partitions: [int],
}

// parts none: every partition the group has committed.
pub fn enc_offset_fetch(w: Buf, group: str, parts: opt[[TopicParts]], require_stable: bool) {
    wr_cstr(w, group);
    if let ps = parts {
        wr_clen(w, len(ps));
        for t in ps {
            wr_cstr(w, t.topic);
            wr_int32s(w, t.partitions);
            wr_tags(w);
        }
    } else {
        wr_uvarint(w, 0);
    }
    wr_bool(w, require_stable);
    wr_tags(w);
}

pub gc struct CommittedOffset {
    topic: str,
    partition: int,
    // -1: nothing committed
    offset: int,
    leader_epoch: int,
    metadata: str,
    error_code: int,
}

pub gc struct OffsetFetchResp {
    offsets: [CommittedOffset],
    error_code: int,
}

pub fn dec_offset_fetch(r: Reader) -> result[OffsetFetchResp, str] {
    rd_i32(r);
    let out = OffsetFetchResp { offsets: [], error_code: 0 };
    let nt = rd_clen(r);
    for k in 0..nt {
        let topic = rd_cstr(r);
        let np = rd_clen(r);
        for j in 0..np {
            push(out.offsets, CommittedOffset { topic: topic, partition: rd_i32(r), offset: rd_i64(r),
                                                leader_epoch: rd_i32(r), metadata: rd_cnstr(r) ?? "",
                                                error_code: rd_i16(r) });
            rd_tags(r);
        }
        rd_tags(r);
    }
    out.error_code = rd_i16(r);
    rd_tags(r);
    let dr = done(r, "OffsetFetch");
    guard let _d = dr else let e = err_of(dr) { return err(e); }
    return ok(out);
}

// ---- describing and listing groups -----------------------------------------------------

pub gc struct GroupMember {
    member_id: str,
    instance_id: opt[str],
    client_id: str,
    client_host: str,
    metadata: bytes,
    assignment: bytes,
}

pub gc struct GroupDescription {
    error_code: int,
    group: str,
    state: str,
    protocol_type: str,
    protocol: str,
    members: [GroupMember],
}

pub fn enc_describe_groups(w: Buf, groups: [str]) {
    wr_cstrs(w, groups);
    wr_bool(w, false);
    wr_tags(w);
}

pub fn dec_describe_groups(r: Reader) -> result[[GroupDescription], str] {
    rd_i32(r);
    let out: [GroupDescription] = [];
    let n = rd_clen(r);
    for k in 0..n {
        let g = GroupDescription { error_code: rd_i16(r), group: rd_cstr(r), state: rd_cstr(r),
                                   protocol_type: rd_cstr(r), protocol: rd_cstr(r), members: [] };
        let nm = rd_clen(r);
        for j in 0..nm {
            push(g.members, GroupMember { member_id: rd_cstr(r), instance_id: rd_cnstr(r), client_id: rd_cstr(r),
                                          client_host: rd_cstr(r), metadata: rd_cbytes(r), assignment: rd_cbytes(r) });
            rd_tags(r);
        }
        rd_i32(r);
        rd_tags(r);
        push(out, g);
    }
    rd_tags(r);
    let dr = done(r, "DescribeGroups");
    guard let _d = dr else let e = err_of(dr) { return err(e); }
    return ok(out);
}

pub gc struct ListedGroup {
    group: str,
    protocol_type: str,
}

pub gc struct ListGroupsResp {
    error_code: int,
    groups: [ListedGroup],
}

pub fn enc_list_groups(w: Buf) {
    wr_tags(w);
}

pub fn dec_list_groups(r: Reader) -> result[ListGroupsResp, str] {
    rd_i32(r);
    let out = ListGroupsResp { error_code: rd_i16(r), groups: [] };
    let n = rd_clen(r);
    for k in 0..n {
        push(out.groups, ListedGroup { group: rd_cstr(r), protocol_type: rd_cstr(r) });
        rd_tags(r);
    }
    rd_tags(r);
    let dr = done(r, "ListGroups");
    guard let _d = dr else let e = err_of(dr) { return err(e); }
    return ok(out);
}

pub gc struct NamedError {
    name: str,
    error_code: int,
    message: str,
}

pub fn enc_delete_groups(w: Buf, groups: [str]) {
    wr_cstrs(w, groups);
    wr_tags(w);
}

pub fn dec_delete_groups(r: Reader) -> result[[NamedError], str] {
    rd_i32(r);
    let out: [NamedError] = [];
    let n = rd_clen(r);
    for k in 0..n {
        push(out, NamedError { name: rd_cstr(r), error_code: rd_i16(r), message: "" });
        rd_tags(r);
    }
    rd_tags(r);
    let dr = done(r, "DeleteGroups");
    guard let _d = dr else let e = err_of(dr) { return err(e); }
    return ok(out);
}
