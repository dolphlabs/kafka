// Request framing and the API versions this package speaks.
//
// A request is a 4-byte size, then a header (api key, api version,
// correlation id, client id as a classic nullable string, and, for
// flexible versions, tagged fields), then the body. A response is a
// 4-byte size, the correlation id, tagged fields for flexible versions
// (except ApiVersions, whose response header never has them), then the
// body.

import "strings";

pub let API_PRODUCE = 0;
pub let API_FETCH = 1;
pub let API_LIST_OFFSETS = 2;
pub let API_METADATA = 3;
pub let API_OFFSET_COMMIT = 8;
pub let API_OFFSET_FETCH = 9;
pub let API_FIND_COORDINATOR = 10;
pub let API_JOIN_GROUP = 11;
pub let API_HEARTBEAT = 12;
pub let API_LEAVE_GROUP = 13;
pub let API_SYNC_GROUP = 14;
pub let API_DESCRIBE_GROUPS = 15;
pub let API_LIST_GROUPS = 16;
pub let API_SASL_HANDSHAKE = 17;
pub let API_API_VERSIONS = 18;
pub let API_CREATE_TOPICS = 19;
pub let API_DELETE_TOPICS = 20;
pub let API_DELETE_RECORDS = 21;
pub let API_INIT_PRODUCER_ID = 22;
pub let API_ADD_PARTITIONS_TO_TXN = 24;
pub let API_ADD_OFFSETS_TO_TXN = 25;
pub let API_END_TXN = 26;
pub let API_TXN_OFFSET_COMMIT = 28;
pub let API_DESCRIBE_CONFIGS = 32;
pub let API_SASL_AUTHENTICATE = 36;
pub let API_CREATE_PARTITIONS = 37;
pub let API_DELETE_GROUPS = 42;
pub let API_INCREMENTAL_ALTER_CONFIGS = 44;

// The one version of each API this package sends (-1: not used).
pub fn api_version(key: int) -> int {
    return switch key {
        case 0 { 9 }
        case 1 { 12 }
        case 2 { 6 }
        case 3 { 9 }
        case 8 { 8 }
        case 9 { 7 }
        case 10 { 3 }
        case 11 { 6 }
        case 12 { 4 }
        case 13 { 4 }
        case 14 { 4 }
        case 15 { 5 }
        case 16 { 3 }
        case 17 { 1 }
        case 18 { 3 }
        case 19 { 5 }
        case 20 { 4 }
        case 21 { 2 }
        case 22 { 4 }
        case 24 { 3 }
        case 25 { 3 }
        case 26 { 3 }
        case 28 { 3 }
        case 32 { 4 }
        case 36 { 2 }
        case 37 { 2 }
        case 42 { 2 }
        case 44 { 1 }
        default { -1 }
    };
}

pub fn api_name(key: int) -> str {
    return switch key {
        case 0 { "Produce" }
        case 1 { "Fetch" }
        case 2 { "ListOffsets" }
        case 3 { "Metadata" }
        case 8 { "OffsetCommit" }
        case 9 { "OffsetFetch" }
        case 10 { "FindCoordinator" }
        case 11 { "JoinGroup" }
        case 12 { "Heartbeat" }
        case 13 { "LeaveGroup" }
        case 14 { "SyncGroup" }
        case 15 { "DescribeGroups" }
        case 16 { "ListGroups" }
        case 17 { "SaslHandshake" }
        case 18 { "ApiVersions" }
        case 19 { "CreateTopics" }
        case 20 { "DeleteTopics" }
        case 21 { "DeleteRecords" }
        case 22 { "InitProducerId" }
        case 24 { "AddPartitionsToTxn" }
        case 25 { "AddOffsetsToTxn" }
        case 26 { "EndTxn" }
        case 28 { "TxnOffsetCommit" }
        case 32 { "DescribeConfigs" }
        case 36 { "SaslAuthenticate" }
        case 37 { "CreatePartitions" }
        case 42 { "DeleteGroups" }
        case 44 { "IncrementalAlterConfigs" }
        default { "api " + to_str(key) }
    };
}

// The older version sent to a broker without api_version(key), for the
// APIs Redpanda serves only in their classic forms (-1: none).
pub fn fallback_version(key: int) -> int {
    return switch key {
        case 0 { 7 }
        case 1 { 11 }
        case 2 { 4 }
        case 3 { 8 }
        case 22 { 3 }
        case 25 { 1 }
        default { -1 }
    };
}

// The first flexible version of each API this package sends.
fn first_flexible(key: int) -> int {
    return switch key {
        case 0 { 9 }
        case 1 { 12 }
        case 2 { 6 }
        case 3 { 9 }
        case 8 { 8 }
        case 9 { 6 }
        case 10 { 3 }
        case 11 { 6 }
        case 12 { 4 }
        case 13 { 4 }
        case 14 { 4 }
        case 15 { 5 }
        case 16 { 3 }
        case 18 { 3 }
        case 19 { 5 }
        case 20 { 4 }
        case 21 { 2 }
        case 22 { 2 }
        case 24 { 3 }
        case 25 { 3 }
        case 26 { 3 }
        case 28 { 3 }
        case 32 { 4 }
        case 36 { 2 }
        case 37 { 2 }
        case 42 { 2 }
        case 44 { 1 }
        default { 1000 }
    };
}

// Whether key at version v uses compact lengths and tagged fields.
pub fn flexible_at(key: int, v: int) -> bool {
    return v >= first_flexible(key);
}

// Whether the version this package prefers for key is flexible (all but
// SaslHandshake v1).
pub fn api_flexible(key: int) -> bool {
    return flexible_at(key, api_version(key));
}

// The whole request, size first: header and body.
pub fn frame_request(key: int, correlation: int, client_id: str, body: Buf) -> Buf {
    return frame_request_v(key, api_version(key), correlation, client_id, body);
}

pub fn frame_request_v(key: int, version: int, correlation: int, client_id: str, body: Buf) -> Buf {
    let h = new_buf();
    wr_i16(h, key);
    wr_i16(h, version);
    wr_i32(h, correlation);
    wr_str(h, client_id);
    if flexible_at(key, version) {
        wr_tags(h);
    }
    let w = new_buf();
    wr_i32(w, h.size() + body.size());
    wr_buf(w, h);
    wr_buf(w, body);
    return w;
}

// A response body's reader: past the correlation id (checked against
// want) and, for flexible responses, the header's tagged fields.
pub fn response_reader(key: int, frame: bytes, want: int) -> result[Reader, str] {
    return response_reader_v(key, api_version(key), frame, want);
}

pub fn response_reader_v(key: int, version: int, frame: bytes, want: int) -> result[Reader, str] {
    let r = reader(frame, 0, len(frame));
    let id = rd_i32(r);
    if !r.good() {
        return err("kafka: a response shorter than its header");
    }
    if id != want {
        return err("kafka: a response for request " + to_str(id) + " where " + to_str(want) + " was next");
    }
    if flexible_at(key, version) && key != API_API_VERSIONS {
        rd_tags(r);
    }
    return ok(r);
}

// A decoder's last check: the body was whole.
fn done(r: Reader, what: str) -> result[bool, str] {
    if r.failed != "" {
        return err("kafka: " + what + " response: " + strip_prefix(r.failed));
    }
    return ok(true);
}

fn strip_prefix(s: str) -> str {
    if len(s) > 7 && strings.has_prefix(s, "kafka: ") {
        return strings.slice(s, 7, len(s));
    }
    return s;
}

// ---- fields in either form ------------------------------------------------------------
//
// The classic (flex false) or compact (flex true) form of a field, for
// the APIs sent at a fallback version.

pub fn wr_s(w: Buf, flex: bool, s: str) {
    if flex {
        wr_cstr(w, s);
    } else {
        wr_str(w, s);
    }
}

pub fn wr_ns(w: Buf, flex: bool, s: opt[str]) {
    if flex {
        wr_cnstr(w, s);
    } else {
        wr_nstr(w, s);
    }
}

pub fn wr_n(w: Buf, flex: bool, n: int) {
    if flex {
        wr_clen(w, n);
    } else {
        wr_len(w, n);
    }
}

// A null array.
pub fn wr_null_arr(w: Buf, flex: bool) {
    if flex {
        wr_uvarint(w, 0);
    } else {
        wr_i32(w, -1);
    }
}

pub fn wr_t(w: Buf, flex: bool) {
    if flex {
        wr_tags(w);
    }
}

pub fn rd_s(r: Reader, flex: bool) -> str {
    if flex {
        return rd_cstr(r);
    }
    return rd_str(r);
}

pub fn rd_ns(r: Reader, flex: bool) -> opt[str] {
    if flex {
        return rd_cnstr(r);
    }
    return rd_nstr(r);
}

pub fn rd_n(r: Reader, flex: bool) -> int {
    if flex {
        return rd_clen(r);
    }
    return rd_len(r);
}

// A nullable array's length: -1 for null.
pub fn rd_nn(r: Reader, flex: bool) -> int {
    if flex {
        let n = rd_uvarint(r);
        if n == 0 {
            return -1;
        }
        if n - 1 > r.left() {
            fail_read(r, "an array of " + to_str(n - 1) + " elements in " + to_str(r.left()) + " bytes");
            return 0;
        }
        return n - 1;
    }
    let n = rd_i32(r);
    if n < 0 {
        return -1;
    }
    if n > r.left() {
        fail_read(r, "an array of " + to_str(n) + " elements in " + to_str(r.left()) + " bytes");
        return 0;
    }
    return n;
}

pub fn rd_t(r: Reader, flex: bool) {
    if flex {
        rd_tags(r);
    }
}

pub fn rd_i32s(r: Reader, flex: bool) -> [int] {
    let n = rd_n(r, flex);
    let out: [int] = [];
    for k in 0..n {
        push(out, rd_i32(r));
    }
    return out;
}

pub fn wr_i32s(w: Buf, flex: bool, xs: [int]) {
    wr_n(w, flex, len(xs));
    for x in xs {
        wr_i32(w, x);
    }
}
