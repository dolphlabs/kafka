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

// Every version this package sends is flexible but SaslHandshake v1.
pub fn api_flexible(key: int) -> bool {
    return key != API_SASL_HANDSHAKE;
}

// The whole request, size first: header and body.
pub fn frame_request(key: int, correlation: int, client_id: str, body: Buf) -> Buf {
    let h = new_buf();
    wr_i16(h, key);
    wr_i16(h, api_version(key));
    wr_i32(h, correlation);
    wr_str(h, client_id);
    if api_flexible(key) {
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
    let r = reader(frame, 0, len(frame));
    let id = rd_i32(r);
    if !r.good() {
        return err("kafka: a response shorter than its header");
    }
    if id != want {
        return err("kafka: a response for request " + to_str(id) + " where " + to_str(want) + " was next");
    }
    if api_flexible(key) && key != API_API_VERSIONS {
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
