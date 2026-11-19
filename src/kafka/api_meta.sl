// ApiVersions v3, Metadata v9, FindCoordinator v3, SaslHandshake v1 and
// SaslAuthenticate v2.

pub fn wr_int32s(w: Buf, xs: [int]) {
    wr_clen(w, len(xs));
    for x in xs {
        wr_i32(w, x);
    }
}

pub fn rd_int32s(r: Reader) -> [int] {
    let n = rd_clen(r);
    let out: [int] = [];
    for k in 0..n {
        push(out, rd_i32(r));
    }
    return out;
}

pub fn wr_cstrs(w: Buf, xs: [str]) {
    wr_clen(w, len(xs));
    for x in xs {
        wr_cstr(w, x);
    }
}

pub fn rd_cstrs(r: Reader) -> [str] {
    let n = rd_clen(r);
    let out: [str] = [];
    for k in 0..n {
        push(out, rd_cstr(r));
    }
    return out;
}

// ---- ApiVersions --------------------------------------------------------------

pub gc struct ApiRange {
    key: int,
    min: int,
    max: int,
}

pub gc struct ApiVersionsResp {
    error_code: int,
    apis: [ApiRange],
    throttle_ms: int,
}

pub fn enc_api_versions(w: Buf, software: str, version: str) {
    wr_cstr(w, software);
    wr_cstr(w, version);
    wr_tags(w);
}

// The response, in v3's layout, or v0's when the broker answered
// UNSUPPORTED_VERSION in the oldest layout it has (as brokers do).
pub fn dec_api_versions(r: Reader) -> result[ApiVersionsResp, str] {
    let start = r.pos;
    let code = rd_i16(r);
    let out = ApiVersionsResp { error_code: code, apis: [], throttle_ms: 0 };
    if code == ERR_UNSUPPORTED_VERSION {
        let n = rd_len(r);
        for k in 0..n {
            push(out.apis, ApiRange { key: rd_i16(r), min: rd_i16(r), max: rd_i16(r) });
        }
        let dr = done(r, "ApiVersions");
        guard let _d = dr else let e = err_of(dr) { return err(e); }
        return ok(out);
    }
    let n = rd_clen(r);
    for k in 0..n {
        push(out.apis, ApiRange { key: rd_i16(r), min: rd_i16(r), max: rd_i16(r) });
        rd_tags(r);
    }
    out.throttle_ms = rd_i32(r);
    rd_tags(r);
    let dr = done(r, "ApiVersions");
    guard let _d = dr else let e = err_of(dr) { return err(e); }
    return ok(out);
}

// ---- Metadata -----------------------------------------------------------------

pub gc struct Broker {
    node_id: int,
    host: str,
    port: int,
    rack: opt[str],
}

pub gc struct PartitionMeta {
    error_code: int,
    partition: int,
    leader: int,
    leader_epoch: int,
    replicas: [int],
    isr: [int],
    offline: [int],
}

pub gc struct TopicMeta {
    error_code: int,
    name: str,
    internal: bool,
    partitions: [PartitionMeta],
}

pub gc struct MetadataResp {
    throttle_ms: int,
    brokers: [Broker],
    cluster_id: opt[str],
    controller: int,
    topics: [TopicMeta],
}

// topics none: every topic.
pub fn enc_metadata(w: Buf, topics: opt[[str]], auto_create: bool) {
    if let ts = topics {
        wr_clen(w, len(ts));
        for t in ts {
            wr_cstr(w, t);
            wr_tags(w);
        }
    } else {
        wr_uvarint(w, 0);
    }
    wr_bool(w, auto_create);
    wr_bool(w, false);
    wr_bool(w, false);
    wr_tags(w);
}

pub fn dec_metadata(r: Reader) -> result[MetadataResp, str] {
    let out = MetadataResp { throttle_ms: rd_i32(r), brokers: [], cluster_id: none, controller: -1, topics: [] };
    let nb = rd_clen(r);
    for k in 0..nb {
        let b = Broker { node_id: rd_i32(r), host: rd_cstr(r), port: rd_i32(r), rack: rd_cnstr(r) };
        rd_tags(r);
        push(out.brokers, b);
    }
    out.cluster_id = rd_cnstr(r);
    out.controller = rd_i32(r);
    let nt = rd_clen(r);
    for k in 0..nt {
        let t = TopicMeta { error_code: rd_i16(r), name: rd_cstr(r), internal: rd_bool(r), partitions: [] };
        let np = rd_clen(r);
        for j in 0..np {
            let p = PartitionMeta { error_code: rd_i16(r), partition: rd_i32(r), leader: rd_i32(r),
                                    leader_epoch: rd_i32(r), replicas: rd_int32s(r), isr: rd_int32s(r),
                                    offline: rd_int32s(r) };
            rd_tags(r);
            push(t.partitions, p);
        }
        rd_i32(r);
        rd_tags(r);
        push(out.topics, t);
    }
    rd_i32(r);
    rd_tags(r);
    let dr = done(r, "Metadata");
    guard let _d = dr else let e = err_of(dr) { return err(e); }
    return ok(out);
}

// ---- FindCoordinator ------------------------------------------------------------

pub let COORDINATOR_GROUP = 0;
pub let COORDINATOR_TRANSACTION = 1;

pub gc struct CoordinatorResp {
    throttle_ms: int,
    error_code: int,
    error_message: str,
    node_id: int,
    host: str,
    port: int,
}

pub fn enc_find_coordinator(w: Buf, key: str, key_type: int) {
    wr_cstr(w, key);
    wr_u8(w, key_type);
    wr_tags(w);
}

pub fn dec_find_coordinator(r: Reader) -> result[CoordinatorResp, str] {
    let out = CoordinatorResp { throttle_ms: rd_i32(r), error_code: rd_i16(r), error_message: rd_cnstr(r) ?? "",
                                node_id: rd_i32(r), host: rd_cstr(r), port: rd_i32(r) };
    rd_tags(r);
    let dr = done(r, "FindCoordinator");
    guard let _d = dr else let e = err_of(dr) { return err(e); }
    return ok(out);
}

// ---- SASL -------------------------------------------------------------------------

pub gc struct SaslHandshakeResp {
    error_code: int,
    mechanisms: [str],
}

pub fn enc_sasl_handshake(w: Buf, mechanism: str) {
    wr_str(w, mechanism);
}

pub fn dec_sasl_handshake(r: Reader) -> result[SaslHandshakeResp, str] {
    let out = SaslHandshakeResp { error_code: rd_i16(r), mechanisms: [] };
    let n = rd_len(r);
    for k in 0..n {
        push(out.mechanisms, rd_str(r));
    }
    let dr = done(r, "SaslHandshake");
    guard let _d = dr else let e = err_of(dr) { return err(e); }
    return ok(out);
}

pub gc struct SaslAuthResp {
    error_code: int,
    error_message: str,
    auth: bytes,
    session_lifetime_ms: int,
}

pub fn enc_sasl_authenticate(w: Buf, auth: bytes) {
    wr_cbytes(w, auth);
    wr_tags(w);
}

pub fn dec_sasl_authenticate(r: Reader) -> result[SaslAuthResp, str] {
    let out = SaslAuthResp { error_code: rd_i16(r), error_message: rd_cnstr(r) ?? "", auth: rd_cbytes(r),
                             session_lifetime_ms: rd_i64(r) };
    rd_tags(r);
    let dr = done(r, "SaslAuthenticate");
    guard let _d = dr else let e = err_of(dr) { return err(e); }
    return ok(out);
}
