// SASL over SaslHandshake v1 and SaslAuthenticate v2: PLAIN, SCRAM-SHA-256
// and SCRAM-SHA-512 (RFC 5802, RFC 7677), and OAUTHBEARER (RFC 7628).
//
// SCRAM never sends the password: the client proves it knows the salted
// password, and checks the server's signature in turn, so a server that
// does not hold the credentials cannot complete the exchange.

import "crypto";
import "encoding";
import "strings";

let SCRAM_MIN_ITERATIONS = 4096;
let SCRAM_MAX_ITERATIONS = 1000000;

fn authenticate(c: Conn, deadline: until) -> result[bool, str] {
    let s = c.cfg.sasl;
    let mech = s.mechanism;
    if mech == "PLAIN" && !c.cfg.tls && !c.cfg.sasl_plain_without_tls {
        return err("kafka: SASL PLAIN sends the password in the clear: use TLS, or set sasl_plain_without_tls");
    }
    if mech != "PLAIN" && mech != "SCRAM-SHA-256" && mech != "SCRAM-SHA-512" && mech != "OAUTHBEARER" {
        return err("kafka: unknown SASL mechanism " + mech);
    }
    let hb = new_buf();
    enc_sasl_handshake(hb, mech);
    let hr = sync_call(c, API_SASL_HANDSHAKE, hb, deadline);
    guard let r = hr else let e = err_of(hr) { return err(e); }
    let dr = dec_sasl_handshake(r);
    guard let h = dr else let e = err_of(dr) { return err(e); }
    if h.error_code != 0 {
        return err("kafka: broker " + c.host + " does not offer SASL " + mech + " (it offers " +
                   strings.join(h.mechanisms, ", ") + ")");
    }
    if mech == "PLAIN" {
        let ar = sasl_round(c, b"\x00" + to_bytes(s.user) + b"\x00" + to_bytes(s.pass), deadline);
        guard let _a = ar else let e = err_of(ar) { return err(e); }
        return ok(true);
    }
    if mech == "OAUTHBEARER" {
        let ar = sasl_round(c, to_bytes("n,,") + b"\x01" + to_bytes("auth=Bearer " + s.token) + b"\x01\x01", deadline);
        guard let _a = ar else let e = err_of(ar) { return err(e); }
        return ok(true);
    }
    let nr = crypto.rand(18);
    guard let nb = nr else let e = err_of(nr) { return err("kafka: SASL nonce: " + e); }
    let cnonce = encoding.base64_encode(nb);
    let first = scram_client_first(s.user, cnonce);
    let sr = sasl_round(c, to_bytes("n,," + first), deadline);
    guard let server_first = sr else let e = err_of(sr) { return err(e); }
    let fr = scram_client_final(mech, s.pass, first, to_str(server_first), cnonce);
    guard let fin = fr else let e = err_of(fr) { return err(e); }
    let vr = sasl_round(c, to_bytes(fin.message), deadline);
    guard let server_final = vr else let e = err_of(vr) { return err(e); }
    let cr = scram_check_server(to_str(server_final), fin.server_signature);
    guard let _c = cr else let e = err_of(cr) { return err(e); }
    return ok(true);
}

fn sasl_round(c: Conn, auth: bytes, deadline: until) -> result[bytes, str] {
    let b = new_buf();
    enc_sasl_authenticate(b, auth);
    let rr = sync_call(c, API_SASL_AUTHENTICATE, b, deadline);
    guard let r = rr else let e = err_of(rr) { return err(e); }
    let dr = dec_sasl_authenticate(r);
    guard let a = dr else let e = err_of(dr) { return err(e); }
    if a.error_code != 0 {
        let msg = a.error_message;
        if msg == "" {
            msg = error_name(a.error_code);
        }
        return err("kafka: SASL " + c.cfg.sasl.mechanism + " authentication failed: " + msg);
    }
    return ok(a.auth);
}

// ---- SCRAM -------------------------------------------------------------------------

fn scram_name(user: str) -> str {
    return strings.replace(strings.replace(user, "=", "=3D"), ",", "=2C");
}

// The client-first-message-bare: "n=<user>,r=<nonce>".
pub fn scram_client_first(user: str, cnonce: str) -> str {
    return "n=" + scram_name(user) + ",r=" + cnonce;
}

pub gc struct ScramFinal {
    message: str,
    server_signature: bytes,
}

fn hmac_for(mech: str, key: bytes, data: bytes) -> bytes {
    if mech == "SCRAM-SHA-512" {
        return crypto.hmac_sha512(key, data);
    }
    return crypto.hmac_sha256(key, data);
}

fn hash_for(mech: str, data: bytes) -> bytes {
    if mech == "SCRAM-SHA-512" {
        return crypto.sha512(data);
    }
    return crypto.sha256(data);
}

// Hi() of RFC 5802: PBKDF2 with the mechanism's HMAC, one block.
fn scram_hi(mech: str, pass: bytes, salt: bytes, iterations: int) -> bytes {
    let u = hmac_for(mech, pass, salt + b"\x00\x00\x00\x01");
    let out = u[0..len(u)];
    for k in 1..iterations {
        u = hmac_for(mech, pass, u);
        for i in 0..len(out) {
            out[i] = out[i] ^ u[i];
        }
    }
    return out;
}

// The client-final-message for the server-first-message, and the
// signature the server must answer with.
pub fn scram_client_final(mech: str, pass: str, client_first_bare: str, server_first: str, cnonce: str)
                          -> result[ScramFinal, str] {
    let nonce = "";
    let salt_b64 = "";
    let iterations = 0;
    for part in strings.split(server_first, ",") {
        if strings.has_prefix(part, "r=") {
            nonce = strings.slice(part, 2, len(part));
        } else if strings.has_prefix(part, "s=") {
            salt_b64 = strings.slice(part, 2, len(part));
        } else if strings.has_prefix(part, "i=") {
            let ir = to_int(strings.slice(part, 2, len(part)));
            guard let i = ir else { return err("kafka: SCRAM: a bad iteration count from the server"); }
            iterations = i;
        } else if strings.has_prefix(part, "e=") {
            return err("kafka: SCRAM: the server refused: " + strings.slice(part, 2, len(part)));
        } else if strings.has_prefix(part, "m=") {
            return err("kafka: SCRAM: the server requires an extension this client does not know");
        }
    }
    if !strings.has_prefix(nonce, cnonce) || len(nonce) <= len(cnonce) {
        return err("kafka: SCRAM: the server's nonce does not extend the client's");
    }
    if iterations < SCRAM_MIN_ITERATIONS || iterations > SCRAM_MAX_ITERATIONS {
        return err("kafka: SCRAM: an iteration count of " + to_str(iterations) + " (allowed: " +
                   to_str(SCRAM_MIN_ITERATIONS) + " to " + to_str(SCRAM_MAX_ITERATIONS) + ")");
    }
    let sr = encoding.base64_decode(salt_b64);
    guard let salt = sr else { return err("kafka: SCRAM: the server's salt is not base64"); }
    let salted = scram_hi(mech, to_bytes(pass), salt, iterations);
    let client_key = hmac_for(mech, salted, to_bytes("Client Key"));
    let stored_key = hash_for(mech, client_key);
    let server_key = hmac_for(mech, salted, to_bytes("Server Key"));
    let without_proof = "c=biws,r=" + nonce;
    let auth_message = to_bytes(client_first_bare + "," + server_first + "," + without_proof);
    let client_sig = hmac_for(mech, stored_key, auth_message);
    let proof = client_key[0..len(client_key)];
    for i in 0..len(proof) {
        proof[i] = proof[i] ^ client_sig[i];
    }
    return ok(ScramFinal { message: without_proof + ",p=" + encoding.base64_encode(proof),
                           server_signature: hmac_for(mech, server_key, auth_message) });
}

pub fn scram_check_server(server_final: str, want: bytes) -> result[bool, str] {
    if strings.has_prefix(server_final, "e=") {
        return err("kafka: SCRAM: the server refused: " + strings.slice(server_final, 2, len(server_final)));
    }
    if !strings.has_prefix(server_final, "v=") {
        return err("kafka: SCRAM: a malformed server-final-message");
    }
    let v = strings.slice(server_final, 2, len(server_final));
    let comma = strings.find(v, ",");
    if comma >= 0 {
        v = strings.slice(v, 0, comma);
    }
    let gr = encoding.base64_decode(v);
    guard let got = gr else { return err("kafka: SCRAM: the server's signature is not base64"); }
    if !crypto.constant_time_eq(got, want) {
        return err("kafka: SCRAM: the server's signature does not match (it does not hold these credentials)");
    }
    return ok(true);
}
