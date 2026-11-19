fn test_scram_sha256_rfc7677_example() {
    let first = scram_client_first("user", "rOprNGfwEbeRWgbNEkqO");
    assert(first == "n=user,r=rOprNGfwEbeRWgbNEkqO");
    let server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
    let fr = scram_client_final("SCRAM-SHA-256", "pencil", first, server_first, "rOprNGfwEbeRWgbNEkqO");
    guard let f = fr else let e = err_of(fr) { panic(e); }
    assert(f.message == "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
           f.message);
    let cr = scram_check_server("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=", f.server_signature);
    guard let _c = cr else let e = err_of(cr) { panic(e); }
    assert(err_of(scram_check_server("v=AAAA", f.server_signature)) ==
           "kafka: SCRAM: the server's signature does not match (it does not hold these credentials)");
    assert(err_of(scram_check_server("e=invalid-proof", f.server_signature)) ==
           "kafka: SCRAM: the server refused: invalid-proof");
}

fn test_scram_refuses_bad_servers() {
    let first = scram_client_first("u", "abc");
    assert(err_of(scram_client_final("SCRAM-SHA-512", "p", first, "r=xyz123,s=AAAA,i=4096", "abc")) ==
           "kafka: SCRAM: the server's nonce does not extend the client's");
    assert(err_of(scram_client_final("SCRAM-SHA-512", "p", first, "r=abcdef,s=AAAA,i=10", "abc")) ==
           "kafka: SCRAM: an iteration count of 10 (allowed: 4096 to 1000000)");
    assert(err_of(scram_client_final("SCRAM-SHA-512", "p", first, "e=unknown-user", "abc")) ==
           "kafka: SCRAM: the server refused: unknown-user");
    assert(scram_client_first("a=b,c", "n") == "n=a=3Db=2Cc,r=n");
}

fn test_split_host_port() {
    let a = split_host_port("broker-1.internal:9093") ?? Target { host: "", port: 0 };
    assert(a.host == "broker-1.internal" && a.port == 9093);
    let b = split_host_port("[::1]:9092") ?? Target { host: "", port: 0 };
    assert(b.host == "::1" && b.port == 9092);
    assert(err_of(split_host_port("broker")) == "kafka: broker address broker has no port");
    assert(err_of(split_host_port("b:70000")) == "kafka: broker address b:70000 is not host:port");
}
