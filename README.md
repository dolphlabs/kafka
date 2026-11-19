# kafka

An Apache Kafka client for slang: producers, consumers and consumer
groups over the stdlib `net` package, with deadlines, verified TLS, SASL
and bounded memory. The plan is [KAFKA_PRD.md](KAFKA_PRD.md).

**Status:** milestones 1-3 of 9: the wire layer (primitive types,
CRC-32C, record batches v2 and message sets v0/v1), the four codecs
(gzip through the stdlib; snappy, lz4 and zstd written in slang), and the
request and response codecs for the 28 APIs the client uses, and the
connection and cluster layers: TLS, SASL (PLAIN, SCRAM-SHA-256/512,
OAUTHBEARER), API version checks, pipelined requests with deadlines,
bootstrap and metadata, and the admin calls.

Needs slang built from `dev` at 7a5bfe1 or later (SCRAM-SHA-512 uses
`crypto.hmac_sha512`, which the 0.2.5 release lacks).

```slang
import "kafka";

let cfg = kafka.config(["kafka-1.internal:9093", "kafka-2.internal:9093"]);
cfg.tls = true;
cfg.sasl = kafka.scram_sha512("billing", secret);
let clr = kafka.connect(cfg, kafka.after_ms(10000));
guard let cl = clr else let e = err_of(clr) { panic(e); }
kafka.create_topic(cl, "orders", 12, 3, deadline);         // result[bool, str]: false if it existed
let end = kafka.list_offset(cl, "orders", 0, kafka.OFFSET_LATEST, deadline);
kafka.close(cl);
```

## Tests

    make test SLANGC=/path/to/slangc

runs the unit tests (`slangc test src/kafka`) and `tests/wire`: record
batches and message sets written by Go's franz-go (`tests/wire/gen`),
decoded and printed, and where they come from a producer encoded again
byte for byte; and varints from Go's `encoding/binary` both ways. The
expected output is written by the generator from its own specs, not by
decoding, so the decoder is checked against an independent writer.

`tests/codec` decompresses 104 vectors that Go's klauspost/compress
(zstd at four levels, with and without checksums, small windows),
pierrec/lz4 (64 KB to 4 MB blocks, block and content checksums, content
size, HC), golang/snappy (raw and xerial-framed) and gzip wrote from eight
inputs; round trips each input through the slang encoders; truncates and
corrupts every vector (624 mutations, which must fail cleanly); and checks
that every decoder stops at its size limit.

`tests/protocol` encodes 34 requests and must match franz-go's `kmsg`
byte for byte, decodes 35 responses `kmsg` encoded and must print the
values `tests/protocol/gen` set, and refuses each response cut short.

`tests/fake` runs the client against scripted brokers that misbehave: a
broker older than Kafka 2.4, one too old for Produce v9, oversized and
negative frame sizes, a wrong correlation id, silence, a connection cut
mid-response, metadata never answered, and malformed metadata.
`tests/cluster` (needs go) runs against in-process clusters from
franz-go's `kfake` (`tests/kfake`): metadata, topic creation and deletion,
partitions, offsets, configs and groups; every SASL mechanism with right
and wrong credentials; TLS against a private CA, and refused without it;
and seed fallback.

    make interop SLANGC=/path/to/slangc

decompresses the slang encoders' output with the same Go libraries (needs
go).
