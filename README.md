# kafka

An Apache Kafka client for slang: producers, consumers and consumer
groups over the stdlib `net` package, with deadlines, verified TLS, SASL
and bounded memory. The plan is [KAFKA_PRD.md](KAFKA_PRD.md).

**Status:** milestones 1-2 of 9: the wire layer (primitive types,
CRC-32C, record batches v2 and message sets v0/v1) and the four codecs
(gzip through the stdlib; snappy, lz4 and zstd written in slang).

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

    make interop SLANGC=/path/to/slangc

decompresses the slang encoders' output with the same Go libraries (needs
go).
