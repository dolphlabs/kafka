# kafka

An Apache Kafka client for slang: producers, consumers and consumer
groups over the stdlib `net` package, with deadlines, verified TLS, SASL
and bounded memory. The plan is [KAFKA_PRD.md](KAFKA_PRD.md).

**Status:** milestone 1 of 9 (the wire layer: primitive types, CRC-32C,
record batches v2 and message sets v0/v1).

## Tests

    make test SLANGC=/path/to/slangc

runs the unit tests (`slangc test src/kafka`) and `tests/wire`: record
batches and message sets written by Go's franz-go (`tests/wire/gen`),
decoded and printed, and where they come from a producer encoded again
byte for byte; and varints from Go's `encoding/binary` both ways. The
expected output is written by the generator from its own specs, not by
decoding, so the decoder is checked against an independent writer.
