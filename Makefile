SLANGC ?= slangc

.PHONY: test
test:
	SLANGC="$(SLANGC)" ./tests/run.sh

# The slang encoders' output, decompressed by the Go libraries (needs go).
.PHONY: interop
interop:
	cd tests/codec && KAFKA_TESTDATA="$(CURDIR)/tests/codec" KAFKA_CODEC_OUT="$(CURDIR)/tests/codec/slang-out.bin" \
		"$(SLANGC)" main.sl --run >/dev/null
	cd tests/codec/gen && go run . check ../slang-out.bin

# The client against Apache Kafka 4.1, 3.9 and Redpanda in Docker, with a
# franz-go peer (needs docker and go). LIVE_FLAVORS picks a subset.
.PHONY: live
live:
	SLANGC="$(SLANGC)" sh ./tests/live/run.sh
