SLANGC ?= slangc

.PHONY: test
test:
	SLANGC="$(SLANGC)" ./tests/run.sh

# The slang encoders' output, decompressed by the Go libraries (needs go).
.PHONY: interop
interop:
	KAFKA_TESTDATA="$(CURDIR)/tests/codec" KAFKA_CODEC_OUT="$(CURDIR)/tests/codec/slang-out.bin" \
		"$(SLANGC)" tests/codec/main.sl --run >/dev/null
	cd tests/codec/gen && go run . check ../slang-out.bin
