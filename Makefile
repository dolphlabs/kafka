SLANGC ?= slangc

.PHONY: test
test:
	SLANGC="$(SLANGC)" ./tests/run.sh
