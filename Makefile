.PHONY: fmt fmt-check vet test check

GO ?= go
GOFMT ?= gofmt
GOFILES := $(shell find . -type f -name '*.go' -not -path './vendor/*')

fmt:
	$(GOFMT) -w $(GOFILES)

fmt-check:
	@unformatted="$$( $(GOFMT) -l $(GOFILES) )"; \
	if [ -n "$$unformatted" ]; then \
		echo "Unformatted Go files:"; \
		echo "$$unformatted"; \
		echo "Run 'make fmt' to format them."; \
		exit 1; \
	fi

vet:
	$(GO) vet ./...

test:
	$(GO) test ./...

check: fmt-check vet test
