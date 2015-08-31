
BIGWIG := $(GOPATH)/bin/bigwig

build: $(BIGWIG)

$(BIGWIG): bigwig.go
	@(go build)
	@(go install)
