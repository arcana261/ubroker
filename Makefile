.PHONY: check help dependencies dev-dependencies .pre-check-go generate

SRCS = $(patsubst ./%,%,$(shell find . -name "*.go" -not -path "*vendor*"))

help: ## Display this help screen
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

check: dev-dependencies | generate ## Run unit tests
	go test ./... -count=1
	go test -race ./... -count=1

benchmark: dev-dependencies | generate ## Run benchmarks
	go test -bench . ./...

dependencies: | generate ##‌ Download dependencies
	go get -v ./...

dev-dependencies: dependencies | generate ##‌ Download development dependencies
	go get -v github.com/stretchr/testify/suite
	go get -v github.com/stretchr/testify/assert
	go get -v github.com/phayes/freeport

ubroker: $(SRCS) pkg/ubroker/ubroker.pb.go | dependencies generate ##‌ Compile us
	go build -o ubroker ./cmd/ubroker

generate: pkg/ubroker/ubroker.pb.go

pkg/ubroker/ubroker.pb.go: api/ubroker.proto | .pre-check-go
	$(PROTOC) $(PROTOC_OPTIONS) --go_out=plugins=grpc:$(GOPATH)/src api/ubroker.proto

.pre-check-go:
	go get -v github.com/golang/protobuf/protoc-gen-go
	go get -v github.com/vektra/mockery/.../

PROTOC ?= protoc
PROTOC_OPTIONS ?=
