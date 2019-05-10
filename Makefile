.PHONY: check help dependencies dev-dependencies .pre-check-go generate

SRCS = $(patsubst ./%,%,$(shell find . -name "*.go" -not -path "*vendor*"))

help: ## Display this help screen
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

check: dev-dependencies | generate ## Run unit tests
	go test ./...
	go test -race ./...

benchmark: dev-dependencies | generate ## Run benchmarks
	go test -bench . ./...

dependencies: | generate ##‌ Download dependencies
	go get -v ./...

dev-dependencies: dependencies ##‌ Download development dependencies
	go get -v github.com/stretchr/testify/suite
	go get -v github.com/stretchr/testify/assert

ubroker: $(SRCS) | dependencies generate ##‌ Compile us
	go build -o ubroker ./cmd/ubroker

generate: pkg/ubroker/ubroker.pb.go

pkg/ubroker/ubroker.pb.go: api/ubroker.proto
	protoc --go_out=plugins=grpc:$(GOPATH)/src $<

.pre-check-go:
ifeq (,$(shell which go))
	$(error golang is required, please install golang and re-run command, see more at https://grpc.io/docs/quickstart/go.html)
endif
ifeq (,$(shell which protoc))
	$(error protoc is required, please install protoc and re-run command, see more at https://grpc.io/docs/quickstart/go.html)
endif
ifeq (NONE,$(GOPATH))
	$(error GOPATH environment variable must be set, refer to golang installation manual)
endif
	go get -v github.com/golang/protobuf/protoc-gen-go
	go get -v github.com/vektra/mockery/.../
