.PHONY: check help dependencies dev-dependencies

SRCS = $(patsubst ./%,%,$(shell find . -name "*.go" -not -path "*vendor*"))

help: ## Display this help screen
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

check: dependencies ## Run unit tests
	go test ./...
	go test -race ./...

dependencies: ##‌ Download dependencies
	go get -v ./...

dev-dependencies: dependencies ##‌ Download development dependencies
	go get -v github.com/stretchr/testify/suite

ubroker: $(SRCS) ##‌ Compile us
	go build -o ubroker ./cmd/ubroker
