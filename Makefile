.PHONY: build clean test run

BINARY_NAME=mysql-interceptor
BUILD_DIR=bin
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "none")
DATE    ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS  = -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)

build:
	go build -ldflags '$(LDFLAGS)' -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/mysql-interceptor/

clean:
	rm -rf $(BUILD_DIR)

test:
	go test ./...

run: build
	$(BUILD_DIR)/$(BINARY_NAME) serve --config config.yaml

bench:
	go run ./cmd/mysql-interceptor/ bench --config config.yaml
