.PHONY: build clean test run

BINARY_NAME=mysql-interceptor
BUILD_DIR=bin

build:
	go build -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/mysql-interceptor/

clean:
	rm -rf $(BUILD_DIR)

test:
	go test ./...

run: build
	$(BUILD_DIR)/$(BINARY_NAME) serve --config config.yaml

bench:
	go run ./cmd/mysql-interceptor/ bench --config config.yaml
