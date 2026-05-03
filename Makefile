.PHONY: build test lint proto clean

build:
	go build ./...

test:
	go test ./... -race -count=1

lint:
	golangci-lint run ./...

proto:
	protoc --go_out=gen --go-grpc_out=gen proto/*.proto

clean:
	rm -rf gen/*.go
