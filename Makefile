.PHONY: build clean test

build:
	go build -o memex-fs ./cmd/memex-fs

clean:
	rm -f memex-fs

test:
	go test ./...
