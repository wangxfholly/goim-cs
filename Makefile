.PHONY: build vet test demo cover clean all

all: build vet test

build:
	go build ./...

vet:
	go vet ./...

test:
	go test ./...

cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

demo:
	go run ./cmd/demo

clean:
	go clean ./...
	rm -f coverage.out coverage.html
