.PHONY: build test race bench clean run

BINARY := bin/ringlimit

build:
	@mkdir -p bin
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BINARY) ./cmd/ringlimit

test:
	go test -v ./pkg/limiter

race:
	go test -v -race ./pkg/limiter

bench:
	go test -run=^$$ -bench=. -benchmem ./pkg/limiter

run: build
	./$(BINARY) -rate 10000000 -burst 1000000 -workers 8

clean:
	rm -rf bin/
