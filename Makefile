default: test

.PHONY: test
test:
	go test -cover -v ./...

.PHONY: bench
bench:
	go test -bench=. -benchmem
