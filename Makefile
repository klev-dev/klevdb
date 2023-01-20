default: test

.PHONY: test
test:
	go test -cover -v ./...

.PHONY: bench
bench:
	go test -bench=. -benchmem

.PHONY: update-libs
update-libs:
	go get -u github.com/klev-dev/kleverr@main
	go mod tidy
