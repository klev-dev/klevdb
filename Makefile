default: test

.PHONY: test test-verbose lint
test:
	go test -cover ./...

test-verbose:
	go test -cover -v ./...

lint:
	golangci-lint run

.PHONY:
build:
	go build -v ./...

.PHONY: bench bench-publish bench-consume bench-get bench-multi
bench:
	go test -bench=. -benchmem -run XXX

bench-publish:
	go test -bench=BenchmarkSingle/Publish -benchmem -run XXX

bench-consume:
	go test -bench=BenchmarkSingle/Consume -benchmem -run XXX

bench-get:
	go test -bench=BenchmarkSingle/Get -benchmem -run XXX

bench-multi:
	go test -bench=BenchmarkMulti -benchmem -run XXX

.PHONY: update-libs
update-libs:
	go get -u ./...
	go mod tidy
