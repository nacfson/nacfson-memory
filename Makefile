.PHONY: build test test-integration lint migrate reindex eval loadtest

build:
	CGO_ENABLED=0 go build -o bin/mem1 ./cmd/mem1

test:
	go test -race ./...

test-integration:
	go test -p 1 -tags=integration -race ./...

lint:
	go vet ./... && golangci-lint run ./...

migrate:
	./bin/mem1 migrate

reindex:
	./bin/mem1 reindex --batch-size=32

eval:
	./bin/mem1 eval --suite=longmemeval_s,memoryagentbench_conflict --max-tokens=2000

loadtest:
	go test -v -tags="integration load" -race -timeout 5m -run TestConcurrencyAndChaos ./tests/load/... && go test -v -tags="integration load" -timeout 3m -run TestRecallLatencySLO ./tests/load/...
