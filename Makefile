GOFLAGS ?= -ldflags="-s -w"

./bin/operion-worker:
	go build $(GOFLAGS) -o ./bin/operion-worker ./cmd/operion-worker


./bin/operion-api:
	go build $(GOFLAGS) -o ./bin/operion-api ./cmd/operion-api

./bin/operion-activator:
	go build $(GOFLAGS) -o ./bin/operion-activator ./cmd/operion-activator

./bin/operion-source-manager:
	go build $(GOFLAGS) -o ./bin/operion-source-manager ./cmd/operion-source-manager


.PHONY: build build-linux clean test test-integration test-all-with-integration test-coverage fmt lint docs mod-check

build: ./bin/operion-worker ./bin/operion-api ./bin/operion-activator ./bin/operion-source-manager

clean:
	rm -rf ./bin

test:
	@echo "Running PostgreSQL persistence tests serially..."
	go test -p=1 ./pkg/persistence/postgresql
	@echo "Running all other tests in parallel..."
	go test $(shell go list ./... | grep -v "pkg/persistence/postgresql")

test-integration:
	@echo "Running integration tests (requires Docker)..."
	@echo "Testing Web API integration..."
	go test -tags=integration -v ./pkg/web/
	@echo "Testing Kafka provider integration..."
	go test -tags=integration -v ./pkg/providers/kafka/persistence/
	go test -tags=integration -v ./pkg/providers/kafka/
	@echo "Testing Scheduler provider integration..."
	go test -tags=integration -v ./pkg/providers/scheduler/persistence/
	@echo "Integration tests completed"

test-all-with-integration: test test-integration

test-all: test

test-coverage:
	@echo "Running PostgreSQL persistence tests serially with coverage..."
	go test -p=1 -coverprofile=coverage-postgres.out -covermode=atomic ./pkg/persistence/postgresql
	@echo "Running all other tests in parallel with coverage..."
	go test -coverprofile=coverage-other.out -covermode=atomic $$(go list ./... | grep -v "pkg/persistence/postgresql")
	@echo "Combining coverage reports..."
	echo "mode: atomic" > coverage.out
	grep -h -v "mode: atomic" coverage-postgres.out coverage-other.out >> coverage.out
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

fmt:
	go fmt ./...

lint:
	golangci-lint run --fix --new-from-rev=origin/main

docs:
	godoc -http=:6060

mod-check:
	go mod verify
	go mod tidy


examples-workder:
	cd examples/ && docker compose up worker-kafka -d --build

examples-all: examples-stop examples-workder
	cd examples/ && docker compose up akhq -d
	open http://localhost:8080

examples-stop:
	cd examples/ && docker compose down

release:
	git tag ${tag} -m "Release ${tag}" -f
	git push origin ${tag} -f
	git push origin main
	docker buildx build -t dukex/operion:${tag} .
	docker tag dukex/operion:${tag} docker.io/dukex/operion:${tag}
	docker push docker.io/dukex/operion:${tag}
	docker tag dukex/operion:${tag} docker.io/dukex/operion:latest
	docker push docker.io/dukex/operion:latest