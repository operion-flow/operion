GOFLAGS ?= -ldflags="-s -w"

./bin/operion:
	go build $(GOFLAGS) -o ./bin/operion ./cmd/operion


.PHONY: build build-linux clean test test-coverage fmt lint docs mod-check

build: ./bin/operion

clean:
	rm -rf ./bin

test:
	@echo "Running PostgreSQL persistence tests serially..."
	go test -p=1 ./pkg/persistence/postgresql
	@echo "Running all other tests in parallel..."
	go test $(shell go list ./... | grep -v "pkg/persistence/postgresql")

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


release:
	git tag ${tag} -m "Release ${tag}" -f
	git push origin ${tag} -f
	git push origin main
	docker buildx build -t dukex/operion:${tag} .
	docker tag dukex/operion:${tag} docker.io/dukex/operion:${tag}
	docker push docker.io/dukex/operion:${tag}
	docker tag dukex/operion:${tag} docker.io/dukex/operion:latest
	docker push docker.io/dukex/operion:latest