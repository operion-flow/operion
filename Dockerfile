FROM golang:1.27 AS builder

WORKDIR /app

COPY go.* ./
RUN go mod download

COPY cmd ./cmd
COPY pkg ./pkg

RUN go build -ldflags="-s -w" -v -o /app/bin/operion /app/cmd/operion

FROM debian:bookworm-slim
RUN set -x && apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/bin/operion /bin/operion

ENTRYPOINT ["/bin/operion"]
CMD ["serve"]
