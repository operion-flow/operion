FROM golang:1.27 AS builder

WORKDIR /app

COPY go.* ./
COPY examples/benefits ./examples/benefits

RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /app/bin/benefits ./examples/benefits

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /app/bin/benefits /benefits

ENTRYPOINT ["/benefits"]
