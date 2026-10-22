FROM --platform=$BUILDPLATFORM golang:1.27 AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /app

COPY go.* ./
RUN go mod download

COPY cmd ./cmd
COPY pkg ./pkg

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /app/bin/operion /app/cmd/operion

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /app/bin/operion /bin/operion

ENTRYPOINT ["/bin/operion"]
CMD ["serve"]
