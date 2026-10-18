# syntax=docker/dockerfile:1.6
FROM golang:1.25-alpine AS builder
WORKDIR /src/inflora-palantir
COPY inflora-shared /src/inflora-shared
COPY inflora-palantir/go.mod inflora-palantir/go.sum ./
RUN go mod download
COPY inflora-palantir .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/inflora-palantir ./cmd/server

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && addgroup -S inflora && adduser -S -G inflora inflora
COPY --from=builder /out/inflora-palantir /usr/local/bin/inflora-palantir
USER inflora:inflora
ENTRYPOINT ["/usr/local/bin/inflora-palantir"]
EXPOSE 7001
