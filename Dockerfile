FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o postroom .

FROM alpine:latest
COPY --from=builder /app/postroom /postroom
RUN mkdir -p /data
VOLUME ["/data"]
EXPOSE 8080 1025
ENTRYPOINT ["/postroom"]
