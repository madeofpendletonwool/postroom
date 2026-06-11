FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o postroom .

FROM scratch
COPY --from=builder /app/postroom /postroom
EXPOSE 8080 1025
ENTRYPOINT ["/postroom"]
