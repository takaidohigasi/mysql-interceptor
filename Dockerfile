FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /mysql-interceptor ./cmd/mysql-interceptor/

FROM alpine:3.20

RUN apk add --no-cache ca-certificates

COPY --from=builder /mysql-interceptor /usr/local/bin/mysql-interceptor

ENTRYPOINT ["mysql-interceptor"]
CMD ["serve", "--config", "/etc/mysql-interceptor/config.yaml"]
