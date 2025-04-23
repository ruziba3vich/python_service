FROM golang:1.24.2 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /python-executor ./cmd/main.go

FROM alpine:3.18

RUN apk --no-cache add ca-certificates

COPY --from=builder /python-executor /python-executor

EXPOSE 7771

CMD ["/python-executor"]
