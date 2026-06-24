# syntax=docker/dockerfile:1

FROM golang:1.26 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /trace-service -ldflags="-s -w"

FROM scratch

COPY --from=builder /trace-service /trace-service

EXPOSE 8081

ENTRYPOINT ["/trace-service"]
