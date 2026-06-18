# syntax=docker/dockerfile:1

FROM golang:1.26

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /trace-service

EXPOSE 8081

CMD ["/trace-service"]