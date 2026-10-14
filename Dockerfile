FROM golang:1.27-alpine AS builder
WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/mini-etcd .

FROM alpine:3.21
RUN adduser -D -u 10001 app
WORKDIR /app
COPY --from=builder /out/mini-etcd .


USER 10001

EXPOSE 8080
ENTRYPOINT ["./mini-etcd"]