FROM docker.io/golang:1.23 AS builder
ARG GOARCH="amd64"
RUN mkdir /app
WORKDIR /app
COPY . /app
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${GOARCH} go build -ldflags="-s -w"

FROM docker.io/alpine:latest
RUN mkdir /app && adduser -h /app -D restapi
WORKDIR /app
COPY --chown=restapi --from=builder /app/restapi .
EXPOSE 4000
CMD ["/app/restapi"]