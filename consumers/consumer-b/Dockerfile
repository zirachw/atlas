FROM golang:1.27.2-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /consumer-b ./cmd/consumer

FROM alpine:3.24
RUN adduser -D -u 10001 atlas && mkdir -p /data && chown atlas:atlas /data
COPY --from=build /consumer-b /usr/local/bin/consumer-b
USER atlas
EXPOSE 8092
HEALTHCHECK --interval=10s --timeout=3s CMD wget -q -O /dev/null http://127.0.0.1:8092/health || exit 1
ENTRYPOINT ["consumer-b"]
