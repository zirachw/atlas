FROM golang:1.27.1-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /aggregator ./cmd/aggregator

FROM alpine:3.24
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 atlas
COPY --from=build /aggregator /usr/local/bin/aggregator
USER atlas
EXPOSE 8083
HEALTHCHECK --interval=10s --timeout=3s CMD wget -q -O /dev/null http://127.0.0.1:8083/health || exit 1
ENTRYPOINT ["aggregator"]
