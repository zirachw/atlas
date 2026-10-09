FROM golang:1.27.2-alpine AS build
WORKDIR /app
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /client-api ./cmd/client-api

FROM alpine:3.24
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 atlas
COPY --from=build /client-api /usr/local/bin/client-api
USER atlas
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s CMD wget -q -O /dev/null http://127.0.0.1:8080/health || exit 1
ENTRYPOINT ["client-api"]
