FROM golang:1.27.1-alpine AS build
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /auth ./cmd/auth

FROM alpine:3.24
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 atlas
COPY --from=build /auth /usr/local/bin/auth
USER atlas
EXPOSE 8084
HEALTHCHECK --interval=10s --timeout=3s CMD wget -q -O /dev/null http://127.0.0.1:8084/health || exit 1
ENTRYPOINT ["auth"]
