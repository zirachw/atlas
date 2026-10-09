FROM golang:1.27.2-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /pvmbg ./cmd/pvmbg

FROM alpine:3.24
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 atlas
COPY --from=build /pvmbg /usr/local/bin/pvmbg
USER atlas
EXPOSE 8082
HEALTHCHECK --interval=10s --timeout=3s CMD wget -q -O /dev/null http://127.0.0.1:8082/health || exit 1
ENTRYPOINT ["pvmbg"]
