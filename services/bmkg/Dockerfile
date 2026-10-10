FROM golang:1.27.2-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /bmkg ./cmd/bmkg

FROM alpine:3.24
RUN adduser -D -u 10001 atlas
COPY --from=build /bmkg /usr/local/bin/bmkg
USER atlas
EXPOSE 8081
HEALTHCHECK --interval=10s --timeout=3s CMD wget -q -O /dev/null http://127.0.0.1:8081/health || exit 1
ENTRYPOINT ["bmkg"]
