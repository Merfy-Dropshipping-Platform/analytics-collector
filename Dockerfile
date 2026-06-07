FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /collector ./cmd/collector

FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata postgresql-client
ENV TZ=Europe/Moscow

COPY --from=builder /collector /collector
COPY static/ /static/
COPY migrations/ /migrations/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 3120

HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
  CMD wget -qO- http://localhost:3120/health || exit 1

ENTRYPOINT ["/entrypoint.sh"]
