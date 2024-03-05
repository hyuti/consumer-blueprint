# Step 1: Modules caching
FROM golang:1.20-alpine as modules
COPY go.mod go.sum /modules/
WORKDIR /modules
RUN go mod download

# Step 2: Builder
FROM golang:1.20-alpine as builder

RUN apk add --no-cache ca-certificates dpkg gcc git musl-dev openssh

COPY --from=modules /go/pkg /go/pkg
COPY . /app
WORKDIR /app

# app cmd
RUN GOOS=linux GOARCH=amd64 \
    go build -tags musl -o /bin/app ./cmd/consumer/

# step3: copy from builder
FROM golang:1.20-alpine

WORKDIR /

COPY --from=builder /bin/app /app
COPY --from=builder /app/config/config.yaml /config/config.yaml

CMD ["/app"]
