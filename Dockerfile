FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder

ARG TARGETOS=linux
ARG TARGETARCH=amd64

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /control-plane ./cmd/controlplane

FROM alpine:3.20

WORKDIR /app

COPY --from=builder /control-plane /usr/local/bin/control-plane
COPY config/local.yml /app/config/local.yml

EXPOSE 8089

ENTRYPOINT ["control-plane"]
CMD ["-config", "/app/config/local.yml"]
