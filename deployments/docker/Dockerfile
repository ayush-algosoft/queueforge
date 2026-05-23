# Multi-stage build shared by every QueueForge binary. The build arg SERVICE
# selects which cmd/<service> to compile and which binary lands in the final
# image. This keeps the dockerfile count to one while still producing
# minimal per-service images.

ARG GO_VERSION=1.23
FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src

RUN apk add --no-cache git ca-certificates

# Cache module downloads in a dedicated layer.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG SERVICE
RUN test -n "$SERVICE" || (echo "SERVICE build-arg is required" && exit 1)

# CGO_ENABLED=0 so we can copy into a scratch-like image.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
        -o /out/app ./cmd/${SERVICE}

# Final stage: distroless-style minimal image.
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata curl && \
    addgroup -S app && adduser -S -G app app
USER app
COPY --from=build /out/app /usr/local/bin/app
ENTRYPOINT ["/usr/local/bin/app"]
