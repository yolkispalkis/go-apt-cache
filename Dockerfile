FROM golang:latest AS builder

WORKDIR /app

# Copy go.mod and go.sum
COPY go.mod ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o apt-cache ./cmd/go-apt-cache

# Create a minimal runtime image
FROM alpine:latest

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/apt-cache .

# Create default config file
COPY --from=builder /app/config.json /app/config.json.default

# Create directory for cache
RUN mkdir -p /app/cache

# Expose the default port
EXPOSE 8080

# Run the application
ENTRYPOINT ["/app/apt-cache"]
CMD ["--config", "/app/config.json"] 