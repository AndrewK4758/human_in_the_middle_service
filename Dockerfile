FROM golang:alpine AS builder

# Install build dependencies
RUN apk add --no-cache git

WORKDIR /app

# In order to support the local `replace shared_protos` go.mod directive,
# we set the Docker build context to the root directory
COPY shared_protos/ ./shared_protos/
COPY human_validation_service/ ./human_validation_service/

WORKDIR /app/human_validation_service

# Add local replace directive
RUN go mod edit -replace github.com/AndrewK4758/shared_protos=../shared_protos

# Download dependencies
RUN go mod download

# Build the executable
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o validation_bin cmd/server/main.go

# --- Final minimal stage ---
FROM alpine:latest  
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary from the builder stage
COPY --from=builder /app/human_validation_service/validation_bin .

EXPOSE 50054

CMD ["./validation_bin"]
