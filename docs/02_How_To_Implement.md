# How to Implement

## Running Locally

To run the Human Validation Service locally:

```bash
cd human_validation_service
go run cmd/server/main.go
```

> **Note**: To cleanly build and run all ecosystem microservices simultaneously with correct relative paths and multiplexed logs, execute `./scripts/run_all.zsh` from the project root instead.

The service runs on port `:11114` by default.

## Docker Deployment

To build and run inside Docker:

```bash
docker build -t human-validation-service -f Dockerfile ..
```
