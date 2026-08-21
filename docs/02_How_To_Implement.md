# How to Implement

## Running Locally

To run the Human Validation Service locally:

```bash
cd human_validation_service
go run cmd/server/main.go
```

> **Note**: To cleanly build and run all ecosystem microservices simultaneously with correct relative paths and multiplexed logs, execute `./scripts/run_all.zsh` from the project root instead.

The service runs on port `:11114` by default.

### Environment Configuration

| Variable | Default | Description |
|---|---|---|
| `PORT` | `11114` | gRPC service port |
| `NATS_URL` | `localhost:4222` | NATS cluster connection address |
| `ORCHESTRATOR_ADDRESS` | `localhost:11112` | gRPC address of orchestrator service |
| `HUMAN_VALIDATION_KV_TTL` | `168h` | KeyValue bucket expiration TTL |
| `HUMAN_VALIDATION_LEASE_TTL_SECONDS` | `300` | Default pessimistic lease lock TTL in seconds |
| `HUMAN_VALIDATION_REAPER_INTERVAL_SECONDS` | `30` | Interval for lease reaper background scan |

## Docker Deployment

To build and run inside Docker:

```bash
docker build -t human-validation-service -f Dockerfile ..
```
