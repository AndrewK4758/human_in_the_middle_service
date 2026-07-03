# Human Validation Service

This service acts as the human-in-the-loop validation manager for the Document Processor system. If/when the AI service produces an extraction output that does not match the JSON schema or business rules, the Orchestrator Service suspends the job and registers a manual validation task with this service.

Once a human logs into a portal or triggers a script to submit a correction, this service updates the state and invokes the Orchestrator's `ResumeJob` endpoint, which increments the step and re-publishes the job to NATS JetStream to seamlessly resume the pipeline.

## Features

- **Decoupled Workflow Processing**: Prevents thread-starvation or queue timeout issues by pausing jobs asynchronously.
- **gRPC RPC API**:
  - `CreateValidationTask`: Registers a job needing human intervention.
  - `GetPendingTasks`: Returns tasks matching a status (e.g., `PENDING`).
  - `SubmitCorrection`: Resolves the task and triggers job resumption.
- **Complete Traceability**: Emits telemetry event tracking and matches system standards.

## Running Locally

To run the Human Validation Service locally:

```bash
cd human_validation_service
go run cmd/server/main.go
```

The service runs on port `:50054` by default.

## Docker Deployment

To build and run inside Docker:

```bash
docker build -t human-validation-service -f Dockerfile ..
```
