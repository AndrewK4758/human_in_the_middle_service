# Core Components

The Human Validation Service acts as the human-in-the-loop validation manager for the Document Processor system. If the AI service produces an extraction output that does not match the JSON schema or business rules, the Orchestrator Service suspends the job and registers a manual validation task with this service.

## Features

- **Decoupled Workflow Processing**: Prevents thread-starvation or queue timeout issues by pausing jobs asynchronously.
- **gRPC RPC API**:
  - `CreateValidationTask`: Registers a job needing human intervention.
  - `GetPendingTasks`: Returns tasks matching a status (e.g., `PENDING`).
  - `SubmitCorrection`: Resolves the task and triggers job resumption via the Orchestrator's `ResumeJob` endpoint.
- **Complete Traceability**: Emits telemetry event tracking and matches system standards.
