# Core Components

The Human Validation Service acts as the human-in-the-loop validation manager for the Document Processor system. If the AI service produces an extraction output that does not match the JSON schema or business rules, the Orchestrator Service suspends the job and registers a manual validation task with this service.

## Features

- **Decoupled Workflow Processing**: Prevents thread-starvation or queue timeout issues by pausing jobs asynchronously.
- **Hierarchical Deterministic Keying**: Uses `$tenant_id.$app_id.$job_id.$step_id` for $O(1)$ idempotent lookups in JetStream KV.
- **Pessimistic Concurrency Leasing**: Provides `ClaimTask` and `ReleaseTask` RPCs to prevent multiple operators from validating the same suspended task simultaneously. Includes an automated background `LeaseReaper` to auto-recover stale leases.
- **gRPC RPC API**:
  - `CreateValidationTask`: Registers a job needing human intervention ($O(1)$ idempotent).
  - `GetPendingTasks`: Returns/streams tasks matching a status (e.g., `PENDING`, `PROCESSING`) and optional tenant identity filter.
  - `ClaimTask`: Claims a task with a configurable lease duration (`lease_seconds`).
  - `ReleaseTask`: Releases an in-progress claimed task back to `PENDING` status.
  - `SubmitCorrection`: Resolves the task and triggers job resumption via the Orchestrator's `ResumeJob` endpoint.
  - `DeleteTask`: Removes a validation task from the KV bucket.
- **Complete Traceability**: Emits structured telemetry events to `trace.validation.{tenant_id}.{app_id}` (`validation.task_suspended`, `validation.task_claimed`, `validation.task_released`, `validation.task_resumed`, `validation.task_deleted`).
