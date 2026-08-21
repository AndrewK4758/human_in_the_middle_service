# Service Communication

Once a human logs into a portal or triggers a correction submission:
1. The UI/client claims the task via `ClaimTask(taskId, operatorId, leaseSeconds)`.
2. Upon user submission, `SubmitCorrection(taskId, corrected)` sends a `ResumeJobRequest` to the Orchestrator Service via gRPC.
3. The Orchestrator Service resumes execution, publishing the resumption event to JetStream.
4. On success, the validation service transitions the task status to `RESOLVED`. If the orchestrator fails, the task rolls back to `PENDING`.

## 📜 Data Contracts & Unified State Resumption

This service adheres to the strict **Unified State & No Preemptive Serialization** mandate. 
- All JSON-based structural state must be parsed natively using `google.protobuf.Struct` (or `structpb.Struct` in Go, `Dictionary<string, object>` / `JsonObject` in C#).
- Preemptive stringification (e.g. converting nested objects to strings before the final persistence layer) is forbidden to prevent schema drift, whitespace corruption, and nested parsing bugs.
- Corrections merge in-place directly into `global_state.functional.extracted_data` (under `ExtractedDataKey`), eliminating intermediate wrapper keys.
- Boundary interfaces rely on strongly-typed schemas rather than intermediate DTOs or shadow properties.
