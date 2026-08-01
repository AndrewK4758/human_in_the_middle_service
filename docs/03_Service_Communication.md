# Service Communication

Once a human logs into a portal or triggers a script to submit a correction, this service updates the state and publishes a `PerformActionResponse` back to the Orchestrator via NATS JetStream on the reply subject, allowing the pipeline to seamlessly resume.

## 📜 Data Contracts & No Preemptive Serialization

This service adheres to the strict **No Preemptive Serialization** mandate. 
- All JSON-based structural state must be parsed natively using `google.protobuf.Struct` (or `structpb.Struct` in Go, `Dictionary<string, object>` in C#).
- Preemptive stringification (e.g. converting nested objects to strings before the final persistence layer) is forbidden to prevent schema drift, whitespace corruption, and nested parsing bugs.
- Boundary interfaces rely on typed object mappings rather than dynamic regex or fuzzy key lookups.
