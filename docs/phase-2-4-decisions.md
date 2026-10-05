# Phase 2–4 engineering decisions

| Level | Problem | Decision | Cost/tradeoff |
|---|---|---|---|
| Source | WebSockets disconnect and delivery is at least once | Resume only from versioned durable cursor; prefer overlap | Duplicates are expected and measured |
| Kafka | Source and processors fail independently | Idempotent producer, `acks=all`, DID key | Higher acknowledgement latency for durability |
| Checkpoint | Advancing early loses data | Advance only after raw/DLQ acknowledgement | Crash after ack can redeliver |
| DLQ | Bad records must not block or disappear | Preserve original payload and reason | Requires an explicit repair/replay workflow |
| Bronze | Object stores do not append transactionally | Deterministic compressed JSONL batches plus manifests | Possible harmless orphan before manifest |
| GCS | Retry could replace raw history | Create-only generation precondition | A conflicting key fails instead of overwriting |
| Bronze partitioning | Replay must be selective | Date/hour/collection | Rare collections may create smaller files |
| Spark schema | Source evolves additively | Explicit stable envelope; raw payload retained | New analytical fields require a versioned Silver change |
| Deduplication | Infinite streaming state is unsafe | 48h live watermark; unbounded batch dedup for replay | Old recovery uses Bronze, not live consumer |
| Mutations | Updates/deletes must be represented | Immutable mutation rows; deterministic current-state fold | Current state is materialized downstream |
| Silver storage | Architecture requires canonical Parquet | Checkpointed Parquet event history and versioned rebuilds | No direct transactional MERGE |
| Execution | Streaming and replay can diverge | Shared pure DataFrame transformations | I/O wrappers remain mode-specific |
| Logging | Per-event INFO logs are expensive | INFO per lifecycle/batch, DEBUG per event, WARNING for data issues, ERROR for unavailable dependencies | Debug detail must be enabled during incident analysis |

See ADRs 0002–0004 for complete reasoning.
