# Valkey migrations

Technical documentation for the standalone **migrations** binary (`cmd/migrations`) that mutates shared Valkey/Redis state used by the **projection (processing)** service.

## Operational rule: no concurrent processing

**You must not run migrations while projection workloads are actively reading and writing the same Valkey data.**

Treat this as a **deployment and runbook constraint**, not something you rely on the code to “fix” for you:

- The implementation includes **safeguards** (distributed lock, existence checks, and a best-effort check that no client using the processing `CLIENT SETNAME` is connected). Those reduce risk but do **not** replace correct orchestration.
- Checks are subject to **time-of-check/time-of-use (TOCTOU)** gaps: state can change between a check and the next operation.
- The “no processing client connected” check is based on **`CLIENT LIST`** semantics and client naming; it is **defense in depth**, not a formal proof of isolation.
- In Kubernetes, **pod restarts, overlapping Jobs, or scaled replicas** can violate assumptions if you do not sequence rollout and the migration Job explicitly.

**Recommended practice:** run the migration Job **before** scaling projection back up or before new projection pods connect to Valkey, and ensure **no projection replica** targets the store during the migration window. Align this with your OpenShift/Kubernetes rollout strategy (e.g. suspend or scale down consumers, run the Job, then roll forward).

## Why idempotency is mandatory

Migration Jobs in Kubernetes are **not** single-shot guarantees:

- A Job can **fail mid-run** (OOM, node drain, network blip) and be **retried** or re-created.
- The process can exit after **partial** progress.
- The distributed lock **TTL** and **extend** logic can expire or fail; operators may need to **re-run** after investigation.

Therefore **every migration must be idempotent**: running it twice (or resuming after a crash) must converge to the same correct end state as a single clean run, without corrupting data or relying on “exactly once” execution.

When adding a migration:

- Prefer **atomic Redis commands** for a single key where possible (e.g. `SET` with expiry in one command rather than separate value + `EXPIRE`).
- Keep a **durable source of truth** until cutover is complete (e.g. do not delete legacy keys until new keys are fully written and validated, if your design requires it).
- Document in code/comments what happens if the Job is **re-run** after partial completion.

The `Migration` interface in `cmd/migrations/main.go` is documented with this expectation; new steps should uphold it.

## How the migrations binary behaves

1. Connects to Valkey using `VALKEY_ADDRESS` / `VALKEY_PASSWORD` and sets the Redis client name to **`migrations`** (`internal/repository/redis.MigrationsClientName`).
2. **Pre-check:** if key `migrations:lock` **exists**, aborts (another holder or stale key until TTL expiry).
3. **Acquires** a **redsync** mutex on `migrations:lock` with a **10-minute** expiry, with a background **auto-extend** loop tied to a derived `context.Context` passed into each migration’s `Run`.
4. **Pre-check:** lists Redis clients and aborts (after releasing the lock) if a connection appears to use the processing client name **`assisted-events-stream`** (`ProcessingClientName`).
5. Runs migrations **in order** from the slice in `main` (currently `DeleteNoTTL`, then `SplitClusters`).
6. **Releases** the lock on success; on migration failure, releases the lock and exits non-zero.

Lock implementation: `internal/migrations/lock`.

## How projection (processing) behaves

On startup, when building the snapshot repository from env (`NewSnapshotRepositoryFromEnv`), the projection connects with the **processing** client name and calls **`EXISTS` on `migrations:lock`**. If the key exists, repository creation **fails** and the consumer does not run against Valkey in that configuration.

This complements, but does **not** replace, the operational rule above: projection must still be **scaled or scheduled** so it does not compete with migrations in production workflows.

## Configuration

| Variable | Used by | Purpose |
|----------|---------|---------|
| `VALKEY_ADDRESS` | migrations + projection | Valkey/Redis address |
| `VALKEY_PASSWORD` | migrations + projection | Authentication |

## OpenShift / Kubernetes

A sample Job template lives at `openshift/jobs/run-migrations.yaml`. Parameterize image, resources, and secrets to match your environment. Ensure cluster procedures enforce **ordering** relative to projection deployments as described above.

## Adding a migration

1. Implement `Run(ctx context.Context) error` with **cancellation** respected where feasible (the context is cancelled if lock extend fails or on shutdown paths).
2. Register the migration in the `migrations` slice in `cmd/migrations/main.go` **in the correct order** relative to existing steps.
3. Confirm **idempotency** and document any operational notes for SREs.

## Related code paths

- Entrypoint: `cmd/migrations/main.go`
- Lock + client checks: `internal/migrations/lock/lock.go`
- Migration implementations: `internal/migrations/processing/`
- Valkey repository used by migrations: `internal/migrations/domain/repo/valkey/`
- Projection guard: `internal/repository/redis/utils.go` (`NewSnapshotRepositoryFromEnv`)
