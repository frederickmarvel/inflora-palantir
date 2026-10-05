# AI Repository Guide: inflora-palantir

## Purpose

`inflora-palantir` is Inflora's internal gRPC payment-provider broker. It isolates provider-specific APIs and credentials from the rest of the platform and exposes normalized payment operations to trusted services. The canonical gRPC port is `7001`.

Palantir does not own public donor APIs, the internal ledger, streamer administration, or schema migrations.

## Important paths

- `cmd/server/`: process composition and startup.
- `internal/grpc/`: internal RPC handlers, including health, top-up, and settlement flows.
- `internal/provider/`: provider adapters and provider-facing behavior.
- `internal/repo/`: provider-operation persistence and idempotency state.
- `internal/events/`: canonical event publication.
- `internal/config/`: environment-driven configuration.

## Contract and correctness rules

- Use `/Users/frederickmarvel/Inflora/almanac/planning/WIRE_GUIDE.md` and `almanac/planning/schemas/` as the cross-repository source of truth.
- Keep provider idempotency keys stable and persist enough state to safely retry uncertain calls.
- Never log provider secrets, API keys, raw sensitive payment data, or authentication headers.
- Money is integer IDR. Preserve exact amounts and normalized provider statuses.
- Publish only canonical, versioned events from `inflora-shared`, and do not publish a success before the corresponding durable state is committed.
- Database schema changes are implemented in `inflora-saruman` migrations, not here.

## Commands

```sh
make tidy
make lint
make test
make build
make run
```

Run `go test ./...`, `go vet ./...`, and `git diff --check` before finishing. Clearly identify any provider sandbox or infrastructure tests that could not run.
