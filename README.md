# inflora-palantir

Internal gRPC payment-provider broker.

Canonical port(s): 7001. Phase 0 contains a compileable placeholder only; implementation begins in Phase 5 of the backend build plan.

```sh
make tidy
make lint
make test
make build
make run
```

Source of truth: `almanac/planning/WIRE_GUIDE.md` and `almanac/planning/schemas/`.
