# Money Laundering Analysis — Distributed Pipeline

A distributed data processing system that ingests financial transaction and account datasets and identifies money laundering patterns across multiple use cases. Built as a scalable pipeline using RabbitMQ as the message broker and Docker Compose for orchestration.

## Architecture

The system is modelled as a DAG of stateless workers connected by message queues. Each worker type implements a generic controller + pluggable strategy pattern, selected via a `STRATEGY` environment variable at startup.

```
Client → Gateway → Filter → [per-UC sub-pipelines] → Join → Client
```

### Worker types

| Worker | Role |
|--------|------|
| **Gateway** | TCP server; accepts client connections, fans transactions and accounts to the queues |
| **Filter** | Drops records that do not match a predicate (e.g. currency = USD, amount < threshold) |
| **GroupBy** | Emits partial aggregations keyed by a field (e.g. `From Bank`) |
| **Aggregate** | Merges partial results from GroupBy workers into a single reduced value |
| **Merge** | Joins two independent streams (transactions + accounts) by a shared key |
| **Join** | Collects all results for a client and builds the final response |
| **Converter** | Transforms transaction amounts (e.g. currency conversion to USD) |

Multiple clients run concurrently; each message carries a `client_id` so pipelines never mix data across clients.

## Use cases

| UC | Name | Status |
|----|------|--------|
| UC1 | Direct filter — USD transactions below $50 | **Implemented** |
| UC2 | Max per bank — highest USD transaction per bank, enriched with bank name | **Implemented** |
| UC3 | Period comparison — transactions in period B below 1% of period-A average per payment format | Not implemented |
| UC4 | Scatter-gather pattern — accounts that fan out to 5–9 intermediaries then reconverge | Not implemented |
| UC5 | Currency count — Wire/ACH transactions in period A whose USD-converted amount is below $1 | Implemented on `feat/uc5-pipeline` |

### UC2 pipeline (implemented on this branch)

```
transactions ──► Filter (USD) ──► GroupBy (max amount / bank) ──► Aggregate ──┐
                                                                               ▼
accounts ────────────────────────► GroupBy (bank names) ──────► Aggregate ──► Merge ──► Join ──► response
```

## Prerequisites

- Docker + Docker Compose
- [`uv`](https://docs.astral.sh/uv/) Python package manager
- The [IBM LI-Small dataset](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml) extracted into `datasets/`:
  - `datasets/LI-Small_Trans.csv`
  - `datasets/LI-Small_accounts.csv`

## Running

```bash
# Start the full pipeline (generates test datasets, builds images, runs until clients finish)
make up

# Tear down all containers and networks
make down
```

`make up` generates sampled transaction datasets and expected responses under `datasets/` and `test/expected/`, builds all Docker images, and streams logs until the pipeline finishes.

## Testing

```bash
make test
```

Runs the integration test suite with `pytest`. Tests generate input datasets and compare the pipeline output in `responses/` against the expected results computed locally.

## Next steps

- **Batch processing** — window-based processing for pipelines that must compare two time periods (UC3, UC4)
- **Horizontal scaling** — replicate stateless workers (Filter, GroupBy, Converter) behind a load-balanced queue; route by `client_id` to keep stateful workers (Aggregate, Join) deterministic
- **Fault tolerance** — worker checkpointing, message acknowledgement on commit, and automatic restart on crash

## Project structure

```
src/
  gateway/        TCP server and queue fan-out
  filter/         Predicate-based message filtering
  group_by/       Partial aggregation by key
  aggregate/      Merge partial results
  merge/          Join two streams by shared key
  join/           Collect results and emit client response
  converter/      Transform transaction fields
  client/         Test client (sends data, receives responses)
  common/         Shared message types, serialization, middleware
test/             Integration tests and expected-output generator
doc/              Assignment specification and project report
```
