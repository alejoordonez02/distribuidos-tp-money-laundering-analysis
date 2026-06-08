# Money Laundering Analysis

Distributed pipeline that runs 5 use cases over transaction datasets.

## Requirements

- Docker and Docker Compose
- [uv](https://github.com/astral-sh/uv)
- Datasets placed in `datasets/`

## Configuration

The dataset to use is set in `scripts/cfg.py` (`TRANSACTIONS_PATH`, `ACCOUNTS_PATH`).
`NCLIENTS` controls how many clients are generated.

## Running

1. Set the dataset in `scripts/cfg.py`.
2. Generate client input and expected responses:

   ```
   make gen_input_output
   ```

3. Build and start the system (follows logs):

   ```
   make up
   ```

4. In another terminal, run the tests once the clients finish:

   ```
   make test
   ```

5. Tear everything down:

   ```
   make down
   ```

## Make targets

- `help` - list available targets.
- `gen_input_output` - generate per-client input files and expected responses from the configured dataset.
- `gen_compose` - generate `docker-compose.yaml` from `scripts/gen_compose/`.
- `up` - generate the compose file, build images, start all services detached, and follow logs.
- `stop_server` - stop every container except RabbitMQ.
- `down` - stop and remove all containers.
- `logs` - print the current logs.
- `test` - run the test suite (compares produced responses against expected responses).
- `report` - build the report under `doc/informe`.
- `demo` - generate demo files.
