# DistKV Chaos Test

This demo kills one node during a write burst and verifies that `QUORUM` writes still succeed with two of three replicas alive.

## Prerequisites

- Docker and Docker Compose
- `grpcurl`

## Run

```bash
cd deploy
docker compose up --build -d
cd ..
./test/chaos/chaos-quorum.sh
```

The script writes through node 1, stops node 3 mid-burst, continues writing with `QUORUM`, and then reads a key back from node 1.

Bring the cluster down:

```bash
cd deploy
docker compose down -v
```
