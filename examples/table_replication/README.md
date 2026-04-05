# Table Replication via CDC

Demonstrates CDC-based table replication: events written to a source Ducklake are consumed via change data capture, filtered by `team_id`, and routed to two downstream Ducklakes.

## Architecture

```
Producer → Ducklake A (postgres-source + minio/source)
                ↓ (CDC: table_insertions)
Router reads CDC, routes by team_id:
    team_id=123 → Ducklake team-123 (postgres-123 + minio/team-123)
    team_id=456 → Ducklake team-456 (postgres-456 + minio/team-456)
    others → dropped
                ↓
Reader-123 polls Ducklake team-123, prints new records
Reader-456 polls Ducklake team-456, prints new records
```

## Services

| Service | Role |
|---------|------|
| `postgres-source` | Metadata store for source Ducklake |
| `postgres-123` | Metadata store for team-123 Ducklake |
| `postgres-456` | Metadata store for team-456 Ducklake |
| `minio` | Object storage with 3 buckets (source, team-123, team-456) |
| `minio-init` | Creates the S3 buckets on startup |
| `producer` | Generates analytics events, writes to source Ducklake |
| `router` | Reads CDC from source, routes to downstream Ducklakes |
| `reader-123` | Polls team-123 Ducklake, prints new events |
| `reader-456` | Polls team-456 Ducklake, prints new events |

## Running

```bash
docker compose up --build
```

## Expected Output

The producer generates events at ~10/sec. Roughly 5% go to team-123 and 3% go to team-456:

```
[producer 12:00:05] flushed 50 events (total: 50, rate: 10.0/s, team-123: 3, team-456: 1)
[router   12:00:06] snapshot 0→1: routed 3→team-123, 1→team-456
[reader-123 12:00:08] +3 events (total: 3) — click=1, pageview=2
[reader-456 12:00:08] +1 events (total: 1) — signup=1
```

## Stopping

```bash
docker compose down -v
```
