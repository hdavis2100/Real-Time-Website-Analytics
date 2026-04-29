# Poker Analytics Dashboard

The dashboard is currently a live-only Streamlit app focused on active
simulation monitoring.

It is separate from the lighter operator shell served by the Express app at
`http://localhost:3000/app`, which handles saved agents, simulations, profile
requests, and structured explorer queries through the app API.

## Run

Use the Docker Compose service:

```bash
docker compose up -d dashboard
```

Then open `http://localhost:8501`.

For a broader repo and runtime overview, see
[`docs/CODEBASE_GUIDE.md`](../docs/CODEBASE_GUIDE.md).

## Data Sources

The dashboard reads Redis materializations produced by `jobs/stream_kafka.py`:

- `live:runs:active`
- `live:run:{run_id}:meta`
- `live:run:{run_id}:leaderboard:profit`
- `live:run:{run_id}:leaderboard:bb_per_100`
- `live:run:{run_id}:leaderboard:high_hand`
- `live:global:leaderboard:profit`
- `live:global:leaderboard:high_hand`
- `live:global:leaderboard:hero_context`
- `live:run:{run_id}:agents`

The page auto-refreshes while the session is open and focuses on:

- active run status
- selected live run leaderboards
- selected run agent statistics
- global live leaderboards

There are no local CSV/JSON fallback datasets in the active runtime.
