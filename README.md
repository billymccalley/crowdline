# Crowdline

Crowdline is a daily social-comparison puzzle. Players place four items on a line, get scored against the crowd, and come back for a new line each day.

The app is served by a small dependency-free Python backend and stores game data in SQLite.

## Run it

```bash
python app.py
```

Then open http://localhost:3000.

If port 3000 is busy in PowerShell, run:

```powershell
$env:PORT = "3001"
python app.py
```

The backend persists local game data in `data/crowdline.sqlite3`.

Accounts, session tokens, player stats, daily locks, leaderboard entries, and crowd votes are all stored in the same SQLite database.

## Deploy on Render

This repo includes `render.yaml` for a Render Blueprint. It creates:

- a Python web service named `crowdline`
- a 1 GB persistent disk mounted at `/var/data`
- a SQLite database at `/var/data/crowdline.sqlite3`

In Render, create a new Blueprint from the repo and let it use `render.yaml`. The app health check is `/api/health`.

Render's persistent disk keeps the SQLite database across deploys. For a much larger public launch, the next database step would be Render Postgres.

The Render disk is the app's external storage layer:

- `DATA_DIR=/var/data`
- `DATABASE_PATH=/var/data/crowdline.sqlite3`

As long as the service keeps that disk attached, player logins and game data survive restarts and deploys.
