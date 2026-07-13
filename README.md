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

The Blueprint also registers `playcrowdline.com` as the custom domain. Keep the Render `onrender.com` subdomain enabled until DNS and TLS are verified.

## Domain setup

In Render, open the `crowdline` service and check Settings -> Custom Domains. The Blueprint should add `playcrowdline.com`. If it does not appear, add it manually.

At your DNS provider:

- Remove any `AAAA` records for `playcrowdline.com`.
- For the root domain, add an `ANAME` or `ALIAS` record pointing to your Render subdomain if your provider supports it.
- If your DNS provider does not support `ANAME` or `ALIAS`, add an `A` record for `@` pointing to `216.24.57.1`.
- If your DNS provider is Cloudflare, use a flattened `CNAME` for the root instead of an `A` record.
- Render automatically adds `www.playcrowdline.com` when the root domain is configured and redirects it to the root domain.

Then return to Render's Custom Domains section and click Verify. Render will issue and renew TLS automatically after verification.

The Render disk is the app's external storage layer:

- `DATA_DIR=/var/data`
- `DATABASE_PATH=/var/data/crowdline.sqlite3`

As long as the service keeps that disk attached, player logins and game data survive restarts and deploys.
