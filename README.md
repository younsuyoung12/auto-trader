## Deployment Topology (Production)
- **Execution (Auto-Trader Worker): AWS**
  - Runs the 24/7 trading worker (WebSocket ingest → strategy → execution).
  - Holds exchange API keys/secrets (never stored on Render).
  - Sends trade/events to the DB over TLS.

- **Database + Dashboard: Render**
  - Managed Postgres on Render stores trades/events/snapshots.
  - Render hosts the dashboard/API server that queries Postgres and visualizes performance.
  - No trading execution happens on Render (read-only / analytics only).

### Security Boundary
- Exchange credentials live **only on AWS**.
- Render services operate **without** exchange trading permissions.
- Any missing/invalid data triggers fail-fast behavior (STRICT · NO-FALLBACK).