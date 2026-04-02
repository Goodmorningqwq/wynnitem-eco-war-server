# wynnitem-eco-war-server

Standalone Socket.io backend for `war.html` multiplayer rooms.

## Run locally

```bash
npm install
npm start
```

Default port is `3001`.

## Render settings

- Service type: **Web Service**
- Root Directory: *(empty)*
- Build Command: `npm install`
- Start Command: `node proxy.js`

## Environment variables

- `ALLOWED_ORIGINS` (recommended)
  - Comma-separated frontend origins allowed to connect.
  - Example:
    - `https://wynnitem-territory.vercel.app,https://wynnitem-territories.vercel.app`
- `ECO_WAR_SHARED_TOKEN` (optional but recommended)
  - Shared token for privileged room actions.
  - Frontend sends it via Socket handshake auth/header.
- `RATE_LIMIT_WINDOW_MS` (optional)
  - Rate-limit window in milliseconds. Default: `10000`.
- `RATE_LIMIT_MAX_SOCKET` (optional)
  - Max events per socket per window. Default: `24`.
- `RATE_LIMIT_MAX_IP` (optional)
  - Max events per IP per window. Default: `60`.
- `TICK_INTERVAL_MS` (optional)
  - Eco tick interval. Default: `6000`.

## Verification checklist

1. `GET /health` returns `ok: true`.
2. `allowedOrigins` shows expected domains.
3. From allowed frontend origin, create and join room works.
4. From disallowed origin, handshake is rejected.
5. Rapid spam attempts produce `Too many requests` and service remains stable.
