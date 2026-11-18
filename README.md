# WORTH Server

Standalone authoritative WebSocket server for WORTH, the 3D arena-sport game.

## Deploy to Render

This repository is now the server's root. Create a Render **Web Service** connected to `XwowzersX-sub/WORTH-Server`, branch `main`, with:

| Setting | Value |
|---|---|
| Root Directory | *(leave blank)* |
| Runtime | Node |
| Build Command | `npm install` |
| Start Command | `npm start` |
| Health Check Path | `/health` |

You can also create a Render Blueprint from the root `render.yaml`.

## Environment variables

- `NODE_VERSION`: `22`
- `SUPABASE_URL`: supplied in `render.yaml`
- `SUPABASE_PUBLISHABLE_KEY`: supplied in `render.yaml`
- `SUPABASE_SERVICE_ROLE_KEY`: enter your Supabase secret/service-role key in the Render Dashboard only
- `CLIENT_ORIGIN`: `https://xwowzersx-sub.github.io`

Render supplies `PORT` automatically. Do not add it manually.

**Security:** Never commit the Supabase secret/service-role key to GitHub or expose it to the browser client.

## Endpoints

- `GET /`: service status and supported maps/modes
- `GET /health`: Render health check
- `wss://<your-service>.onrender.com/ws`: WebSocket matchmaking and match updates

## Gameplay services

- 1v1 matchmaking by arena and mode
- Server-authoritative player movement and central-zone scoring
- Five arena maps; Loadout and Addition modes
- First-to-five rounds and first-to-five match wins
- Addition-mode upgrade points on a round loss
- Shared focus slow motion: when either player holds right-click focus, the full match advances at 38% simulation speed
- Supabase-backed player progression when the server secret key is configured

## Local development

```bash
npm install
npm run check
npm run dev
```
