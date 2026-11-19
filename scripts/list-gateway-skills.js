#!/usr/bin/env node
/**
 * Lists installed skills through the gateway (skills.status).
 *
 * Usage: node scripts/list-gateway-skills.js [--ws <url>]
 * Requires HERMES3D_GATEWAY_TOKEN in the environment or in the repo .env.
 */
const WebSocket = require("ws");
const { ORIGIN, requireGatewayToken, resolveWsUrl } = require("./lib/gateway-config");

const WS_URL = resolveWsUrl();
const TOKEN = requireGatewayToken();

const ws = new WebSocket(WS_URL, { headers: { Origin: ORIGIN } });
let nextId = 0;
const pending = new Map();

const request = (method, params = {}) =>
  new Promise((resolve, reject) => {
    const id = `sk-${++nextId}`;
    pending.set(id, { resolve, reject });
    ws.send(JSON.stringify({ type: "req", id, method, params }));
  });

ws.on("message", (data) => {
  let frame;
  try {
    frame = JSON.parse(String(data));
  } catch {
    return;
  }
  if (frame?.type !== "res" || !frame.id) return;
  const entry = pending.get(frame.id);
  if (!entry) return;
  pending.delete(frame.id);
  if (frame.ok) entry.resolve(frame.payload);
  else entry.reject(new Error(`${frame.error?.code}: ${frame.error?.message}`));
});

ws.on("open", async () => {
  try {
    await request("connect", {
      minProtocol: 3,
      maxProtocol: 4,
      client: { id: "skills-list", version: "1.0.0", platform: process.platform, mode: "webchat" },
      role: "operator",
      scopes: [],
      caps: [],
      auth: { token: TOKEN },
    });
    const status = await request("skills.status");
    console.log(JSON.stringify(status, null, 2));
    ws.close();
  } catch (err) {
    console.error(`skills.status failed: ${err.message}`);
    process.exitCode = 1;
    ws.close();
  }
});

ws.on("error", (err) => {
  console.error(`ws error: ${err.message}`);
  process.exitCode = 1;
});
