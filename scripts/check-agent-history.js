#!/usr/bin/env node
/**
 * Reads an agent's main-session chat history to confirm a Run now turn
 * actually produced a model response.
 *
 * Usage: node scripts/check-agent-history.js [agentId] [maxMessages] [--ws <url>]
 * Requires HERMES3D_GATEWAY_TOKEN in the environment or in the repo .env.
 */
const WebSocket = require("ws");
const { ORIGIN, requireGatewayToken, resolveWsUrl } = require("./lib/gateway-config");

const WS_URL = resolveWsUrl();
const TOKEN = requireGatewayToken();
const positional = process.argv.slice(2).filter((arg) => !arg.startsWith("--"));
const AGENT_ID = positional[0] || "tdd-guide";
const MAX = Number(positional[1] || 6);

const ws = new WebSocket(WS_URL, { headers: { Origin: ORIGIN } });
let nextId = 0;
const pending = new Map();

const request = (method, params = {}, timeoutMs = 60_000) =>
  new Promise((resolve, reject) => {
    const id = `hist-${++nextId}`;
    const timer = setTimeout(() => {
      pending.delete(id);
      reject(new Error(`${method} timed out`));
    }, timeoutMs);
    pending.set(id, { resolve, reject, timer });
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
  clearTimeout(entry.timer);
  if (frame.ok) entry.resolve(frame.payload);
  else entry.reject(new Error(`${frame.error?.code}: ${frame.error?.message}`));
});

ws.on("open", async () => {
  try {
    await request("connect", {
      minProtocol: 3,
      maxProtocol: 4,
      client: { id: "check-history", version: "1.0.0", platform: process.platform, mode: "webchat" },
      role: "operator",
      scopes: [],
      caps: [],
      auth: { token: TOKEN },
    });
    const history = await request("chat.history", {
      sessionKey: `agent:${AGENT_ID}:main`,
      limit: MAX,
    });
    const messages = history?.messages || [];
    console.log(`${messages.length} message(s) for ${AGENT_ID}:`);
    for (const message of messages.slice(-MAX)) {
      const role = message.role ?? message.kind ?? "?";
      const text = String(message.text ?? message.content ?? "").replace(/\s+/g, " ");
      console.log(`  [${role}] ${text.slice(0, 300)}`);
    }
    ws.close();
  } catch (err) {
    console.error(`history check failed: ${err.message}`);
    process.exitCode = 1;
    ws.close();
  }
});

ws.on("error", (err) => {
  console.error(`ws error: ${err.message}`);
  process.exitCode = 1;
});
