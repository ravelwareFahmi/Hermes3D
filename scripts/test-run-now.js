#!/usr/bin/env node
/**
 * Fires one paused playbook job through cron.run to prove the Run now path
 * (ensureSession in the agent workspace -> prompt.submit -> streamed turn).
 *
 * Usage: node scripts/test-run-now.js [jobNameSubstring] [--ws <url>]
 * Requires HERMES3D_GATEWAY_TOKEN in the environment or in the repo .env.
 */
const WebSocket = require("ws");
const { ORIGIN, requireGatewayToken, resolveWsUrl } = require("./lib/gateway-config");

const WS_URL = resolveWsUrl();
const TOKEN = requireGatewayToken();
const NEEDLE = process.argv[2] && !process.argv[2].startsWith("--") ? process.argv[2] : "TDD Guide";

const ws = new WebSocket(WS_URL, { headers: { Origin: ORIGIN } });
let nextId = 0;
const pending = new Map();

const request = (method, params = {}, timeoutMs = 60_000) =>
  new Promise((resolve, reject) => {
    const id = `run-${++nextId}`;
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
      client: { id: "test-run-now", version: "1.0.0", platform: process.platform, mode: "webchat" },
      role: "operator",
      scopes: [],
      caps: [],
      auth: { token: TOKEN },
    });
    const { jobs } = await request("cron.list", { includeDisabled: true });
    const job = (jobs || []).find((j) => String(j.name).includes(NEEDLE));
    if (!job) {
      throw new Error(`no job matching "${NEEDLE}"`);
    }
    console.log(`firing "${job.name}" (${job.id}) for agent ${job.agentId}...`);
    const result = await request("cron.run", { id: job.id }, 90_000);
    console.log(`cron.run -> ${JSON.stringify(result)}`);
    ws.close();
    if (result?.ok !== true) process.exitCode = 1;
  } catch (err) {
    console.error(`run-now test failed: ${err.message}`);
    process.exitCode = 1;
    ws.close();
  }
});

ws.on("error", (err) => {
  console.error(`ws error: ${err.message}`);
  process.exitCode = 1;
});
