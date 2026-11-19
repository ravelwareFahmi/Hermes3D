#!/usr/bin/env node
/**
 * Creates one virtual office agent (idempotent by name) with an AGENTS.md role
 * brief and a paused "Role Job" so it shows in Active Jobs without firing.
 *
 * Usage:
 *   node scripts/create-agent.js --name "Full Stack Developer" \
 *     --role "..." --brief path/to/brief.md [--job "Job name"]
 *
 * Omit --brief to skip the AGENTS.md write.
 * Requires HERMES3D_GATEWAY_TOKEN in the environment or in the repo .env.
 */
const fs = require("node:fs");
const WebSocket = require("ws");
const { ORIGIN, requireGatewayToken, resolveWsUrl } = require("./lib/gateway-config");

const TOKEN = requireGatewayToken();
const WS_URL = resolveWsUrl();

const argv = process.argv.slice(2);
const arg = (flag, fallback = "") => {
  const i = argv.indexOf(flag);
  return i === -1 ? fallback : (argv[i + 1] || "").trim();
};
const NAME = arg("--name");
const ROLE = arg("--role");
const BRIEF_PATH = arg("--brief");
const JOB_NAME = arg("--job", `${NAME} Role Job`);

if (!NAME) {
  console.error("usage: node scripts/create-agent.js --name <name> [--role <role>] [--brief <file>]");
  process.exit(1);
}

const ws = new WebSocket(WS_URL, { headers: { Origin: ORIGIN } });
let nextId = 0;
const pending = new Map();

const request = (method, params = {}, timeoutMs = 45_000) =>
  new Promise((resolve, reject) => {
    const id = `ca-${++nextId}`;
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

ws.on("error", (err) => {
  console.error(`connect failed: ${err.message}`);
  process.exitCode = 1;
});

ws.on("open", async () => {
  try {
    await request("connect", {
      minProtocol: 3,
      maxProtocol: 4,
      client: { id: "create-agent", version: "1.0.0", platform: process.platform, mode: "webchat" },
      role: "operator",
      scopes: [],
      caps: [],
      auth: { token: TOKEN },
    });

    const roster = await request("agents.list", {});
    const existing = (roster.agents || []).find((a) => a.name === NAME);
    let agentId;
    if (existing) {
      agentId = existing.id;
      console.log(`= agent "${NAME}" already exists (${agentId})`);
    } else {
      const created = await request("agents.create", { name: NAME, role: ROLE });
      agentId = created.agentId;
      console.log(`+ agent "${NAME}" -> ${agentId} @ ${created.workspace}`);
    }

    if (BRIEF_PATH) {
      const content = fs.readFileSync(BRIEF_PATH, "utf8");
      await request("agents.files.set", { agentId, name: "AGENTS.md", content });
      console.log(`  AGENTS.md written (${content.length} chars)`);
    }

    const jobs = await request("cron.list", { includeDisabled: true });
    if ((jobs.jobs || []).some((j) => j.name === JOB_NAME)) {
      console.log(`= job "${JOB_NAME}" already exists`);
    } else {
      const job = await request("cron.add", {
        name: JOB_NAME,
        agentId,
        enabled: false,
        schedule: { kind: "cron", expr: "0 9 * * *" },
        sessionTarget: "isolated",
        wakeMode: "now",
        payload: {
          kind: "agentTurn",
          message:
            `Jalankan peran "${NAME}" sesuai brief peranmu (AGENTS.md di workspace-mu). ` +
            "Kerjakan tugas yang diminta pengguna; bila belum ada tugas spesifik, " +
            "laporkan kesiapanmu: ringkas peran, keahlian utama, dan langkah berikutnya.",
        },
      });
      console.log(`+ job "${JOB_NAME}" -> ${job.id} (enabled=${job.enabled})`);
    }
  } catch (err) {
    console.error(`create failed: ${err.message}`);
    process.exitCode = 1;
  } finally {
    ws.close();
  }
});
