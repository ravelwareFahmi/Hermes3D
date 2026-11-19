#!/usr/bin/env node
/**
 * Verifies the seeded oee-graphene office roster: agents.list, one paused
 * playbook job per agent (cron.list), and that each workspace AGENTS.md is
 * readable through agents.files.get.
 *
 * Usage: node scripts/verify-oee-graphene-agents.js [--ws <url>]
 * Requires HERMES3D_GATEWAY_TOKEN in the environment or in the repo .env.
 */
const WebSocket = require("ws");
const { requireGatewayToken, resolveWsUrl } = require("./lib/gateway-config");

const WS_URL = resolveWsUrl();
const GATEWAY_TOKEN = requireGatewayToken();

class GatewaySession {
  constructor(url) {
    this.url = url;
    this.nextId = 0;
    this.pending = new Map();
  }

  connect() {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(this.url, { headers: { Origin: new URL(this.url).origin } });
      this.ws = ws;
      const timer = setTimeout(() => reject(new Error("handshake timed out")), 20_000);
      ws.on("unexpected-response", (_req, res) =>
        reject(new Error(`unexpected HTTP ${res.statusCode}`)),
      );
      ws.on("error", reject);
      ws.on("message", (data) => this.onMessage(String(data)));
      ws.on("open", () => {
        this.request("connect", {
          minProtocol: 3,
          maxProtocol: 4,
          client: { id: "verify-oee", version: "1.0.0", platform: process.platform, mode: "webchat" },
          role: "operator",
          scopes: [],
          caps: [],
          auth: { token: GATEWAY_TOKEN },
        })
          .then((hello) => {
            clearTimeout(timer);
            resolve(hello);
          })
          .catch((err) => {
            clearTimeout(timer);
            reject(err);
          });
      });
    });
  }

  onMessage(raw) {
    let frame;
    try {
      frame = JSON.parse(raw);
    } catch {
      return;
    }
    if (frame?.type !== "res" || !frame.id) return;
    const pending = this.pending.get(frame.id);
    if (!pending) return;
    this.pending.delete(frame.id);
    clearTimeout(pending.timer);
    if (frame.ok) pending.resolve(frame.payload);
    else pending.reject(new Error(`${frame.error?.code}: ${frame.error?.message}`));
  }

  request(method, params = {}, timeoutMs = 60_000) {
    const id = `verify-${++this.nextId}`;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`${method} timed out`)), timeoutMs);
      this.pending.set(id, { resolve, reject, timer });
      this.ws.send(JSON.stringify({ type: "req", id, method, params }));
    });
  }

  close() {
    try {
      this.ws?.close();
    } catch {
      /* ignore */
    }
  }
}

const main = async () => {
  const session = new GatewaySession(WS_URL);
  await session.connect();
  const problems = [];
  try {
    const roster = await session.request("agents.list", {});
    const agents = roster.agents || [];
    console.log(`agents.list: ${agents.length} agent(s), defaultId=${roster.defaultId}`);
    for (const agent of agents) {
      console.log(
        `  ${String(agent.id).padEnd(28)} ${agent.name}${agent.workspace ? `  @ ${agent.workspace}` : ""}`,
      );
    }

    const jobsResult = await session.request("cron.list", { includeDisabled: true });
    const jobs = jobsResult.jobs || [];
    console.log(`\ncron.list: ${jobs.length} job(s)`);
    for (const job of jobs) {
      console.log(
        `  ${String(job.id).slice(0, 8).padEnd(9)} agent=${String(job.agentId).padEnd(28)} ` +
          `enabled=${String(job.enabled).padEnd(5)} ${job.name}`,
      );
    }

    const virtualAgents = agents.filter((a) => a.id !== roster.defaultId);
    const pausedForVirtual = jobs.filter(
      (j) => virtualAgents.some((a) => a.id === j.agentId) && j.enabled === false,
    );
    console.log(
      `\nvirtual agents: ${virtualAgents.length}, paused playbook jobs on them: ${pausedForVirtual.length}`,
    );
    if (virtualAgents.length !== 13) problems.push(`expected 13 virtual agents, got ${virtualAgents.length}`);
    if (pausedForVirtual.length !== 13) {
      problems.push(`expected 13 paused jobs, got ${pausedForVirtual.length}`);
    }

    console.log("\nAGENTS.md spot-check (first 3 virtual agents):");
    for (const agent of virtualAgents.slice(0, 3)) {
      const result = await session.request("agents.files.get", {
        agentId: agent.id,
        name: "AGENTS.md",
      });
      const file = result.file || {};
      const size = typeof file.content === "string" ? file.content.length : 0;
      const firstLine = typeof file.content === "string" ? file.content.split("\n")[0] : "";
      console.log(`  ${agent.id.padEnd(28)} missing=${Boolean(file.missing)} chars=${size} "${firstLine}"`);
      if (file.missing || size < 500) problems.push(`${agent.id}: AGENTS.md missing or too small`);
    }
  } finally {
    session.close();
  }

  if (problems.length > 0) {
    console.error(`\nPROBLEMS:\n  - ${problems.join("\n  - ")}`);
    process.exitCode = 1;
  } else {
    console.log("\nAll checks passed.");
  }
};

main().catch((err) => {
  console.error(`verify failed: ${err.message}`);
  process.exitCode = 1;
});
