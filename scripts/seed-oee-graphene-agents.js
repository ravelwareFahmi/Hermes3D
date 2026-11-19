#!/usr/bin/env node
/**
 * Seeds the Hermes3D office with one agent per workflow file from
 * oee-graphene's `.agents` folder, plus one paused playbook job per agent.
 *
 * Each agent gets an AGENTS.md role brief in its workspace that combines the
 * shared QC rules (`.agents/AGENTS.md`) with that workflow's definition, and
 * the bridge runs the agent's sessions inside that workspace so hermes
 * injects the brief as context.
 *
 * Jobs are created paused ("manual"): they show up in the office's Active
 * Jobs but only fire when the operator clicks Run now.
 *
 * Idempotent: agents and jobs that already exist (matched by name) are
 * skipped, so the script can be re-run safely.
 *
 * Usage:
 *   OEE_AGENTS_DIR=/path/to/oee-graphene/.agents \
 *     node scripts/seed-oee-graphene-agents.js [--ws <url>]
 *
 * Requires HERMES3D_GATEWAY_TOKEN in the environment or in the repo .env.
 * SOURCE_ROOT has no baked-in default: point OEE_AGENTS_DIR at the `.agents`
 * folder of the source repository (it must contain AGENTS.md and workflows/).
 */
const fs = require("node:fs");
const path = require("node:path");
const WebSocket = require("ws");
const { requireGatewayToken, resolveWsUrl } = require("./lib/gateway-config");

const SOURCE_ROOT = (process.env.OEE_AGENTS_DIR || "").trim();
const WORKFLOWS_DIR = SOURCE_ROOT ? path.join(SOURCE_ROOT, "workflows") : "";
const SHARED_RULES_FILE = SOURCE_ROOT ? path.join(SOURCE_ROOT, "AGENTS.md") : "";
const GATEWAY_TOKEN = requireGatewayToken();

const JOB_SCHEDULE = "0 9 * * *"; // inert: every job is created paused
const ROLE_MAX_CHARS = 110;

const WS_URL = resolveWsUrl();

const titleize = (slug) =>
  slug
    .split(/[-_]/)
    .map((word) => {
      const lower = word.toLowerCase();
      if (lower === "e2e") return "E2E";
      if (lower === "tdd") return "TDD";
      return word.charAt(0).toUpperCase() + word.slice(1);
    })
    .join(" ");

/** Split YAML frontmatter (or the plain Description/Content layout) off a workflow. */
const parseWorkflowFile = (raw) => {
  const text = raw.replace(/\r\n/g, "\n");
  if (text.startsWith("---\n")) {
    const end = text.indexOf("\n---", 4);
    if (end !== -1) {
      const frontmatter = text.slice(4, end);
      const body = text.slice(end + 4).replace(/^\n/, "");
      const description = /^description:\s*(.+)$/m.exec(frontmatter)?.[1]?.trim();
      const name = /^name:\s*(.+)$/m.exec(frontmatter)?.[1]?.trim();
      return { meta: { name, description }, body };
    }
  }
  const plain = /^Description\n([\s\S]*?)\nContent\n([\s\S]*)$/.exec(text);
  if (plain) return { meta: { description: plain[1].trim() }, body: plain[2] };
  return { meta: {}, body: text };
};

const clip = (value, max) =>
  value.length > max ? `${value.slice(0, max - 1).trimEnd()}…` : value;

const buildRoleBrief = ({ title, sourceFile, description, sharedRules, body }) => {
  const parts = [
    `# ${title}`,
    "",
    `Role brief untuk agent office Hermes3D. Workflow source: \`${sourceFile}\`.`,
  ];
  if (description) parts.push("", description);
  parts.push("", "---", "", sharedRules.trim(), "", "---", "", "## Workflow", "", body.trim());
  return `${parts.join("\n")}\n`;
};

const buildJobMessage = (title) =>
  `Jalankan peran "${title}" sesuai brief peranmu (AGENTS.md di workspace-mu). ` +
  "Kerjakan tugas yang diminta pengguna; bila belum ada tugas spesifik, " +
  "laporkan kesiapanmu: ringkas peran, keahlian utama, dan langkah berikutnya.";

/** Minimal client for the Hermes3D gateway protocol through the Studio proxy. */
class GatewaySession {
  constructor(url) {
    this.url = url;
    this.nextId = 0;
    this.pending = new Map();
    this.hello = null;
  }

  connect() {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(this.url, {
        headers: { Origin: new URL(this.url).origin },
      });
      this.ws = ws;
      const timer = setTimeout(() => reject(new Error("handshake timed out")), 20_000);
      ws.on("unexpected-response", (_req, res) => {
        clearTimeout(timer);
        reject(new Error(`unexpected HTTP ${res.statusCode} from ${this.url}`));
      });
      ws.on("error", (err) => {
        clearTimeout(timer);
        reject(err);
      });
      ws.on("message", (data) => this.onMessage(String(data)));
      ws.on("open", () => {
        this.request("connect", {
          minProtocol: 3,
          maxProtocol: 4,
          client: {
            id: "seed-oee-graphene",
            version: "1.0.0",
            platform: process.platform,
            mode: "webchat",
          },
          role: "operator",
          scopes: [],
          caps: [],
          auth: { token: GATEWAY_TOKEN },
        })
          .then((hello) => {
            clearTimeout(timer);
            this.hello = hello;
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
    else pending.reject(new Error(`${frame.error?.code || "error"}: ${frame.error?.message || "request failed"}`));
  }

  request(method, params = {}, timeoutMs = 45_000) {
    const id = `seed-${++this.nextId}`;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`${method} timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      this.pending.set(id, { resolve, reject, timer });
      this.ws.send(JSON.stringify({ type: "req", id, method, params }));
    });
  }

  close() {
    try {
      this.ws?.close();
    } catch {
      /* closing twice is fine */
    }
  }
}

const main = async () => {
  if (!SOURCE_ROOT) {
    throw new Error(
      [
        "OEE_AGENTS_DIR is not set.",
        "",
        "Point it at the `.agents` folder of the source repository, e.g.:",
        "  OEE_AGENTS_DIR=C:\\path\\to\\oee-graphene\\.agents \\",
        "    node scripts/seed-oee-graphene-agents.js",
      ].join("\n")
    );
  }
  if (!fs.existsSync(WORKFLOWS_DIR)) {
    throw new Error(`Workflows folder not found: ${WORKFLOWS_DIR}`);
  }
  if (!fs.existsSync(SHARED_RULES_FILE)) {
    throw new Error(`Shared rules file not found: ${SHARED_RULES_FILE}`);
  }
  const sharedRules = fs.readFileSync(SHARED_RULES_FILE, "utf8");

  const workflows = fs
    .readdirSync(WORKFLOWS_DIR)
    .filter((name) => name.toLowerCase().endsWith(".md"))
    .sort()
    .map((name) => {
      const raw = fs.readFileSync(path.join(WORKFLOWS_DIR, name), "utf8");
      const { meta, body } = parseWorkflowFile(raw);
      const title = titleize(path.basename(name, ".md"));
      return {
        file: name,
        title,
        role: clip(meta.description || `${title} specialist`, ROLE_MAX_CHARS),
        brief: buildRoleBrief({
          title,
          sourceFile: path.join(WORKFLOWS_DIR, name),
          description: meta.description || "",
          sharedRules,
          body,
        }),
        jobMessage: buildJobMessage(title),
      };
    });

  console.log(`Source: ${SOURCE_ROOT}`);
  console.log(`Gateway: ${WS_URL}`);
  console.log(`Workflows found: ${workflows.length}`);

  const session = new GatewaySession(WS_URL);
  const hello = await session.connect();
  console.log(`Connected (adapterType=${hello.adapterType}, protocol=${hello.protocol})`);

  const existingAgents = await session.request("agents.list", {});
  const agentNameSet = new Set((existingAgents.agents || []).map((a) => a.name));
  const existingJobs = await session.request("cron.list", { includeDisabled: true });
  const jobNameSet = new Set((existingJobs.jobs || []).map((j) => j.name));

  const results = [];
  try {
    for (const workflow of workflows) {
      let agentId;
      let workspace;
      if (agentNameSet.has(workflow.title)) {
        const row = (existingAgents.agents || []).find((a) => a.name === workflow.title);
        agentId = row?.id;
        workspace = row?.workspace;
        console.log(`= "${workflow.title}" already exists (${agentId}), writing brief only`);
      } else {
        const created = await session.request("agents.create", {
          name: workflow.title,
          role: workflow.role,
        });
        agentId = created.agentId;
        workspace = created.workspace;
        console.log(`+ agent "${workflow.title}" -> ${agentId} @ ${workspace}`);
      }
      if (!agentId) {
        results.push({ title: workflow.title, status: "FAILED (no agentId)" });
        continue;
      }
      await session.request("agents.files.set", {
        agentId,
        name: "AGENTS.md",
        content: workflow.brief,
      });

      const jobName = `${workflow.title} Role Job`;
      let jobStatus;
      if (jobNameSet.has(jobName)) {
        jobStatus = "job exists";
      } else {
        const job = await session.request("cron.add", {
          name: jobName,
          agentId,
          enabled: false,
          schedule: { kind: "cron", expr: JOB_SCHEDULE },
          sessionTarget: "isolated",
          wakeMode: "now",
          payload: { kind: "agentTurn", message: workflow.jobMessage },
        });
        jobStatus = job?.enabled === false ? "job paused" : "job created (check state!)";
      }
      results.push({ title: workflow.title, agentId, status: jobStatus });
    }
  } finally {
    session.close();
  }

  console.log("\nSeeded agents:");
  for (const row of results) {
    console.log(`  ${row.title.padEnd(28)} ${row.agentId || "-".padEnd(4)} ${row.status}`);
  }
  const failed = results.filter((row) => String(row.status).startsWith("FAILED"));
  if (failed.length > 0) {
    console.error(`\n${failed.length} workflow(s) failed.`);
    process.exitCode = 1;
  }
};

main().catch((err) => {
  console.error(`seed failed: ${err.message}`);
  process.exitCode = 1;
});
