#!/usr/bin/env node
/**
 * MCP server that exposes the Hermes3D office (agents, playbook jobs, chat) to
 * any MCP client, so work can be driven from outside the office UI — e.g. from
 * OpenCode.
 *
 * Transport is MCP stdio (newline-delimited JSON-RPC 2.0), implemented directly
 * rather than via @modelcontextprotocol/sdk: the surface is three methods and
 * stdio framing is one line per message, so the SDK would only add a
 * dependency to a repo that does not ship one for this.
 *
 * The upstream gateway bridge keeps per-connection state (sessions, active
 * runs), so this process holds ONE persistent WebSocket to Hermes3D and
 * multiplexes every tool call through it.
 *
 * Usage (OpenCode mcp config):
 *   {
 *     "mcp": {
 *       "hermes3d": {
 *         "type": "local",
 *         "command": ["node", "D:/hermes3d/scripts/hermes3d-mcp-server.js"],
 *         "enabled": true
 *       }
 *     }
 *   }
 *
 * Requires HERMES3D_GATEWAY_TOKEN in the environment or the repo .env.
 */
const WebSocket = require("ws");
const { ORIGIN, resolveGatewayToken, resolveWsUrl } = require("./lib/gateway-config");

const SERVER_INFO = { name: "hermes3d-office", version: "1.0.0" };
const PROTOCOL_FALLBACK = "2025-06-18";
const REQUEST_TIMEOUT_MS = 60_000;
const DEFAULT_WAIT_MS = 180_000;
const MAX_WAIT_MS = 600_000;

/** stdout is the MCP channel: everything else must go to stderr. */
const log = (message) => process.stderr.write(`[hermes3d-mcp] ${message}\n`);

const send = (message) => {
  process.stdout.write(`${JSON.stringify(message)}\n`);
};

// --- gateway link -----------------------------------------------------------

let socket = null;
let connecting = null;
let nextRequestId = 0;
const pending = new Map();

const failAllPending = (error) => {
  for (const [id, entry] of pending) {
    clearTimeout(entry.timer);
    pending.delete(id);
    entry.reject(error);
  }
};

const connect = () => {
  if (socket && socket.readyState === WebSocket.OPEN) return Promise.resolve();
  if (connecting) return connecting;

  const token = resolveGatewayToken();
  if (!token) {
    return Promise.reject(
      new Error(
        "No gateway token. Set HERMES3D_GATEWAY_TOKEN in the environment or in D:/hermes3d/.env.",
      ),
    );
  }

  const url = resolveWsUrl();
  connecting = new Promise((resolve, reject) => {
    const ws = new WebSocket(url, { headers: { Origin: ORIGIN } });
    const handshake = setTimeout(() => {
      try {
        ws.terminate();
      } catch {}
      reject(new Error(`Timed out connecting to ${url}.`));
    }, 20_000);

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
      else {
        entry.reject(new Error(`${frame.error?.code ?? "error"}: ${frame.error?.message ?? ""}`));
      }
    });

    ws.on("open", async () => {
      try {
        await rawRequest(
          ws,
          "connect",
          {
            minProtocol: 3,
            maxProtocol: 4,
            client: {
              id: "opencode-mcp",
              version: SERVER_INFO.version,
              platform: process.platform,
              mode: "webchat",
            },
            role: "operator",
            scopes: [],
            caps: [],
            auth: { token },
          },
          20_000,
        );
        clearTimeout(handshake);
        socket = ws;
        log(`connected to ${url.split("?")[0]}`);
        resolve();
      } catch (err) {
        clearTimeout(handshake);
        try {
          ws.terminate();
        } catch {}
        reject(err);
      }
    });

    ws.on("error", (err) => {
      clearTimeout(handshake);
      reject(err);
    });

    ws.on("close", () => {
      if (socket === ws) socket = null;
      failAllPending(new Error("Hermes3D gateway connection closed."));
    });
  }).finally(() => {
    connecting = null;
  });

  return connecting;
};

/** Send one request on a socket, bypassing the shared `pending` table. */
const rawRequest = (ws, method, params, timeoutMs) =>
  new Promise((resolve, reject) => {
    const id = `mcp-${++nextRequestId}`;
    const timer = setTimeout(() => {
      pending.delete(id);
      reject(new Error(`${method} timed out after ${Math.round(timeoutMs / 1000)}s.`));
    }, timeoutMs);
    pending.set(id, { resolve, reject, timer });
    ws.send(JSON.stringify({ type: "req", id, method, params }));
  });

/** Send one request over the shared, lazily (re)opened connection. */
const request = async (method, params = {}, timeoutMs = REQUEST_TIMEOUT_MS) => {
  await connect();
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    throw new Error("Not connected to Hermes3D.");
  }
  const id = `mcp-${++nextRequestId}`;
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      pending.delete(id);
      reject(new Error(`${method} timed out after ${Math.round(timeoutMs / 1000)}s.`));
    }, timeoutMs);
    pending.set(id, { resolve, reject, timer });
    socket.send(JSON.stringify({ type: "req", id, method, params }));
  });
};

// --- office helpers ---------------------------------------------------------

const mainKeyFor = (agentId) => `agent:${agentId}:main`;

const listAgents = async () => {
  const result = await request("agents.list", {}, 20_000);
  return {
    defaultId: result?.defaultId ?? "",
    agents: (result?.agents ?? []).map((agent) => ({
      id: agent.id,
      name: agent.name,
      role: agent.role || "",
      workspace: agent.workspace || "",
    })),
  };
};

const listJobs = async () => {
  const result = await request("cron.list", { includeDisabled: true }, 20_000);
  return (result?.jobs ?? []).map((job) => ({
    id: job.id,
    name: job.name,
    agentId: job.agentId || "",
    schedule: job.schedule || "",
    enabled: job.enabled !== false,
    lastStatus: job.lastStatus || "",
  }));
};

/** Recent messages from an agent's main session. */
const readMessages = async (agentId, limit = 12) => {
  const history = await request("chat.history", { sessionKey: mainKeyFor(agentId), limit }, 30_000);
  const messages = history?.messages ?? [];
  return messages.map((message) => ({
    role: message.role ?? "?",
    content: String(message.content ?? ""),
  }));
};

/** Last assistant turn from an agent's main session. */
const readReply = async (agentId, limit = 12) => {
  const messages = await readMessages(agentId, limit);
  for (let i = messages.length - 1; i >= 0; i -= 1) {
    if (messages[i].role === "assistant") return messages[i].content;
  }
  return "";
};

/**
 * Wait for a run to finish, then return the agent's reply.
 *
 * `agent.wait` polls the bridge's activeRuns table on this same connection, so
 * a run started by this process is visible to it.
 */
const awaitReply = async (agentId, runId, waitMs) => {
  const startedAt = Date.now();
  const status = await request("agent.wait", { runId, timeoutMs: waitMs }, waitMs + 15_000);
  const elapsedMs = Date.now() - startedAt;
  if (status?.status !== "done") {
    return {
      status: "running",
      reply: "",
      elapsedMs,
      note: `Still running after ${Math.round(elapsedMs / 1000)}s. Call hermes_get_history to read the reply later.`,
    };
  }
  return { status: "done", reply: await readReply(agentId), elapsedMs };
};

const clampWait = (value) => {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) return DEFAULT_WAIT_MS;
  return Math.min(Math.max(value, 1_000), MAX_WAIT_MS);
};

const requireAgent = async (agentId) => {
  if (!agentId) throw new Error("agentId is required.");
  const { agents } = await listAgents();
  const found = agents.find((agent) => agent.id === agentId);
  if (!found) {
    throw new Error(
      `Unknown agent "${agentId}". Known agents: ${agents.map((a) => a.id).join(", ")}`,
    );
  }
  return found;
};

// --- tools ------------------------------------------------------------------

const TOOLS = [
  {
    name: "hermes_list_agents",
    description:
      "List the agents in the Hermes3D office with their ids, roles and workspace paths. Call this first to discover valid agentId values.",
    inputSchema: { type: "object", properties: {}, additionalProperties: false },
  },
  {
    name: "hermes_send_message",
    description:
      "Send a task to one office agent and wait for its reply. The agent runs in its own workspace with its AGENTS.md brief as context.",
    inputSchema: {
      type: "object",
      properties: {
        agentId: { type: "string", description: "Agent id from hermes_list_agents." },
        message: { type: "string", description: "Instruction or question for the agent." },
        waitMs: {
          type: "number",
          description: `How long to wait for the reply, in ms (default ${DEFAULT_WAIT_MS}, max ${MAX_WAIT_MS}).`,
        },
      },
      required: ["agentId", "message"],
      additionalProperties: false,
    },
  },
  {
    name: "hermes_get_history",
    description:
      "Read recent messages from an agent's main session. Use it to pick up a reply that was still running.",
    inputSchema: {
      type: "object",
      properties: {
        agentId: { type: "string" },
        limit: { type: "number", description: "How many messages to return (default 12)." },
      },
      required: ["agentId"],
      additionalProperties: false,
    },
  },
  {
    name: "hermes_list_jobs",
    description:
      "List playbook jobs in the office, including paused ones. Each paused job belongs to exactly one agent.",
    inputSchema: { type: "object", properties: {}, additionalProperties: false },
  },
  {
    name: "hermes_run_job",
    description:
      "Fire a playbook job now (the same thing the office Run now button does) and wait for the agent's reply.",
    inputSchema: {
      type: "object",
      properties: {
        jobId: { type: "string", description: "Job id from hermes_list_jobs." },
        waitMs: { type: "number", description: "How long to wait for the reply, in ms." },
      },
      required: ["jobId"],
      additionalProperties: false,
    },
  },
  {
    name: "hermes_create_agent",
    description:
      "Create a new office agent with a paused placeholder job. Writes AGENTS.md into its workspace when brief is given.",
    inputSchema: {
      type: "object",
      properties: {
        name: { type: "string", description: "Display name, e.g. \"Data Engineer\"." },
        role: { type: "string", description: "Short role summary." },
        brief: { type: "string", description: "Contents of the agent's AGENTS.md." },
      },
      required: ["name"],
      additionalProperties: false,
    },
  },
  {
    name: "hermes_abort",
    description: "Abort the agent's running turn.",
    inputSchema: {
      type: "object",
      properties: { agentId: { type: "string" } },
      required: ["agentId"],
      additionalProperties: false,
    },
  },
];

const runTool = async (name, args) => {
  switch (name) {
    case "hermes_list_agents":
      return listAgents();

    case "hermes_list_jobs":
      return { jobs: await listJobs() };

    case "hermes_get_history": {
      await requireAgent(args.agentId);
      return {
        agentId: args.agentId,
        messages: await readMessages(args.agentId, Number(args.limit) || 12),
      };
    }

    case "hermes_send_message": {
      if (typeof args.message !== "string" || !args.message.trim()) {
        throw new Error("message is required.");
      }
      await requireAgent(args.agentId);
      const run = await request(
        "chat.send",
        { sessionKey: mainKeyFor(args.agentId), message: args.message.trim() },
        30_000,
      );
      if (run?.status !== "started") return { status: run?.status ?? "unknown", reply: "" };
      const settled = await awaitReply(args.agentId, run.runId, clampWait(args.waitMs));
      return { agentId: args.agentId, runId: run.runId, ...settled };
    }

    case "hermes_run_job": {
      const jobs = await listJobs();
      const job = jobs.find((entry) => entry.id === args.jobId);
      if (!job) {
        throw new Error(
          `Unknown job "${args.jobId}". Known jobs: ${jobs.map((j) => j.id).join(", ")}`,
        );
      }
      // cron.run returns no runId, so poll the agent's session for a new
      // assistant turn instead of waiting on agent.wait.
      const before = (await readMessages(job.agentId, 30)).length;
      const result = await request("cron.run", { id: job.id }, 120_000);
      if (result?.ok !== true) {
        return {
          ok: false,
          job: job.name,
          note: "The gateway refused the run. Backend-native jobs have no stored prompt to replay.",
        };
      }
      const waitMs = clampWait(args.waitMs);
      const deadline = Date.now() + waitMs;
      let messages = [];
      while (Date.now() < deadline) {
        messages = await readMessages(job.agentId, 30);
        const fresh = messages.slice(before);
        if (fresh.some((message) => message.role === "assistant" && message.content.trim())) {
          const reply = [...fresh].reverse().find((m) => m.role === "assistant");
          return {
            ok: true,
            job: job.name,
            agentId: job.agentId,
            status: "done",
            reply: reply ? reply.content : "",
            elapsedMs: Date.now() - (deadline - waitMs),
          };
        }
        await new Promise((resolve) => setTimeout(resolve, 3_000));
      }
      return {
        ok: true,
        job: job.name,
        agentId: job.agentId,
        status: "running",
        reply: "",
        note: `Still running after ${Math.round(waitMs / 1000)}s. Call hermes_get_history with agentId "${job.agentId}" to read the reply later.`,
      };
    }

    case "hermes_create_agent": {
      const created = await request(
        "agents.create",
        { name: args.name, role: args.role || "" },
        30_000,
      );
      const agentId = created?.agentId;
      if (typeof args.brief === "string" && args.brief.trim()) {
        await request(
          "agents.files.set",
          { agentId, name: "AGENTS.md", content: args.brief },
          30_000,
        );
      }
      return {
        agentId,
        name: created?.name ?? args.name,
        workspace: created?.workspace ?? "",
        wroteBrief: Boolean(args.brief && args.brief.trim()),
      };
    }

    case "hermes_abort": {
      const result = await request("chat.abort", { sessionKey: mainKeyFor(args.agentId) }, 30_000);
      return { agentId: args.agentId, aborted: result?.aborted ?? 0 };
    }

    default:
      throw new Error(`Unknown tool "${name}".`);
  }
};

// --- MCP protocol -----------------------------------------------------------

const handle = async (message) => {
  const { id, method, params } = message;
  // Notifications carry no id and expect no reply.
  if (id === undefined || id === null) return;

  switch (method) {
    case "initialize":
      return send({
        jsonrpc: "2.0",
        id,
        result: {
          protocolVersion:
            typeof params?.protocolVersion === "string" ? params.protocolVersion : PROTOCOL_FALLBACK,
          capabilities: { tools: { listChanged: false } },
          serverInfo: SERVER_INFO,
        },
      });

    case "ping":
      return send({ jsonrpc: "2.0", id, result: {} });

    case "tools/list":
      return send({ jsonrpc: "2.0", id, result: { tools: TOOLS } });

    case "tools/call": {
      const name = params?.name;
      const args = params?.arguments ?? {};
      try {
        const payload = await runTool(name, args);
        return send({
          jsonrpc: "2.0",
          id,
          result: {
            content: [{ type: "text", text: JSON.stringify(payload, null, 2) }],
            isError: false,
          },
        });
      } catch (err) {
        // Tool failures are reported in-band so the client can show the reason.
        return send({
          jsonrpc: "2.0",
          id,
          result: {
            content: [{ type: "text", text: `Error: ${err.message}` }],
            isError: true,
          },
        });
      }
    }

    default:
      return send({
        jsonrpc: "2.0",
        id,
        error: { code: -32601, message: `Method not found: ${method}` },
      });
  }
};

let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  let newline = buffer.indexOf("\n");
  while (newline !== -1) {
    const line = buffer.slice(0, newline).trim();
    buffer = buffer.slice(newline + 1);
    newline = buffer.indexOf("\n");
    if (!line) continue;
    let message;
    try {
      message = JSON.parse(line);
    } catch {
      send({ jsonrpc: "2.0", id: null, error: { code: -32700, message: "Parse error" } });
      continue;
    }
    handle(message).catch((err) => log(`handler failed: ${err.message}`));
  }
});

process.stdin.on("end", () => {
  try {
    socket?.close();
  } catch {}
  process.exit(0);
});

log(`ready (gateway ${resolveWsUrl()})`);
