/**
 * Translates the Hermes3D gateway protocol into hermes-agent's JSON-RPC 2.0 API.
 *
 * hermes-agent has no server that speaks the Hermes3D protocol — that protocol
 * came from OpenClaw. Rather than run a separate adapter process, this module
 * presents a virtual upstream to `gateway-proxy.js`: it exposes the small slice
 * of the `ws` WebSocket surface the proxy actually uses (`readyState`, `send`,
 * `close`, `terminate`, and the open/message/close/error events), so the proxy's
 * connect handling and lifecycle stay untouched.
 *
 * Hermes3D talks in agents and session keys; hermes-agent talks in runtime
 * session ids. A hermes-agent backend is a single agent, so the fleet is
 * synthesised as one entry and session keys are mapped onto runtime sessions
 * that are created or resumed on first use.
 */

const { EventEmitter } = require("node:events");
const { randomUUID } = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const { HermesAgentJsonRpcClient, redactUrl } = require("./jsonrpc-client");
const { createOfficeSpeechSubscriber } = require("./office-speech");
const {
  KANBAN_TASK_ID_PREFIX,
  toHermes3dKanbanTaskRecord,
  toHermes3dKanbanTasks,
  toKanbanPatchBody,
  kanbanRequest,
} = require("./kanban");

/** Mirrors the numeric WebSocket readyState constants the proxy compares against. */
const CONNECTING = 0;
const OPEN = 1;
const CLOSED = 3;

const AGENT_ID = "hermes";
const AGENT_NAME = "Hermes";
const MAIN_KEY = "main";
const MAIN_SESSION_KEY = `agent:${AGENT_ID}:${MAIN_KEY}`;

/** hermes-agent's `session.create` / `session.resume` can be slow on a cold profile. */
const SESSION_RPC_TIMEOUT_MS = 60_000;

// --- synthesised-agent store ------------------------------------------------
//
// hermes-agent is a single-agent backend: it has no profile or agent creation
// RPC at all. Hermes3D's office is a fleet, so multi-agent support is done
// here — the roster of bridge-created agents and their playbook jobs lives in
// a small JSON store next to the Studio settings, and each agent gets a
// workspace directory whose AGENTS.md hermes injects as the role brief.

/** Studio-side state directory for the hermes-agent adapter. */
const STATE_DIR = path.join(os.homedir(), ".hermes", "hermes3d");
/** Persisted roster: { agents: [...], jobs: { "<job_id>": {...} } }. */
const VIRTUAL_STORE_FILE = path.join(STATE_DIR, "virtual-agents.json");
/** Reported through `config.get` so the UI can derive per-agent workspaces. */
const VIRTUAL_CONFIG_PATH = path.join(STATE_DIR, "config.json");
/** Agent directive files are plain markdown leaves inside the agent workspace. */
const AGENT_FILE_NAME = /^[A-Za-z0-9_-]+\.md$/;

const loadVirtualStore = () => {
  try {
    const parsed = JSON.parse(fs.readFileSync(VIRTUAL_STORE_FILE, "utf8"));
    return {
      agents: Array.isArray(parsed?.agents) ? parsed.agents : [],
      jobs: parsed?.jobs && typeof parsed.jobs === "object" ? parsed.jobs : {},
    };
  } catch {
    return { agents: [], jobs: {} };
  }
};

const saveVirtualStore = (store) => {
  fs.mkdirSync(STATE_DIR, { recursive: true });
  const tmp = `${VIRTUAL_STORE_FILE}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify(store, null, 2)}\n`, "utf8");
  fs.renameSync(tmp, VIRTUAL_STORE_FILE);
};

const isExistingDirectory = (value) => {
  try {
    return fs.statSync(value).isDirectory();
  } catch {
    return false;
  }
};

const slugifyAgentName = (name) =>
  asString(name)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 48);

/**
 * The slice of the `ws` WebSocket surface `gateway-proxy.js` relies on.
 *
 * @typedef {import("node:events").EventEmitter & {
 *   readyState: number,
 *   send: (raw: string) => void,
 *   close: (code?: number, reason?: string) => void,
 *   terminate: () => void,
 * }} HermesAgentUpstream
 */

const resOk = (id, payload) => ({ type: "res", id, ok: true, payload: payload ?? {} });
const resErr = (id, code, message) => ({ type: "res", id, ok: false, error: { code, message } });

const asString = (value, fallback = "") =>
  typeof value === "string" && value.trim() ? value.trim() : fallback;

const errorMessage = (err) => {
  if (!err) return "hermes-agent request failed";
  if (typeof err === "string") return err;
  return err.message || String(err);
};

/** hermes-agent history rows use `text`; Hermes3D expects `content`. */
const toHermes3dMessages = (messages) => {
  if (!Array.isArray(messages)) return [];
  return messages
    .filter((m) => m && (m.role === "user" || m.role === "assistant"))
    .map((m) => ({
      role: m.role,
      content: typeof m.text === "string" ? m.text : String(m.content ?? ""),
    }));
};

const parseTimestampMs = (value) => {
  if (typeof value !== "string" || !value.trim()) return undefined;
  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? undefined : parsed;
};

const DURATION_UNIT_MS = { s: 1000, m: 60_000, h: 3_600_000, d: 86_400_000 };

/**
 * hermes-agent stores a schedule as one string; Hermes3D wants a tagged union.
 *
 * The string is a 5-field cron expression, a duration such as "30m", or an ISO
 * timestamp for a one-shot job.
 */
const toHermes3dSchedule = (raw) => {
  const value = asString(raw);
  if (!value) return { kind: "cron", expr: "" };

  const duration = /^every\s+(\d+)\s*([smhd])$/i.exec(value) || /^(\d+)\s*([smhd])$/i.exec(value);
  if (duration) {
    const unit = DURATION_UNIT_MS[duration[2].toLowerCase()];
    if (unit) return { kind: "every", everyMs: Number(duration[1]) * unit };
  }

  if (value.split(/\s+/).length === 5) return { kind: "cron", expr: value };

  const at = parseTimestampMs(value);
  if (at !== undefined) return { kind: "at", at: value };

  return { kind: "cron", expr: value };
};

const CRON_STATUSES = new Set(["ok", "error", "skipped"]);

/**
 * Translate hermes-agent cron rows into Hermes3D's `CronJobSummary`.
 *
 * The two shapes disagree on nearly every field: hermes-agent uses `job_id`, a
 * schedule string, `prompt_preview`, ISO timestamps, and a `state` *string*,
 * while Hermes3D expects `id`, a schedule object, a `payload` object, epoch
 * milliseconds, and a `state` object. Forwarding the raw rows crashes the
 * office task board, which reads `job.payload.kind` unguarded.
 */
const toHermes3dCronJobs = (jobs, agentForJob = AGENT_ID) => {
  if (!Array.isArray(jobs)) return [];
  const resolveAgentId =
    typeof agentForJob === "function" ? agentForJob : () => agentForJob;
  return jobs
    .filter((job) => job && typeof job === "object")
    .map((job) => {
      const nextRunAtMs = parseTimestampMs(job.next_run_at);
      const lastRunAtMs = parseTimestampMs(job.last_run_at);
      const lastStatus = asString(job.last_status).toLowerCase();
      const lastError = asString(job.last_fire_error) || asString(job.last_delivery_error);
      const message =
        asString(job.prompt_preview) || asString(job.name) || "Scheduled job";

      const jobId = asString(job.job_id) || asString(job.id);
      return {
        id: jobId,
        name: asString(job.name) || jobId || "Scheduled job",
        agentId: resolveAgentId(jobId),
        description: asString(job.prompt_preview) || undefined,
        enabled: job.enabled !== false,
        updatedAtMs: lastRunAtMs ?? nextRunAtMs ?? Date.now(),
        schedule: toHermes3dSchedule(job.schedule),
        sessionTarget: "isolated",
        wakeMode: "next-heartbeat",
        payload: { kind: "agentTurn", message },
        state: {
          ...(nextRunAtMs !== undefined ? { nextRunAtMs } : {}),
          ...(lastRunAtMs !== undefined ? { lastRunAtMs } : {}),
          ...(asString(job.state).toLowerCase() === "running"
            ? { runningAtMs: Date.now() }
            : {}),
          ...(CRON_STATUSES.has(lastStatus) ? { lastStatus } : {}),
          ...(lastError ? { lastError } : {}),
        },
        delivery: { mode: asString(job.deliver) && job.deliver !== "none" ? "announce" : "none" },
      };
    })
    .filter((job) => job.id);
};

/** Used when the backend predates `profiles.list` or the call fails. */
const fallbackAgent = () => ({
  id: AGENT_ID,
  name: AGENT_NAME,
  workspace: "",
  identity: { name: AGENT_NAME, emoji: "🤖" },
  role: "",
  profile: "",
});

/** Title-case a profile directory name for display ("allan" -> "Allan"). */
const toDisplayName = (name) =>
  name ? name.charAt(0).toUpperCase() + name.slice(1) : "";

/**
 * Turn hermes-agent's profile list into Hermes3D agents.
 *
 * A profile is the backend's unit of identity — its own model, skills, memory
 * and sessions — which is exactly what Hermes3D calls an agent. Collapsing them
 * into one entry hides the operator's whole fleet, so each profile gets a desk.
 *
 * The `profile` field is what routes sessions back; the default profile carries
 * an empty string because omitting `profile` means "launch profile" upstream.
 */
const toHermes3dAgents = (profiles) => {
  if (!Array.isArray(profiles)) return [];
  const agents = profiles
    .filter((p) => p && typeof p === "object" && asString(p.name))
    .map((p) => {
      const name = asString(p.name);
      const isDefault = p.is_default === true;
      const display = asString(p.display_name) || toDisplayName(name);
      // The long profile descriptions read as a role ("Allan — technical
      // planner…"); keep the part after the dash so the desk label stays short.
      const description = asString(p.description);
      const role = description.includes("—")
        ? description.split("—").slice(1).join("—").trim()
        : description;
      return {
        id: name,
        name: display,
        workspace: asString(p.path),
        identity: { name: display, emoji: isDefault ? "🤖" : "🧑‍💻" },
        role,
        model: asString(p.model),
        isDefault,
        profile: isDefault ? "" : name,
      };
    });
  return agents;
};

const resolveDefaultAgentId = (agents) => {
  const explicit = agents.find((a) => a.isDefault);
  return explicit?.id ?? agents[0]?.id ?? AGENT_ID;
};

/** Persisted store row -> Hermes3D agent row. */
const toVirtualAgent = (row) => {
  const name = asString(row?.name) || asString(row?.id, "Agent");
  return {
    id: asString(row?.id),
    name,
    workspace: asString(row?.workspace),
    identity: { name, emoji: asString(row?.emoji, "🧑‍💻") },
    role: asString(row?.role),
    profile: "",
    virtual: true,
  };
};

/**
 * Hermes3D schedule union -> the one string `cron.manage` accepts.
 *
 * `parse_schedule` understands 5-field cron, `every <n><m|h|d>` intervals
 * (no seconds), and one-shot timestamps.
 */
const toBackendSchedule = (schedule) => {
  if (!schedule || typeof schedule !== "object") return "0 9 * * *";
  if (schedule.kind === "every") {
    const everyMs = Number(schedule.everyMs);
    if (Number.isFinite(everyMs) && everyMs > 0) {
      if (everyMs % 86_400_000 === 0) return `every ${everyMs / 86_400_000}d`;
      if (everyMs % 3_600_000 === 0) return `every ${everyMs / 3_600_000}h`;
      // Durations below a minute round up: parse_duration has no seconds unit.
      return `every ${Math.max(1, Math.ceil(everyMs / 60_000))}m`;
    }
  }
  if (schedule.kind === "at" && asString(schedule.at)) return asString(schedule.at);
  return asString(schedule.expr) || "0 9 * * *";
};

/**
 * The scheduler runs job prompts from its own cwd, not the agent's workspace,
 * so the role brief is handed over as an absolute path to read first.
 */
const buildJobPrompt = (agent, message) => {
  const name = asString(agent?.name, AGENT_NAME);
  const workspace = asString(agent?.workspace);
  const brief = workspace
    ? `You are the office agent "${name}". Read your role brief at ` +
      `${path.join(workspace, "AGENTS.md")} and follow it for this task.`
    : `You are the office agent "${name}".`;
  return `${brief}\n\nTask:\n${message}`;
};

/**
 * Hermes3D `CronJobSummary` for a job the backend list hides.
 *
 * `cron.manage list` never returns paused jobs (`include_disabled` is not
 * exposed over RPC), but a paused playbook still belongs in the office's
 * Active Jobs, so it is re-hydrated from the local store.
 */
const toStoredCronJob = (jobId, meta) => ({
  id: jobId,
  name: asString(meta?.name, jobId),
  agentId: asString(meta?.agentId, AGENT_ID),
  description: asString(meta?.prompt) || undefined,
  enabled: meta?.enabled === true,
  updatedAtMs: Number(meta?.createdAt) || Date.now(),
  schedule: toHermes3dSchedule(asString(meta?.schedule)),
  sessionTarget: "isolated",
  wakeMode: "next-heartbeat",
  payload: {
    kind: "agentTurn",
    message: asString(meta?.message, asString(meta?.prompt, "Scheduled job")),
  },
  state: {
    ...(Number.isFinite(meta?.lastRunAtMs) ? { lastRunAtMs: meta.lastRunAtMs } : {}),
    ...(meta?.lastStatus ? { lastStatus: meta.lastStatus } : {}),
  },
  delivery: { mode: "none" },
});

function createHermesAgentUpstream(options) {
  const {
    url,
    token,
    handshakeTimeoutMs,
    log = () => {},
    logError = () => {},
  } = options || {};

  const upstream = /** @type {HermesAgentUpstream} */ (new EventEmitter());
  upstream.readyState = CONNECTING;

  let client;
  try {
    client = new HermesAgentJsonRpcClient({ url, token, handshakeTimeoutMs, log });
  } catch (err) {
    // Defer so the caller can attach listeners before the failure lands.
    setImmediate(() => upstream.emit("error", err));
    upstream.readyState = CLOSED;
    upstream.send = () => {};
    upstream.close = () => {};
    upstream.terminate = () => {};
    return upstream;
  }

  /** sessionKey -> { runtimeId, storedId, title } */
  const sessions = new Map();
  /** runtime session id -> sessionKey */
  const sessionKeyByRuntimeId = new Map();
  /**
   * Hermes3D agents, one per hermes-agent profile.
   *
   * Profiles are the backend's fleet: each is a fully isolated instance with
   * its own model, skills, memory, and session store. Resolved once at connect
   * because the set only changes when the operator creates or deletes one.
   */
  let agentRoster = [fallbackAgent()];
  let defaultAgentId = AGENT_ID;
  /** Optional feed of turns driven from other clients; see ./office-speech.js. */
  let officeSpeech = null;
  /** runId -> { sessionKey, runtimeId, buffer, aborted } */
  const activeRuns = new Map();
  /** sessionKey -> runId, so session-scoped events find their run. */
  const runBySessionKey = new Map();

  let seq = 0;
  let closed = false;

  const emitFrame = (frame) => {
    if (closed) return;
    upstream.emit("message", JSON.stringify(frame));
  };

  const emitEvent = (event, payload) => {
    emitFrame({ type: "event", event, seq: seq++, payload });
  };

  const emitChat = (runId, sessionKey, state, extra) => {
    emitEvent("chat", { runId, sessionKey, state, ...extra });
  };

  // --- session mapping ------------------------------------------------------

  const rememberSession = (sessionKey, result) => {
    const runtimeId = asString(result?.session_id);
    if (!runtimeId) throw new Error("hermes-agent did not return a session id.");
    const entry = {
      runtimeId,
      storedId: asString(result?.stored_session_id) || asString(result?.session_key),
      title: asString(result?.info?.title) || asString(result?.title),
    };
    sessions.set(sessionKey, entry);
    sessionKeyByRuntimeId.set(runtimeId, sessionKey);
    return entry;
  };

  /**
   * Split `agent:<agentId>:<tail>` into the agent and the stored session id.
   */
  const parseSessionKey = (sessionKey) => {
    const parts = String(sessionKey ?? "").split(":");
    if (parts[0] !== "agent" || parts.length < 3) {
      return { agentId: defaultAgentId, tail: "" };
    }
    return { agentId: parts[1] || defaultAgentId, tail: parts.slice(2).join(":") };
  };

  /**
   * The `profile` value to send upstream for an agent.
   *
   * Empty means "the launch profile", which is what hermes-agent expects for
   * the default; naming it explicitly is unnecessary and, for a backend with no
   * profiles at all, would be rejected.
   */
  const profileForAgent = (agentId) => {
    const agent = agentRoster.find((a) => a.id === agentId);
    return agent ? asString(agent.profile) : "";
  };

  /** Main session of whichever agent is default; the roster is known only at runtime. */
  const defaultMainKey = () => `agent:${defaultAgentId}:${MAIN_KEY}`;

  /**
   * Resolve the runtime session backing a Hermes3D session key, creating or
   * resuming one on hermes-agent the first time the key is used.
   *
   * The agent segment of the key selects the profile, so a prompt sent to
   * `agent:allan:main` runs with Allan's model, skills, and session store.
   */
  const ensureSession = async (sessionKey) => {
    const existing = sessions.get(sessionKey);
    if (existing?.runtimeId) return existing;

    const { agentId, tail } = parseSessionKey(sessionKey);
    const profile = profileForAgent(agentId);
    const scope = profile ? { profile } : {};
    // A synthesised agent's workspace carries its AGENTS.md role brief, and
    // hermes injects the context files under the session cwd — so the brief
    // only reaches the model when the session runs inside that workspace.
    const rosterEntry = agentRoster.find((a) => a.id === agentId);
    const workspace = rosterEntry?.virtual ? asString(rosterEntry.workspace) : "";
    if (workspace && isExistingDirectory(workspace)) scope.cwd = workspace;

    // A key of the form `agent:<id>:<storedId>` refers to a stored hermes-agent
    // session; anything else starts a fresh one.
    if (tail && tail !== MAIN_KEY) {
      try {
        const resumed = await client.request(
          "session.resume",
          { session_id: tail, omit_messages: false, ...scope },
          SESSION_RPC_TIMEOUT_MS
        );
        return rememberSession(sessionKey, resumed);
      } catch (err) {
        log(`[hermes-agent] resume of "${tail}" failed, creating a new session: ${errorMessage(err)}`);
      }
    }

    const created = await client.request("session.create", scope, SESSION_RPC_TIMEOUT_MS);
    log(`[hermes-agent] session for "${sessionKey}" -> profile "${profile || "(default)"}"`);
    return rememberSession(sessionKey, created);
  };

  // --- upstream event fan-out ----------------------------------------------

  client.on("event", (type, runtimeSessionId, payload) => {
    const sessionKey = sessionKeyByRuntimeId.get(runtimeSessionId);
    if (!sessionKey) return;
    const runId = runBySessionKey.get(sessionKey);
    const run = runId ? activeRuns.get(runId) : null;

    switch (type) {
      case "message.start":
        if (run) run.buffer = "";
        return;

      case "message.delta": {
        if (!run || run.aborted) return;
        const text = typeof payload?.text === "string" ? payload.text : "";
        if (!text) return;
        run.buffer += text;
        emitChat(runId, sessionKey, "delta", {
          message: { role: "assistant", content: run.buffer },
        });
        return;
      }

      case "message.complete": {
        if (!run) return;
        const finalText =
          typeof payload?.text === "string" && payload.text ? payload.text : run.buffer;
        if (run.aborted) {
          emitChat(runId, sessionKey, "aborted", {});
        } else if (payload?.status === "error" || payload?.error) {
          emitChat(runId, sessionKey, "error", {
            errorMessage: asString(payload?.error, "hermes-agent reported an error"),
          });
        } else {
          emitChat(runId, sessionKey, "final", {
            stopReason: "end_turn",
            message: { role: "assistant", content: finalText },
          });
          emitEvent("presence", {
            sessions: {
              recent: [{ key: sessionKey, updatedAt: Date.now() }],
              byAgent: [
                {
                  agentId: parseSessionKey(sessionKey).agentId,
                  recent: [{ key: sessionKey, updatedAt: Date.now() }],
                },
              ],
            },
          });
        }
        activeRuns.delete(runId);
        runBySessionKey.delete(sessionKey);
        return;
      }

      case "tool.start":
        if (!run) return;
        emitEvent("agent", {
          runId,
          sessionKey,
          stream: "tool",
          data: { phase: "start", name: asString(payload?.name), text: asString(payload?.context) },
        });
        return;

      case "tool.complete":
        if (!run) return;
        emitEvent("agent", {
          runId,
          sessionKey,
          stream: "tool",
          data: { phase: "complete", name: asString(payload?.name), text: asString(payload?.summary) },
        });
        return;

      case "reasoning.delta":
      case "thinking.delta":
        if (!run) return;
        emitEvent("agent", {
          runId,
          sessionKey,
          stream: "reasoning",
          data: { phase: "delta", text: typeof payload?.text === "string" ? payload.text : "" },
        });
        return;

      case "status.update":
        if (!run) return;
        emitEvent("agent", {
          runId,
          sessionKey,
          stream: "lifecycle",
          data: { phase: asString(payload?.kind, "status"), text: asString(payload?.text) },
        });
        return;

      case "approval.request":
        emitEvent("exec.approval.requested", {
          id: asString(payload?.request_id),
          request: { command: asString(payload?.command), cwd: asString(payload?.cwd) },
          createdAtMs: Date.now(),
          expiresAtMs: Date.now() + 120_000,
        });
        return;

      case "error":
        if (!run) return;
        emitChat(runId, sessionKey, "error", {
          errorMessage: asString(payload?.message, "hermes-agent reported an error"),
        });
        activeRuns.delete(runId);
        runBySessionKey.delete(sessionKey);
        return;

      default:
    }
  });

  // --- method dispatch ------------------------------------------------------

  /**
   * Rebuild the roster from disk so an agent created or deleted on one
   * connection is seen by every connection (and by the next roster read).
   */
  const applyVirtualAgents = () => {
    const virtuals = loadVirtualStore()
      .agents.map(toVirtualAgent)
      .filter((agent) => agent.id);
    agentRoster = [...agentRoster.filter((agent) => !agent.virtual), ...virtuals];
  };

  /** Load the fleet once per connection; a backend without profiles keeps one agent. */
  const loadAgentRoster = async () => {
    try {
      const result = await client.request("profiles.list", {}, SESSION_RPC_TIMEOUT_MS);
      const mapped = toHermes3dAgents(result?.profiles);
      if (mapped.length > 0) {
        agentRoster = mapped;
        defaultAgentId = resolveDefaultAgentId(mapped);
        log(`[hermes-agent] ${mapped.length} profile(s) mapped to agents: ${mapped.map((a) => a.id).join(", ")}`);
        applyVirtualAgents();
        return;
      }
      log("[hermes-agent] profiles.list returned nothing; using a single agent");
    } catch (err) {
      log(`[hermes-agent] profiles.list unavailable (${errorMessage(err)}); using a single agent`);
    }
    applyVirtualAgents();
    const virtualCount = agentRoster.filter((a) => a.virtual).length;
    if (virtualCount > 0) {
      log(`[hermes-agent] roster: default agent + ${virtualCount} synthesised agent(s)`);
    }
  };

  /**
   * Relay a turn published by the office bridge plugin.
   *
   * The plugin names the profile that spoke, and Hermes3D names each agent
   * after its profile, so the two line up directly. A turn from a profile this
   * connection does not know about is dropped rather than guessed at.
   */
  const handlePublishedTurn = (turn) => {
    const agent = agentRoster.find((a) => a.id === turn.profile);
    if (!agent) {
      log(`[office-speech] no agent for profile "${turn.profile}"; turn ignored`);
      return;
    }
    emitEvent("office.speech", {
      agentId: agent.id,
      name: agent.name,
      text: turn.text,
      atMs: turn.atMs,
      sessionId: turn.sessionId,
    });
  };

  const startOfficeSpeech = () => {
    if (officeSpeech) return;
    officeSpeech = createOfficeSpeechSubscriber({
      url,
      token,
      onTurn: handlePublishedTurn,
      log,
    });
  };

  const handleConnect = async (id) => {
    await loadAgentRoster();
    // Only worth subscribing once the roster exists to map turns onto.
    startOfficeSpeech();
    const agents = agentRoster.map((a) => ({
      agentId: a.id,
      name: a.name,
      isDefault: a.id === defaultAgentId,
    }));
    return resOk(id, {
      type: "hello-ok",
      protocol: 3,
      // The client trusts this over the configured type once connected, so
      // report what the backend actually is — hermes-agent capabilities
      // (native kanban, profile fleet) hang off this detection.
      adapterType: "hermes-agent",
      features: {
        methods: [
          "agents.list",
          "agents.create",
          "agents.update",
          "agents.delete",
          "agents.files.get",
          "agents.files.set",
          "sessions.list",
          "sessions.preview",
          "sessions.patch",
          "sessions.reset",
          "chat.send",
          "chat.abort",
          "chat.history",
          "agent.wait",
          "status",
          "config.get",
          "config.set",
          "config.patch",
          "exec.approvals.get",
          "exec.approvals.set",
          "exec.approval.resolve",
          "wake",
          "skills.status",
          "models.list",
          "tasks.list",
          "tasks.update",
          "cron.list",
          "cron.add",
          "cron.remove",
          "cron.run",
        ],
        events: ["chat", "agent", "presence", "heartbeat", "cron"],
      },
      snapshot: {
        health: { agents, defaultAgentId },
        sessionDefaults: { mainKey: MAIN_KEY },
      },
      auth: { role: "operator", scopes: ["operator.admin", "operator.approvals"] },
      policy: { tickIntervalMs: 30_000 },
    });
  };

  const handleMethod = async (method, params, id) => {
    const p = params || {};

    switch (method) {
      case "agents.list":
        applyVirtualAgents();
        return resOk(id, {
          defaultId: defaultAgentId,
          mainKey: MAIN_KEY,
          agents: agentRoster.map(({ id: agentId, name, workspace, identity, role }) => ({
            id: agentId,
            name,
            workspace,
            identity,
            role,
          })),
        });

      case "agents.create": {
        const name = asString(p.name);
        if (!name) {
          return resErr(id, "hermes_agent.agent_name_required", "Agent name is required.");
        }
        const store = loadVirtualStore();
        const taken = new Set([
          ...agentRoster.map((a) => a.id),
          ...store.agents.map((a) => asString(a.id)),
        ]);
        const base = slugifyAgentName(name) || "agent";
        let agentId = base;
        for (let n = 2; taken.has(agentId); n += 1) agentId = `${base}-${n}`;
        // The UI derives a workspace from `config.get`'s path and hands it
        // back here; anything non-absolute falls back next to the store.
        const requested = asString(p.workspace);
        const workspace =
          requested && path.isAbsolute(requested)
            ? requested
            : path.join(STATE_DIR, `workspace-${agentId}`);
        try {
          fs.mkdirSync(workspace, { recursive: true });
        } catch (err) {
          return resErr(id, "hermes_agent.workspace_failed", errorMessage(err));
        }
        store.agents.push({
          id: agentId,
          name,
          workspace,
          role: asString(p.role),
          createdAt: Date.now(),
        });
        saveVirtualStore(store);
        applyVirtualAgents();
        log(`[hermes-agent] created agent "${name}" (${agentId}) at ${workspace}`);
        return resOk(id, { agentId, name, workspace });
      }

      case "agents.update": {
        const agentId = asString(p.agentId);
        const name = asString(p.name);
        if (!agentId || !name) {
          return resErr(id, "hermes_agent.agent_update_invalid", "agentId and name are required.");
        }
        const store = loadVirtualStore();
        const row = store.agents.find((a) => asString(a.id) === agentId);
        if (!row) {
          return resErr(id, "hermes_agent.agent_not_found", `Agent "${agentId}" not found.`);
        }
        row.name = name;
        if (asString(p.role)) row.role = asString(p.role);
        saveVirtualStore(store);
        applyVirtualAgents();
        return resOk(id, { ok: true, agentId, name });
      }

      case "agents.delete": {
        const agentId = asString(p.agentId);
        if (!agentId) {
          return resErr(id, "hermes_agent.agent_id_required", "agentId is required.");
        }
        const store = loadVirtualStore();
        const kept = store.agents.filter((a) => asString(a.id) !== agentId);
        if (kept.length === store.agents.length) {
          return resErr(id, "hermes_agent.agent_not_found", `Agent "${agentId}" not found.`);
        }
        store.agents = kept;
        saveVirtualStore(store);
        applyVirtualAgents();
        log(`[hermes-agent] deleted agent "${agentId}"`);
        return resOk(id, { ok: true, removed: true, removedBindings: 0 });
      }

      case "agents.files.get": {
        const agentId = asString(p.agentId);
        const name = asString(p.name);
        const row = loadVirtualStore().agents.find((a) => asString(a.id) === agentId);
        if (!row || !AGENT_FILE_NAME.test(name)) {
          return resOk(id, { file: { missing: true } });
        }
        const workspace = asString(row.workspace);
        const filePath = path.join(workspace, name);
        try {
          const content = fs.readFileSync(filePath, "utf8");
          return resOk(id, { workspace, file: { missing: false, content, path: filePath } });
        } catch {
          return resOk(id, { workspace, file: { missing: true } });
        }
      }

      case "agents.files.set": {
        const agentId = asString(p.agentId);
        const name = asString(p.name);
        const content = typeof p.content === "string" ? p.content : null;
        const row = loadVirtualStore().agents.find((a) => asString(a.id) === agentId);
        if (!row || content === null || !AGENT_FILE_NAME.test(name)) {
          return resErr(
            id,
            "hermes_agent.files_set_invalid",
            "agentId, a markdown file name, and content are required.",
          );
        }
        try {
          const workspace = asString(row.workspace);
          fs.mkdirSync(workspace, { recursive: true });
          fs.writeFileSync(path.join(workspace, name), content, "utf8");
        } catch (err) {
          return resErr(id, "hermes_agent.files_set_failed", errorMessage(err));
        }
        return resOk(id, { ok: true, agentId, name });
      }

      case "config.get":
        return resOk(id, {
          config: { gateway: { reload: { mode: "hot" } } },
          hash: "hermes-agent",
          exists: true,
          // Non-empty so `agents.create` can derive a default workspace.
          path: VIRTUAL_CONFIG_PATH,
        });

      case "config.patch":
      case "config.set":
        return resOk(id, { hash: "hermes-agent" });

      case "sessions.list": {
        // Each profile keeps its own session store, so the stored rows have to
        // be read per agent; they're local SQLite reads, so fan out in parallel.
        const perAgent = await Promise.all(
          agentRoster.map(async (agent) => {
            // Stored sessions belong to the backend's single default profile;
            // listing them under every synthesised agent would duplicate the
            // same rows across the whole fleet, so a virtual agent only owns
            // its main session key.
            if (agent.virtual) {
              return [
                {
                  key: `agent:${agent.id}:${MAIN_KEY}`,
                  agentId: agent.id,
                  updatedAt: Date.now(),
                  displayName: "Main",
                  origin: { label: agent.name, provider: "hermes" },
                  modelProvider: "hermes",
                },
              ];
            }
            const profile = asString(agent.profile);
            let stored = [];
            try {
              const result = await client.request("session.list", {
                limit: 20,
                ...(profile ? { profile } : {}),
              });
              stored = Array.isArray(result?.sessions) ? result.sessions : [];
            } catch (err) {
              log(`[hermes-agent] session.list for "${agent.id}" failed: ${errorMessage(err)}`);
            }
            return [
              {
                key: `agent:${agent.id}:${MAIN_KEY}`,
                agentId: agent.id,
                updatedAt: Date.now(),
                displayName: "Main",
                origin: { label: agent.name, provider: "hermes" },
                modelProvider: "hermes",
              },
              ...stored.map((s) => ({
                key: `agent:${agent.id}:${asString(s.id)}`,
                agentId: agent.id,
                updatedAt: typeof s.started_at === "number" ? s.started_at * 1000 : null,
                displayName: asString(s.title, "Session"),
                origin: { label: agent.name, provider: "hermes" },
                modelProvider: "hermes",
              })),
            ];
          })
        );
        return resOk(id, { sessions: perAgent.flat() });
      }

      case "sessions.preview": {
        const keys = Array.isArray(p.keys) ? p.keys : [];
        const limit = typeof p.limit === "number" ? p.limit : 8;
        const maxChars = typeof p.maxChars === "number" ? p.maxChars : 240;
        const previews = await Promise.all(
          keys.map(async (key) => {
            const entry = sessions.get(key);
            if (!entry?.runtimeId) return { key, status: "empty", items: [] };
            try {
              const history = await client.request("session.history", {
                session_id: entry.runtimeId,
              });
              const items = toHermes3dMessages(history?.messages)
                .slice(-limit)
                .map((m) => ({
                  role: m.role,
                  text: m.content.slice(0, maxChars),
                  timestamp: Date.now(),
                }));
              return { key, status: items.length ? "ok" : "empty", items };
            } catch {
              return { key, status: "empty", items: [] };
            }
          })
        );
        return resOk(id, { ts: Date.now(), previews });
      }

      case "sessions.patch": {
        const key = asString(p.key, defaultMainKey());
        const model = typeof p.model === "string" ? p.model.trim() : "";
        if (model) {
          try {
            const entry = await ensureSession(key);
            await client.request("config.set", {
              key: "model",
              value: model,
              session_id: entry.runtimeId,
            });
          } catch (err) {
            log(`[hermes-agent] model switch failed: ${errorMessage(err)}`);
          }
        }
        return resOk(id, {
          ok: true,
          key,
          entry: { thinkingLevel: p.thinkingLevel },
          resolved: { model: model || undefined, modelProvider: "hermes" },
        });
      }

      case "sessions.reset": {
        const key = asString(p.key, defaultMainKey());
        const entry = sessions.get(key);
        if (entry?.runtimeId) {
          sessionKeyByRuntimeId.delete(entry.runtimeId);
          try {
            await client.request("session.close", { session_id: entry.runtimeId });
          } catch {}
        }
        sessions.delete(key);
        return resOk(id, { ok: true });
      }

      case "chat.send": {
        const sessionKey = asString(p.sessionKey, defaultMainKey());
        const text =
          typeof p.message === "string" ? p.message.trim() : String(p.message ?? "").trim();
        const runId = asString(p.idempotencyKey) || randomUUID();
        if (!text) return resOk(id, { status: "no-op", runId });

        let entry;
        try {
          entry = await ensureSession(sessionKey);
        } catch (err) {
          return resErr(id, "hermes_agent.session_failed", errorMessage(err));
        }

        activeRuns.set(runId, { sessionKey, runtimeId: entry.runtimeId, buffer: "", aborted: false });
        runBySessionKey.set(sessionKey, runId);

        try {
          await client.request("prompt.submit", { session_id: entry.runtimeId, text });
        } catch (err) {
          activeRuns.delete(runId);
          runBySessionKey.delete(sessionKey);
          return resErr(id, "hermes_agent.prompt_failed", errorMessage(err));
        }

        return resOk(id, { status: "started", runId });
      }

      case "chat.abort": {
        const runId = asString(p.runId);
        const sessionKey = asString(p.sessionKey);
        const targets = runId
          ? [runId]
          : [...activeRuns.entries()]
              .filter(([, run]) => run.sessionKey === sessionKey)
              .map(([rid]) => rid);

        let aborted = 0;
        for (const rid of targets) {
          const run = activeRuns.get(rid);
          if (!run) continue;
          run.aborted = true;
          aborted += 1;
          try {
            await client.request("session.interrupt", { session_id: run.runtimeId });
          } catch (err) {
            log(`[hermes-agent] interrupt failed: ${errorMessage(err)}`);
          }
        }
        return resOk(id, { ok: true, aborted });
      }

      case "chat.history": {
        const sessionKey = asString(p.sessionKey, defaultMainKey());
        const entry = sessions.get(sessionKey);
        if (!entry?.runtimeId) return resOk(id, { sessionKey, messages: [] });
        try {
          const history = await client.request("session.history", {
            session_id: entry.runtimeId,
          });
          return resOk(id, { sessionKey, messages: toHermes3dMessages(history?.messages) });
        } catch (err) {
          log(`[hermes-agent] session.history failed: ${errorMessage(err)}`);
          return resOk(id, { sessionKey, messages: [] });
        }
      }

      case "agent.wait": {
        const runId = asString(p.runId);
        const timeoutMs = typeof p.timeoutMs === "number" ? p.timeoutMs : 30_000;
        const start = Date.now();
        while (activeRuns.has(runId) && Date.now() - start < timeoutMs) {
          await new Promise((r) => setTimeout(r, 100));
        }
        return resOk(id, { status: activeRuns.has(runId) ? "running" : "done" });
      }

      case "status": {
        const recent = [...sessions.keys()].map((key) => ({ key, updatedAt: Date.now() }));
        const byAgent = agentRoster.map((agent) => ({
          agentId: agent.id,
          recent: recent.filter((entry) => parseSessionKey(entry.key).agentId === agent.id),
        }));
        return resOk(id, { sessions: { recent, byAgent } });
      }

      case "wake": {
        const text = asString(p.text);
        if (!text) return resOk(id, { ok: true });
        try {
          const entry = await ensureSession(defaultMainKey());
          await client.request("prompt.submit", { session_id: entry.runtimeId, text });
        } catch (err) {
          log(`[hermes-agent] wake failed: ${errorMessage(err)}`);
        }
        return resOk(id, { ok: true });
      }

      case "models.list": {
        try {
          const result = await client.request("model.options", {});
          // model.options returns { providers: [{ slug, name, models: [...] }], model, provider }.
          // Flatten each authenticated provider row into { provider, id } choices.
          const rows = Array.isArray(result?.providers) ? result.providers : [];
          const models = [];
          for (const row of rows) {
            const providerSlug = asString(row?.slug) || asString(row?.name);
            const list = Array.isArray(row?.models) ? row.models : [];
            for (const mid of list) {
              const modelId = asString(mid);
              if (!modelId) continue;
              models.push({ id: modelId, name: modelId, provider: providerSlug });
            }
          }
          return resOk(id, { models: models.length ? models : [{ id: "hermes", name: "hermes" }] });
        } catch {
          return resOk(id, { models: [{ id: "hermes", name: "hermes" }] });
        }
      }

      case "skills.status": {
        try {
          const result = await client.request("skills.manage", { action: "list" });
          // `get_available_skills()` groups names by category
          // ({ category: [names] }), while the office expects a flat
          // SkillStatusEntry[] — flatten and fill the status fields the
          // presentation layer derives readiness from.
          const raw = result?.skills;
          const names = Array.isArray(raw)
            ? raw
            : raw && typeof raw === "object"
              ? Object.values(raw).flat()
              : [];
          const skills = Array.from(new Set(names.filter((name) => typeof name === "string"))).map(
            (name) => ({
              name,
              description: "",
              source: "hermes-bundled",
              bundled: true,
              filePath: "",
              baseDir: "",
              skillKey: name,
              always: false,
              disabled: false,
              blockedByAllowlist: false,
              eligible: true,
              requirements: { bins: [], anyBins: [], env: [], config: [], os: [] },
              missing: { bins: [], anyBins: [], env: [], config: [], os: [] },
              configChecks: [],
              install: [],
            }),
          );
          return resOk(id, { skills });
        } catch {
          return resOk(id, { skills: [] });
        }
      }

      case "cron.list": {
        try {
          // cron.manage reads the launch profile's scheduler, so jobs are
          // attributed through the local store's job->agent mapping (backend
          // rows carry no agent id); jobs the backend hides because they are
          // paused are re-hydrated from that same store.
          const result = await client.request("cron.manage", { action: "list" });
          const store = loadVirtualStore();
          const summaries = toHermes3dCronJobs(result?.jobs, (jobId) =>
            asString(store.jobs[jobId]?.agentId, defaultAgentId),
          );
          for (const summary of summaries) {
            const message = asString(store.jobs[summary.id]?.message);
            if (message) summary.payload = { kind: "agentTurn", message };
          }
          const listed = new Set(summaries.map((entry) => entry.id));
          for (const [jobId, meta] of Object.entries(store.jobs)) {
            if (!listed.has(jobId)) summaries.push(toStoredCronJob(jobId, meta));
          }
          return resOk(id, { jobs: summaries });
        } catch (err) {
          log(`[hermes-agent] cron.manage failed: ${errorMessage(err)}`);
          return resOk(id, { jobs: [] });
        }
      }

      case "cron.add": {
        const name = asString(p.name);
        if (!name) {
          return resErr(id, "hermes_agent.cron_name_required", "Cron job name is required.");
        }
        const agentId = asString(p.agentId, defaultAgentId);
        const agent = agentRoster.find((a) => a.id === agentId);
        const payload = p.payload && typeof p.payload === "object" ? p.payload : {};
        const message = asString(payload.message) || asString(payload.text) || name;
        const prompt = buildJobPrompt(agent, message);
        const schedule = toBackendSchedule(p.schedule);

        let created;
        try {
          created = await client.request(
            "cron.manage",
            { action: "add", name, schedule, prompt },
            SESSION_RPC_TIMEOUT_MS
          );
        } catch (err) {
          return resErr(id, "hermes_agent.cron_create_failed", errorMessage(err));
        }
        const jobId = asString(created?.job_id);
        if (created?.success === false || !jobId) {
          return resErr(
            id,
            "hermes_agent.cron_create_failed",
            asString(created?.message, "hermes-agent did not create the job."),
          );
        }
        // `enabled: false` means "parked until Run now": create the row, then
        // pause it — the backend has no disabled-at-create option.
        const enabled = p.enabled !== false;
        if (!enabled) {
          try {
            await client.request(
              "cron.manage",
              { action: "pause", name: jobId },
              SESSION_RPC_TIMEOUT_MS
            );
          } catch (err) {
            log(`[hermes-agent] pause of cron job "${jobId}" failed: ${errorMessage(err)}`);
          }
        }
        const store = loadVirtualStore();
        store.jobs[jobId] = {
          agentId,
          name,
          prompt,
          message,
          schedule,
          enabled,
          createdAt: Date.now(),
        };
        saveVirtualStore(store);
        log(
          `[hermes-agent] cron job "${name}" (${jobId}) created for agent "${agentId}"` +
            (enabled ? "" : " (paused)"),
        );
        const summary = toHermes3dCronJobs(
          [created?.job ?? { job_id: jobId, name, schedule }],
          agentId,
        )[0];
        if (summary && !enabled) summary.enabled = false;
        return resOk(id, summary ?? { id: jobId, name, agentId, enabled });
      }

      case "cron.remove": {
        const jobId = asString(p.id);
        if (!jobId) {
          return resErr(id, "hermes_agent.cron_id_required", "Cron job id is required.");
        }
        let removed = false;
        try {
          const result = await client.request(
            "cron.manage",
            { action: "remove", name: jobId },
            SESSION_RPC_TIMEOUT_MS
          );
          removed = result?.success !== false;
        } catch (err) {
          log(`[hermes-agent] cron remove of "${jobId}" failed: ${errorMessage(err)}`);
        }
        // Local mapping goes even when the backend already lost the row, so a
        // paused job can never survive as a ghost in the office.
        const store = loadVirtualStore();
        if (store.jobs[jobId]) {
          delete store.jobs[jobId];
          saveVirtualStore(store);
        }
        return resOk(id, { ok: removed, removed });
      }

      case "cron.run": {
        const jobId = asString(p.id);
        const store = loadVirtualStore();
        const meta = store.jobs[jobId];
        if (!meta) {
          // A backend-native job (created outside the office) has no stored
          // prompt to replay — the gateway RPC has no run action either.
          return resOk(id, { ok: false });
        }
        const sessionKey = `agent:${asString(meta.agentId, defaultAgentId)}:${MAIN_KEY}`;
        let entry;
        try {
          entry = await ensureSession(sessionKey);
        } catch (err) {
          logError(`[hermes-agent] cron.run session for "${jobId}" failed.`, err);
          return resOk(id, { ok: false });
        }
        // Replay through the normal chat-run machinery so the office console
        // streams the turn like any other run.
        const runId = randomUUID();
        activeRuns.set(runId, { sessionKey, runtimeId: entry.runtimeId, buffer: "", aborted: false });
        runBySessionKey.set(sessionKey, runId);
        try {
          await client.request(
            "prompt.submit",
            { session_id: entry.runtimeId, text: asString(meta.prompt, meta.name) },
            SESSION_RPC_TIMEOUT_MS
          );
        } catch (err) {
          activeRuns.delete(runId);
          if (runBySessionKey.get(sessionKey) === runId) runBySessionKey.delete(sessionKey);
          logError(`[hermes-agent] cron.run of "${jobId}" failed.`, err);
          return resOk(id, { ok: false });
        }
        meta.lastRunAtMs = Date.now();
        meta.lastStatus = "ok";
        store.jobs[jobId] = meta;
        saveVirtualStore(store);
        return resOk(id, { ok: true, ran: true });
      }

      case "exec.approvals.get":
        return resOk(id, {
          path: "",
          exists: true,
          hash: "hermes-agent",
          file: {
            version: 1,
            defaults: { security: "full", ask: "off", autoAllowSkills: true },
            agents: {},
          },
        });

      case "exec.approvals.set":
        return resOk(id, { hash: "hermes-agent" });

      case "exec.approval.resolve": {
        const requestId = asString(p.id);
        const decision = asString(p.decision, "deny");
        const runtimeId = [...sessions.values()][0]?.runtimeId;
        if (requestId && runtimeId) {
          try {
            await client.request("approval.respond", {
              session_id: runtimeId,
              request_id: requestId,
              choice: decision === "allow" ? "once" : "deny",
            });
          } catch (err) {
            log(`[hermes-agent] approval.respond failed: ${errorMessage(err)}`);
          }
        }
        return resOk(id, { ok: true });
      }

      // Kanban is built into hermes-agent — the board rides the same origin
      // and session token as the JSON-RPC gateway, so the office task board
      // reflects the real `hermes kanban` board with nothing to install.
      case "tasks.list": {
        try {
          const includeArchived = p.includeArchived === false ? "false" : "true";
          const board = await kanbanRequest({
            wsUrl: url,
            token,
            useLoopbackHost: client.usedLoopbackHost,
            method: "GET",
            path: `/board?include_archived=${includeArchived}`,
          });
          return resOk(id, { tasks: toHermes3dKanbanTasks(board) });
        } catch (err) {
          // A hidden/disabled kanban plugin or older backend is not an error
          // state for the office — the board just has no hermes tasks.
          log(`[hermes-agent] kanban board unavailable: ${errorMessage(err)}`);
          return resOk(id, { tasks: [] });
        }
      }

      case "tasks.update": {
        const rawId = asString(p.id);
        if (!rawId.startsWith(KANBAN_TASK_ID_PREFIX)) {
          return resErr(
            id,
            "hermes_agent.tasks_update_unsupported",
            "Only Hermes kanban tasks can be updated on this backend.",
          );
        }
        const taskId = rawId.slice(KANBAN_TASK_ID_PREFIX.length);
        try {
          const result = await kanbanRequest({
            wsUrl: url,
            token,
            useLoopbackHost: client.usedLoopbackHost,
            method: "PATCH",
            path: `/tasks/${encodeURIComponent(taskId)}`,
            body: toKanbanPatchBody(p),
          });
          const record = toHermes3dKanbanTaskRecord(result?.task);
          if (!record) {
            return resErr(
              id,
              "hermes_agent.tasks_update_failed",
              "hermes-agent did not return the updated task.",
            );
          }
          return resOk(id, record);
        } catch (err) {
          return resErr(id, "hermes_agent.tasks_update_failed", errorMessage(err));
        }
      }

      default:
        log(`[hermes-agent] unhandled method: ${method}`);
        return resOk(id, {});
    }
  };

  // --- virtual WebSocket surface -------------------------------------------

  upstream.send = (raw) => {
    let frame;
    try {
      frame = JSON.parse(String(raw ?? ""));
    } catch {
      return;
    }
    if (!frame || frame.type !== "req") return;

    const { id, method, params } = frame;
    const respond = (result) => emitFrame(result);

    if (method === "connect") {
      handleConnect(id).then(respond, (err) =>
        respond(resErr(id, "hermes_agent.connect_failed", errorMessage(err)))
      );
      return;
    }

    handleMethod(method, params, id).then(respond, (err) => {
      logError(`[hermes-agent] method "${method}" failed.`, err);
      respond(resErr(id, "hermes_agent.request_failed", errorMessage(err)));
    });
  };

  const stopOfficeSpeech = () => {
    officeSpeech?.close();
    officeSpeech = null;
  };

  upstream.close = (code, reason) => {
    closed = true;
    upstream.readyState = CLOSED;
    stopOfficeSpeech();
    client.close(code, reason);
  };

  upstream.terminate = () => {
    closed = true;
    upstream.readyState = CLOSED;
    stopOfficeSpeech();
    client.terminate();
  };

  client.on("ready", () => {
    upstream.readyState = OPEN;
    log(`[hermes-agent] JSON-RPC gateway ready at ${redactUrl(client.url)}`);
    upstream.emit("open");
  });

  client.on("close", (code, reason) => {
    closed = true;
    upstream.readyState = CLOSED;
    stopOfficeSpeech();
    upstream.emit("close", code, Buffer.from(String(reason ?? "")));
  });

  client.on("error", (err) => {
    upstream.emit("error", err);
  });

  client.connect();

  return upstream;
}

module.exports = {
  createHermesAgentUpstream,
  toHermes3dMessages,
  toHermes3dCronJobs,
  toHermes3dSchedule,
  toHermes3dAgents,
  resolveDefaultAgentId,
  MAIN_SESSION_KEY,
  AGENT_ID,
};
