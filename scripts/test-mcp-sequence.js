#!/usr/bin/env node
/**
 * Runs a fixed MCP sequence inside ONE server process, which is how a real MCP
 * client (OpenCode) behaves. This matters because the gateway bridge keeps
 * sessions per connection: a fresh process per call cannot see history written
 * by an earlier call.
 *
 * Usage: node scripts/test-mcp-sequence.js
 */
const { spawn } = require("node:child_process");
const path = require("node:path");

const child = spawn(process.execPath, [path.join(__dirname, "hermes3d-mcp-server.js")], {
  stdio: ["pipe", "pipe", "pipe"],
});

let nextId = 0;
let buffer = "";
const pending = new Map();

child.stdout.on("data", (chunk) => {
  buffer += chunk;
  let i = buffer.indexOf("\n");
  while (i !== -1) {
    const line = buffer.slice(0, i).trim();
    buffer = buffer.slice(i + 1);
    i = buffer.indexOf("\n");
    if (!line) continue;
    let frame;
    try {
      frame = JSON.parse(line);
    } catch {
      continue;
    }
    const entry = pending.get(frame.id);
    if (!entry) continue;
    pending.delete(frame.id);
    clearTimeout(entry.timer);
    entry.resolve(frame);
  }
});

child.stderr.on("data", (chunk) => {
  const text = String(chunk).trim();
  if (text) console.error(`  [server] ${text}`);
});

const call = (method, params, timeoutMs = 300_000) =>
  new Promise((resolve, reject) => {
    const id = ++nextId;
    const timer = setTimeout(() => {
      pending.delete(id);
      reject(new Error(`${method} timed out`));
    }, timeoutMs);
    pending.set(id, { resolve, reject, timer });
    child.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", id, method, params })}\n`);
  });

const tool = async (name, args = {}) => {
  const started = Date.now();
  const frame = await call("tools/call", { name, arguments: args });
  const elapsed = ((Date.now() - started) / 1000).toFixed(1);
  const text = frame.result?.content?.[0]?.text ?? JSON.stringify(frame);
  console.log(`\n[${name}] ${elapsed}s isError=${frame.result?.isError ?? false}`);
  console.log(text.length > 900 ? `${text.slice(0, 900)}\n... (${text.length} chars)` : text);
  return text;
};

const main = async () => {
  await call("initialize", {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "sequence-test", version: "1.0.0" },
  });

  await tool("hermes_send_message", {
    agentId: "planner",
    message: "Reply with exactly: SEQ-OK",
    waitMs: 120_000,
  });

  console.log("\n--- same process, reading history ---");
  await tool("hermes_get_history", { agentId: "planner", limit: 4 });

  child.stdin.end();
  child.kill();
};

main().catch((err) => {
  console.error(`sequence failed: ${err.message}`);
  child.kill();
  process.exitCode = 1;
});
