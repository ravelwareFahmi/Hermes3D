#!/usr/bin/env node
/**
 * Minimal MCP stdio client used to smoke-test scripts/hermes3d-mcp-server.js
 * without OpenCode. Sends one tools/call and prints the result.
 *
 * Usage:
 *   node scripts/test-mcp-client.js <toolName> [jsonArgs]
 *   node scripts/test-mcp-client.js <toolName> --args-file <path>
 */
const { spawn } = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");

const TOOL = process.argv[2] || "hermes_list_agents";
const readArgs = () => {
  const fileIndex = process.argv.indexOf("--args-file");
  if (fileIndex !== -1 && process.argv[fileIndex + 1]) {
    return JSON.parse(fs.readFileSync(process.argv[fileIndex + 1], "utf8"));
  }
  return process.argv[3] ? JSON.parse(process.argv[3]) : {};
};
const ARGS = readArgs();

const child = spawn(process.execPath, [path.join(__dirname, "hermes3d-mcp-server.js")], {
  stdio: ["pipe", "pipe", "pipe"],
});

let nextId = 0;
const pending = new Map();
let buffer = "";

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

const call = (method, params, timeoutMs = 240_000) =>
  new Promise((resolve, reject) => {
    const id = ++nextId;
    const timer = setTimeout(() => {
      pending.delete(id);
      reject(new Error(`${method} timed out`));
    }, timeoutMs);
    pending.set(id, { resolve, reject, timer });
    child.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", id, method, params })}\n`);
  });

const main = async () => {
  const init = await call("initialize", {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "test-client", version: "1.0.0" },
  });
  console.log(`initialize -> ${init.result?.serverInfo?.name} (protocol ${init.result?.protocolVersion})`);

  const listed = await call("tools/list", {});
  console.log(`tools -> ${(listed.result?.tools || []).map((t) => t.name).join(", ")}`);

  console.log(`\ncalling ${TOOL} ${JSON.stringify(ARGS)} ...`);
  const started = Date.now();
  const result = await call("tools/call", { name: TOOL, arguments: ARGS });
  const elapsed = ((Date.now() - started) / 1000).toFixed(1);
  const text = result.result?.content?.[0]?.text ?? JSON.stringify(result);

  console.log(`\n--- result (${elapsed}s, isError=${result.result?.isError ?? false}) ---`);
  console.log(text.length > 2500 ? `${text.slice(0, 2500)}\n... (${text.length} chars total)` : text);

  child.stdin.end();
  child.kill();
  if (result.result?.isError) process.exitCode = 1;
};

main().catch((err) => {
  console.error(`test failed: ${err.message}`);
  child.kill();
  process.exitCode = 1;
});
