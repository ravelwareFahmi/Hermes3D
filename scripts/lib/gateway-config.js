/**
 * Shared gateway connection settings for the maintenance scripts in this
 * folder.
 *
 * Nothing here may carry a credential default: this repository is public, and
 * a hardcoded gateway token would leak the operator's local gateway to anyone
 * who can reach the port. Supply the token through the environment instead:
 *
 *   HERMES3D_GATEWAY_TOKEN=<token> node scripts/test-run-now.js
 *
 * `.env` is gitignored, so `HERMES3D_GATEWAY_TOKEN` set there is loaded when
 * the scripts are started from the repository root.
 */

const fs = require("fs");
const path = require("path");

const DEFAULT_WS_URL = "ws://127.0.0.1:3001/api/gateway/ws";
const ORIGIN = "http://localhost:3001";

/** Read a key from the repo .env without pulling in a dotenv dependency. */
const readDotEnv = (key) => {
  try {
    // scripts/lib/gateway-config.js -> repository root is two levels up.
    const envPath = path.resolve(__dirname, "..", "..", ".env");
    const raw = fs.readFileSync(envPath, "utf8");
    for (const line of raw.split(/\r?\n/)) {
      const match = /^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$/.exec(line);
      if (match && match[1] === key) {
        return match[2].trim().replace(/^["']|["']$/g, "");
      }
    }
  } catch {
    // No .env, or unreadable: fall through to "unset".
  }
  return "";
};

const resolveGatewayToken = () => {
  const fromEnv = (process.env.HERMES3D_GATEWAY_TOKEN || "").trim();
  return fromEnv || readDotEnv("HERMES3D_GATEWAY_TOKEN");
};

/**
 * Resolve the gateway URL, honouring an explicit --ws argument so the scripts
 * can be pointed at a non-default port.
 */
const resolveWsUrl = () => {
  const index = process.argv.indexOf("--ws");
  if (index !== -1 && process.argv[index + 1]) return process.argv[index + 1];
  return process.env.HERMES3D_GATEWAY_URL || DEFAULT_WS_URL;
};

/** Resolve the token, or exit with an actionable message when it is unset. */
const requireGatewayToken = () => {
  const token = resolveGatewayToken();
  if (!token) {
    console.error(
      [
        "No gateway token configured.",
        "",
        "Set HERMES3D_GATEWAY_TOKEN in the environment or in the repo .env file",
        "(the same value Hermes3D uses to open its gateway connection), then",
        "run the script again.",
      ].join("\n")
    );
    process.exit(1);
  }
  return token;
};

module.exports = {
  DEFAULT_WS_URL,
  ORIGIN,
  readDotEnv,
  resolveGatewayToken,
  resolveWsUrl,
  requireGatewayToken,
};
