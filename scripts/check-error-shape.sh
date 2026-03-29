#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

node - "$ROOT_DIR" <<'NODE'
const path = require("path");

function createMockRes() {
  return {
    statusCode: 200,
    headers: {},
    body: null,
    setHeader(key, value) {
      this.headers[String(key).toLowerCase()] = String(value);
    },
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.body = payload;
      return this;
    },
    end(payload) {
      this.body = payload ?? null;
      return this;
    },
  };
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function assertErrorShape(res, expectedStatus, expectedCode, label) {
  assert(res.statusCode === expectedStatus, `${label}: expected status ${expectedStatus}, got ${res.statusCode}`);
  assert(res.body && typeof res.body === "object", `${label}: response body must be an object`);
  assert(res.body.error && typeof res.body.error === "object", `${label}: missing error object`);
  assert(res.body.error.code === expectedCode, `${label}: expected code ${expectedCode}, got ${res.body.error.code}`);
  assert(typeof res.body.error.message === "string" && res.body.error.message.length > 0, `${label}: missing error.message`);
}

function assertHeaderPresence(res, label) {
  const required = [
    "access-control-allow-origin",
    "access-control-allow-methods",
    "x-content-type-options",
    "x-frame-options",
    "referrer-policy",
    "cache-control",
  ];
  for (const key of required) {
    assert(Boolean(res.headers[key]), `${label}: missing required header ${key}`);
  }
}

(async () => {
  const rootDir = process.argv[2];

  process.env.MATCH_CONTEXT_API_KEY = "ci_dummy_proxy_key";

  const demoHandler = require(path.join(rootDir, "api/demo-matches.js"));
  const matchContextHandler = require(path.join(rootDir, "api/match-context.js"));
  const trendsHandler = require(path.join(rootDir, "api/trends.js"));

  {
    const res = createMockRes();
    await demoHandler({ method: "POST", query: {} }, res);
    assertErrorShape(res, 405, "METHOD_NOT_ALLOWED", "demo POST");
    assertHeaderPresence(res, "demo POST");
  }

  {
    const res = createMockRes();
    await matchContextHandler({ method: "POST", query: {} }, res);
    assertErrorShape(res, 405, "METHOD_NOT_ALLOWED", "match-context POST");
    assertHeaderPresence(res, "match-context POST");
  }

  {
    const res = createMockRes();
    await matchContextHandler({ method: "GET", query: {} }, res);
    assertErrorShape(res, 400, "MISSING_MATCH_ID", "match-context missing match_id");
    assertHeaderPresence(res, "match-context missing match_id");
  }

  {
    const res = createMockRes();
    await trendsHandler({ method: "POST", query: {} }, res);
    assertErrorShape(res, 405, "METHOD_NOT_ALLOWED", "trends POST");
    assertHeaderPresence(res, "trends POST");
  }

  console.log("check-error-shape: PASS");
})().catch((error) => {
  console.error(`check-error-shape: FAIL - ${error.message}`);
  process.exit(1);
});
NODE
