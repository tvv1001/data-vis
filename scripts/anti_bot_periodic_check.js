#!/usr/bin/env node
import { spawnSync } from "child_process";
import fs from "fs";

const url = process.argv[2];
const intervalMs = Number(process.argv[3] || "60") * 1000;
const maxAttempts = Number(process.argv[4] || "60");
const python = process.env.PYTHON_BIN || "python3";
const scraper = "server/services/crawler/anti_bot_scraper.py";

if (!url) {
  console.error(
    "Usage: anti_bot_periodic_check.js <URL> [intervalSeconds] [maxAttempts]",
  );
  process.exit(2);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const now = new Date().toISOString();
    console.log(`[${now}] Attempt ${attempt}/${maxAttempts} -> ${url}`);
    const res = spawnSync(python, [scraper, url], {
      encoding: "utf8",
      maxBuffer: 20 * 1024 * 1024,
      env: process.env,
    });

    if (res.error) {
      console.error("Failed to spawn scraper:", res.error);
    }

    const out = res.stdout || "";
    try {
      const parsed = JSON.parse(out);
      const { ok, peoplePayload, signals } = parsed;
      if (ok && peoplePayload && peoplePayload.length > 0) {
        console.log(
          `[${now}] Found peoplePayload (length=${peoplePayload.length})`,
        );
        const p = `anti_bot_result_${Date.now()}.json`;
        fs.writeFileSync(p, JSON.stringify(parsed, null, 2));
        console.log("Wrote result to", p);
        process.exit(0);
      } else {
        console.log(`[${now}] No peoplePayload yet. signals=`, signals || {});
      }
    } catch (e) {
      console.error("Failed to parse scraper output:", e.message);
      console.log("Raw output (truncated):", out.slice(0, 2000));
    }

    if (attempt < maxAttempts) {
      console.log(`Sleeping ${intervalMs / 1000}s before next attempt...`);
      await sleep(intervalMs);
    }
  }

  console.log("Max attempts reached without finding peoplePayload.");
  process.exit(1);
})();
