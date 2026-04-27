#!/usr/bin/env node
import { readFile } from "node:fs/promises";
import path from "node:path";
import axios from "axios";

const ROOT = path.resolve(
  path.dirname(new URL(import.meta.url).pathname),
  "..",
);
const SEEDS_FILE = path.join(
  ROOT,
  "server",
  "data",
  "national",
  "finra-seeds.json",
);
const PROG_FILE = path.join(ROOT, "server", ".download-progress.json");
const POLL_MS = 30 * 1000;

function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

async function loadJson(file) {
  try {
    const raw = await readFile(file, "utf8");
    return JSON.parse(raw);
  } catch (e) {
    return null;
  }
}

async function main() {
  const seeds = await loadJson(SEEDS_FILE);
  if (!Array.isArray(seeds)) {
    console.error("Could not read seeds file at", SEEDS_FILE);
    process.exit(1);
  }
  const target = seeds.length;
  console.log(`Monitor started: target seeds=${target}`);

  while (true) {
    const prog = (await loadJson(PROG_FILE)) || { pos: 0 };
    const pos = Number(prog.pos || 0);
    console.log(new Date().toISOString(), `progress ${pos}/${target}`);
    if (pos >= target) {
      console.log(
        "All seeds downloaded (pos >= target). Triggering /api/finra/run-scraper",
      );
      try {
        const res = await axios.post(
          "http://localhost:3001/api/finra/run-scraper",
          {},
          { timeout: 120000 },
        );
        console.log(
          "run-scraper response:",
          typeof res.data === "object"
            ? JSON.stringify(res.data)
            : String(res.data),
        );
      } catch (e) {
        console.error("run-scraper call failed:", e.message || e);
      }
      console.log("Monitor exiting.");
      process.exit(0);
    }
    await sleep(POLL_MS);
  }
}

main().catch((e) => {
  console.error(e.stack || e);
  process.exit(1);
});
