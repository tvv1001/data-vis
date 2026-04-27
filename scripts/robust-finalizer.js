#!/usr/bin/env node
import fs from "node:fs/promises";
import path from "node:path";
import axios from "axios";

const ROOT = path.resolve(
  path.dirname(new URL(import.meta.url).pathname),
  "..",
);
const PROG_FILE = path.join(ROOT, "server", ".download-progress.json");
const MERGE_URL = "http://localhost:3001/api/finra/run-scraper";
const GRAPH_FILE = path.join(
  ROOT,
  "server",
  "data",
  "national",
  "finra-graph.json",
);
const SUMMARY_FILE = path.join(ROOT, "server", "final-summary.json");

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function readProgress() {
  try {
    const raw = await fs.readFile(PROG_FILE, "utf8");
    return JSON.parse(raw);
  } catch (e) {
    return { pos: 0 };
  }
}

async function fileExists(p) {
  try {
    await fs.access(p);
    return true;
  } catch (e) {
    return false;
  }
}

async function waitForGraph(timeoutMs = 1000 * 60 * 60) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await fileExists(GRAPH_FILE)) return true;
    await sleep(10 * 1000);
  }
  return false;
}

async function writeSummary(obj) {
  try {
    await fs.writeFile(SUMMARY_FILE, JSON.stringify(obj, null, 2), "utf8");
  } catch (e) {
    console.error("Failed to write summary", e.message || e);
  }
}

async function main() {
  console.log("robust-finalizer: starting");
  while (true) {
    const prog = await readProgress();
    const pos = Number(prog.pos || 0);
    console.log(new Date().toISOString(), "pos=", pos);
    if (pos >= 6732) {
      console.log("robust-finalizer: downloads complete; triggering merge");
      try {
        await axios.post(MERGE_URL, {}, { timeout: 0 });
        console.log("robust-finalizer: merge endpoint called");
      } catch (e) {
        console.error("robust-finalizer: merge call failed", e.message || e);
      }
      const ok = await waitForGraph();
      if (ok) {
        try {
          const st = await fs.stat(GRAPH_FILE);
          const summary = {
            finishedAt: new Date().toISOString(),
            pos,
            graphSize: st.size,
            graphMtime: st.mtime,
          };
          await writeSummary(summary);
          console.log("robust-finalizer: final summary written", SUMMARY_FILE);
        } catch (e) {
          console.error(
            "robust-finalizer: error reading graph file",
            e.message || e,
          );
        }
      } else {
        console.error(
          "robust-finalizer: timed out waiting for finra-graph.json",
        );
        await writeSummary({
          pos,
          error: "timeout_waiting_for_graph",
          timestamp: new Date().toISOString(),
        });
      }
      break;
    }
    await sleep(30 * 1000);
  }
  console.log("robust-finalizer: exiting");
}

main().catch((e) => {
  console.error("robust-finalizer: fatal", e.stack || e);
  process.exit(1);
});
