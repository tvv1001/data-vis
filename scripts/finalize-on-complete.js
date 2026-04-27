#!/usr/bin/env node
import { readFile, writeFile } from "node:fs/promises";
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
const GRAPH_FILE = path.join(
  ROOT,
  "server",
  "data",
  "national",
  "finra-graph.json",
);
const OUT_FILE = path.join(ROOT, "server", "final-summary.json");
const POLL_MS = 15000;

async function loadJson(file) {
  try {
    const raw = await readFile(file, "utf8");
    return JSON.parse(raw);
  } catch (e) {
    return null;
  }
}

async function saveJson(file, obj) {
  await writeFile(file, JSON.stringify(obj, null, 2), "utf8");
}

async function main() {
  const seeds = await loadJson(SEEDS_FILE);
  if (!Array.isArray(seeds)) {
    console.error("seeds file missing");
    process.exit(1);
  }
  const target = seeds.length;
  console.log("Finalizer: waiting for downloads target", target);

  while (true) {
    const prog = (await loadJson(PROG_FILE)) || { pos: 0 };
    const pos = Number(prog.pos || 0);
    console.log(new Date().toISOString(), "pos", pos, "/", target);
    if (pos >= target) break;
    await new Promise((r) => setTimeout(r, POLL_MS));
  }

  console.log("Downloads complete; triggering merge");
  try {
    const res = await axios.post(
      "http://localhost:3001/api/finra/run-scraper",
      {},
      { timeout: 300000 },
    );
    console.log("merge triggered");
  } catch (e) {
    console.error("merge call failed", e.message || e);
  }

  // Wait a bit for server to finish merging
  await new Promise((r) => setTimeout(r, 10000));

  const graph = await loadJson(GRAPH_FILE);
  const summary = {
    timestamp: new Date().toISOString(),
    seedsTarget: target,
    progressAtFinish: await loadJson(PROG_FILE),
    graphMeta: graph && graph.meta ? graph.meta : null,
    nodes: graph ? (graph.nodes ? graph.nodes.length : null) : null,
  };

  await saveJson(OUT_FILE, summary);
  console.log("Finalizer: summary written to", OUT_FILE);
  process.exit(0);
}

main().catch((e) => {
  console.error(e.stack || e);
  process.exit(1);
});
