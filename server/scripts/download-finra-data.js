#!/usr/bin/env node
import fs from "node:fs";
import { mkdir, writeFile, readFile } from "node:fs/promises";
import path from "node:path";
import Bottleneck from "bottleneck";
import axios from "axios";

const ROOT = path.resolve(
  path.dirname(new URL(import.meta.url).pathname),
  "..",
);
const DATA_DIR = path.join(ROOT, "data", "national");
const OUT_DIR = path.join(DATA_DIR, "brokercheck.finra.org");
const SEEDS_FILE = path.join(DATA_DIR, "finra-seeds.json");
const PROG_FILE = path.join(ROOT, ".download-progress.json");

const argv = Object.fromEntries(process.argv.slice(2).map((a) => a.split("=")));
const rate = Number(argv.rate || 1);
const concurrency = Number(argv.concurrency || 1);
const resume = argv.resume !== "false";

async function ensureDirs() {
  await mkdir(OUT_DIR, { recursive: true });
}

function safeName(n) {
  return n.replace(/[^a-z0-9_\-\.]/gi, "_").slice(0, 200);
}

async function loadSeeds() {
  try {
    const raw = await readFile(SEEDS_FILE, "utf-8");
    return JSON.parse(raw);
  } catch (e) {
    return [];
  }
}

async function loadProgress() {
  try {
    const raw = await readFile(PROG_FILE, "utf-8");
    return JSON.parse(raw);
  } catch (e) {
    return { pos: 0, individuals: {}, firms: {} };
  }
}

async function saveProgress(p) {
  await writeFile(PROG_FILE, JSON.stringify(p, null, 2), "utf-8");
}

async function saveJson(filePath, obj) {
  await writeFile(filePath, JSON.stringify(obj, null, 2), "utf-8");
}

async function main() {
  await ensureDirs();
  const seeds = await loadSeeds();
  if (!seeds.length) {
    console.error("No seeds found at", SEEDS_FILE);
    process.exitCode = 1;
    return;
  }

  const limiter = new Bottleneck({
    minTime: Math.ceil(1000 / Math.max(1, rate)),
    maxConcurrent: Math.max(1, concurrency),
  });

  const prog = resume
    ? await loadProgress()
    : { pos: 0, individuals: {}, firms: {} };

  console.log(
    `Starting download: seeds=${seeds.length} rate=${rate}/s concurrency=${concurrency} resume=${resume}`,
  );

  async function doSearch(name, index) {
    const baseUrl = "https://api.brokercheck.finra.org/search/individual";
    const params = { query: name, wt: "json" };
    const fname = `search_${index}_${safeName(name)}.json`;
    const outPath = path.join(OUT_DIR, fname);
    if (fs.existsSync(outPath)) return null;
    try {
      const r = await axios.get(baseUrl, {
        params,
        timeout: 20000,
        headers: { Accept: "application/json" },
      });
      await saveJson(outPath, r.data);
      return r.data;
    } catch (e) {
      console.error("Search failed for", name, e.message || e);
      return null;
    }
  }

  async function doIndividual(crd) {
    if (!crd) return null;
    if (prog.individuals[String(crd)]) return null;
    const url = `https://api.brokercheck.finra.org/search/individual/${encodeURIComponent(crd)}`;
    const outPath = path.join(OUT_DIR, `individual_${crd}.json`);
    if (fs.existsSync(outPath)) {
      prog.individuals[String(crd)] = true;
      await saveProgress(prog);
      return null;
    }
    try {
      const r = await axios.get(url, {
        timeout: 20000,
        headers: { Accept: "application/json" },
      });
      await saveJson(outPath, r.data);
      prog.individuals[String(crd)] = true;
      await saveProgress(prog);
      return r.data;
    } catch (e) {
      console.error("Individual fetch failed", crd, e.message || e);
      return null;
    }
  }

  async function doFirm(id) {
    if (!id) return null;
    if (prog.firms[String(id)]) return null;
    const url = `https://api.brokercheck.finra.org/search/firm/${encodeURIComponent(id)}`;
    const outPath = path.join(OUT_DIR, `firm_${id}.json`);
    if (fs.existsSync(outPath)) {
      prog.firms[String(id)] = true;
      await saveProgress(prog);
      return null;
    }
    try {
      const r = await axios.get(url, {
        timeout: 20000,
        headers: { Accept: "application/json" },
      });
      await saveJson(outPath, r.data);
      prog.firms[String(id)] = true;
      await saveProgress(prog);
      return r.data;
    } catch (e) {
      console.error("Firm fetch failed", id, e.message || e);
      return null;
    }
  }

  for (let i = prog.pos || 0; i < seeds.length; i++) {
    const name = seeds[i];
    console.log(`Processing [${i + 1}/${seeds.length}]: ${name}`);
    const data = await limiter.schedule(() => doSearch(name, i));
    if (data && data.hits && Array.isArray(data.hits.hits)) {
      for (const h of data.hits.hits) {
        const src = h._source || {};
        if (src.ind_source_id) {
          await limiter.schedule(() => doIndividual(src.ind_source_id));
        }
        const emps = src.ind_current_employments || [];
        for (const e of emps) {
          if (e && e.firm_id) await limiter.schedule(() => doFirm(e.firm_id));
        }
        if (src.firm_id) await limiter.schedule(() => doFirm(src.firm_id));
      }
    }
    prog.pos = i + 1;
    await saveProgress(prog);
  }

  console.log("Download complete.");
}

main().catch((e) => {
  console.error(e.stack || e);
  process.exit(1);
});
