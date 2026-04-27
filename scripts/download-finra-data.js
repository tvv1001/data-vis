#!/usr/bin/env node
import fs from "node:fs";
import { mkdir, writeFile, readFile } from "node:fs/promises";
import path from "node:path";
import Bottleneck from "bottleneck";
import axios from "axios";

async function sleep(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

async function axiosGetWithRetry(url, config = {}) {
  const maxRetries = 5;
  let attempt = 0;
  let delay = 1000;
  // track consecutive 429s across calls to pause globally if too many occur
  if (typeof axiosGetWithRetry._consecutive429 === "undefined") {
    axiosGetWithRetry._consecutive429 = 0;
  }
  while (true) {
    try {
      return await axios.get(url, config);
    } catch (e) {
      attempt += 1;
      const status = e?.response?.status;
      const headers = e?.response?.headers || {};
      if (attempt > maxRetries) throw e;
      if (status === 429 || !e.response) {
        // compute delay from Retry-After header when available
        let retryAfter = 0;
        if (headers["retry-after"]) {
          const ra = headers["retry-after"];
          const asInt = parseInt(ra, 10);
          if (!Number.isNaN(asInt)) retryAfter = asInt * 1000;
          else {
            const dateTs = Date.parse(ra);
            if (!Number.isNaN(dateTs))
              retryAfter = Math.max(0, dateTs - Date.now());
          }
        }
        // apply jitter to avoid thundering herd
        const jitter = 0.5 + Math.random(); // [0.5,1.5)
        let useDelay = retryAfter > 0 ? retryAfter : delay;
        useDelay = Math.min(5 * 60 * 1000, Math.round(useDelay * jitter));
        console.warn(
          `Request failed (status=${status || "network"}) - retry ${attempt}/${maxRetries} after ${useDelay}ms`,
        );
        if (status === 429) {
          axiosGetWithRetry._consecutive429 += 1;
        } else {
          axiosGetWithRetry._consecutive429 = 0;
        }
        if (axiosGetWithRetry._consecutive429 >= 10) {
          console.warn(
            `Observed ${axiosGetWithRetry._consecutive429} consecutive 429s — pausing for 5 minutes to avoid further rate-limiting`,
          );
          await sleep(5 * 60 * 1000);
          axiosGetWithRetry._consecutive429 = 0;
        }
        await sleep(useDelay);
        delay = Math.min(30000, Math.round(delay * 2));
        continue;
      }
      throw e;
    }
  }
}

const ROOT = path.resolve(
  path.dirname(new URL(import.meta.url).pathname),
  "..",
);
const DATA_DIR = path.join(ROOT, "server", "data", "national");
const OUT_DIR = path.join(DATA_DIR, "brokercheck.finra.org");
const SEC_OUT_DIR = path.join(DATA_DIR, "adviserinfo.sec.gov");
const SEEDS_FILE = path.join(DATA_DIR, "finra-seeds.json");
const PROG_FILE = path.join(
  path.resolve(ROOT, "server"),
  ".download-progress.json",
);

const argv = Object.fromEntries(process.argv.slice(2).map((a) => a.split("=")));
const rate = Number(argv.rate || 2);
const concurrency = Number(argv.concurrency || 2);
const resume = argv.resume !== "false";

async function ensureDirs() {
  await mkdir(OUT_DIR, { recursive: true });
  await mkdir(SEC_OUT_DIR, { recursive: true });
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
      const r = await axiosGetWithRetry(baseUrl, {
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

  async function doSecSearch(name, index) {
    const baseUrl = "https://api.adviserinfo.sec.gov/search/individual";
    const params = { query: name, wt: "json" };
    const fname = `sec_search_${index}_${safeName(name)}.json`;
    const outPath = path.join(SEC_OUT_DIR, fname);
    if (fs.existsSync(outPath)) return null;
    try {
      const r = await axiosGetWithRetry(baseUrl, {
        params,
        timeout: 20000,
        headers: { Accept: "application/json" },
      });
      await saveJson(outPath, r.data);
      return r.data;
    } catch (e) {
      console.error("SEC search failed for", name, e.message || e);
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
      const r = await axiosGetWithRetry(url, {
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
      const r = await axiosGetWithRetry(url, {
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
    let scheduledAny = false;
    if (data && data.hits && Array.isArray(data.hits.hits)) {
      for (const h of data.hits.hits) {
        const src = h._source || {};
        const crd = src.ind_source_id || src.firm_id || src?.person?.crd;
        if (src.ind_source_id) {
          scheduledAny = true;
          await limiter.schedule(() => doIndividual(src.ind_source_id));
        }
        // check for firm id in employment or firm fields
        const emps = src.ind_current_employments || [];
        for (const e of emps) {
          if (e && e.firm_id) {
            scheduledAny = true;
            await limiter.schedule(() => doFirm(e.firm_id));
          }
        }
        if (src.firm_id) {
          scheduledAny = true;
          await limiter.schedule(() => doFirm(src.firm_id));
        }
      }
    }

    // Fallback to SEC adviserinfo search if BrokerCheck returned nothing useful
    if (!scheduledAny) {
      await limiter.schedule(() => doSecSearch(name, i));
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
