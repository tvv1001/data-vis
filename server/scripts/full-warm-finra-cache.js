#!/usr/bin/env node
import fs from "fs/promises";
import path from "path";
import Bottleneck from "bottleneck";
import axios from "axios";
import meow from "meow";

const cli = meow(
  `
Usage
  $ node scripts/full-warm-finra-cache.js [options]

Options
  --host           server host (default http://localhost:3002)
  --rate, -r       requests per second (default 5)
  --concurrency, -c concurrent requests (default 5)
  --input, -i      path to a JSON file with array of seed names
  --detail, -d     also fetch individual/firm detail records (default false)
  --progress, -p   progress file path (default .warm-progress.json)
  --resume         resume from progress file if present
`,
  {
    importMeta: import.meta,
    flags: {
      host: { type: "string", default: "http://localhost:3002" },
      rate: { type: "number", alias: "r", default: 5 },
      concurrency: { type: "number", alias: "c", default: 5 },
      input: { type: "string", alias: "i" },
      detail: { type: "boolean", alias: "d", default: false },
      progress: { type: "string", alias: "p", default: ".warm-progress.json" },
      resume: { type: "boolean", default: false },
    },
  },
);

const { host, rate, concurrency, input, detail, progress, resume } = cli.flags;

const limiter = new Bottleneck({
  minTime: Math.ceil(1000 / Math.max(1, rate)),
  maxConcurrent: Math.max(1, concurrency),
});

async function loadSeeds() {
  if (input) {
    const raw = await fs.readFile(path.resolve(input), "utf-8");
    const arr = JSON.parse(raw);
    if (!Array.isArray(arr)) throw new Error("Input file must be a JSON array");
    return arr;
  }

  // Try server seeds endpoint
  const url = `${host}/api/finra/seeds`;
  const r = await axios.get(url, { timeout: 15000 });
  if (!Array.isArray(r.data))
    throw new Error("Server seeds endpoint did not return an array");
  return r.data;
}

async function saveProgress(obj) {
  await fs.writeFile(progress, JSON.stringify(obj, null, 2), "utf-8");
}

async function loadProgress() {
  try {
    const raw = await fs.readFile(progress, "utf-8");
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

async function fetchSearch(q) {
  const url = `${host}/api/finra/search`;
  const resp = await axios.get(url, {
    params: { query: q, wt: "json" },
    timeout: 20000,
  });
  return resp.data;
}

async function fetchIndividual(crd) {
  const url = `${host}/api/finra/individual/${encodeURIComponent(crd)}`;
  return axios
    .get(url, { timeout: 15000 })
    .then((r) => r.data)
    .catch(() => null);
}

async function fetchFirm(id) {
  const url = `${host}/api/finra/firm/${encodeURIComponent(id)}`;
  return axios
    .get(url, { timeout: 15000 })
    .then((r) => r.data)
    .catch(() => null);
}

async function main() {
  console.log(`Loading seeds (host=${host})`);
  const seeds = await loadSeeds();
  if (!seeds || seeds.length === 0) {
    console.error(
      "No seeds found. Provide --input or populate server /api/finra/seeds.",
    );
    process.exit(1);
  }
  console.log(`Found ${seeds.length} seeds.`);

  let pos = 0;
  if (resume) {
    const p = await loadProgress();
    if (p && typeof p.pos === "number") pos = p.pos;
  }

  console.log(`Starting at index ${pos}`);

  for (let i = pos; i < seeds.length; i++) {
    const name = seeds[i];
    const job = async () => {
      try {
        const s = await fetchSearch(name);
        // optionally fetch details
        if (detail && Array.isArray(s?.hits?.hits)) {
          for (const h of s.hits.hits) {
            const src = h?._source?.content;
            let parsed = src;
            if (typeof src === "string") {
              try {
                parsed = JSON.parse(src);
              } catch {}
            }
            if (parsed?.crd) await fetchIndividual(parsed.crd);
            if (parsed?.firmId) await fetchFirm(parsed.firmId);
          }
        }
        return { ok: true };
      } catch (e) {
        return { ok: false, error: e?.message || String(e) };
      }
    };

    const res = await limiter.schedule(job);
    if (!res.ok) {
      console.warn(`Seed[${i}] ${name} failed: ${res.error}`);
    }

    // update progress file
    await saveProgress({
      pos: i + 1,
      last: name,
      timestamp: new Date().toISOString(),
    });
    if ((i + 1) % 100 === 0) console.log(`Progress: ${i + 1}/${seeds.length}`);
  }

  console.log("Warm complete.");
  await saveProgress({
    pos: seeds.length,
    last: null,
    finished: true,
    timestamp: new Date().toISOString(),
  });
}

main().catch((e) => {
  console.error(e.stack || e);
  process.exit(1);
});
