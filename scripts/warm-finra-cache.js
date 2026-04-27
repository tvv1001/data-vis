#!/usr/bin/env node
import Bottleneck from "bottleneck";
import axios from "axios";
import meow from "meow";
import { cachedFetch } from "../server/services/finraCache.js";

const cli = meow(
  `
Usage
  $ node scripts/warm-finra-cache.js [options]

Options
  --rate, -r        requests per second (default 5)
  --concurrency, -c concurrent jobs (default 5)
  --sample, -n      number of sample tasks to run when no input provided (default 3)
  --host            server host (default http://localhost:3001)
`,
  {
    importMeta: import.meta,
    flags: {
      rate: { type: "number", alias: "r", default: 5 },
      concurrency: { type: "number", alias: "c", default: 5 },
      sample: { type: "number", alias: "n", default: 3 },
      host: { type: "string", default: "http://localhost:3001" },
    },
  },
);

const { rate, concurrency, sample, host } = cli.flags;

const limiter = new Bottleneck({
  minTime: Math.ceil(1000 / Math.max(1, rate)),
  maxConcurrent: Math.max(1, concurrency),
});

// Small built-in sample tasks using user-provided example queries
const tasks = [
  { type: "search", q: "Jennifer Janet David" },
  { type: "search", q: "Jennifer Brooks Brooks" },
  { type: "search", q: "David Smith" },
];

async function runTask(t) {
  try {
    if (t.type === "search") {
      const url = `${host}/api/finra/search`;
      const resp = await axios.get(url, {
        params: { query: t.q, hl: true, wt: "json" },
        timeout: 20000,
      });
      return { ok: true, url, status: resp.status };
    }

    if (t.type === "firm") {
      const url = `${host}/api/finra/firm/${encodeURIComponent(t.id)}`;
      const resp = await axios.get(url, { timeout: 15000 });
      return { ok: true, url, status: resp.status };
    }

    if (t.type === "individual") {
      const url = `${host}/api/finra/individual/${encodeURIComponent(t.crd)}`;
      const resp = await axios.get(url, { timeout: 15000 });
      return { ok: true, url, status: resp.status };
    }
  } catch (err) {
    return { ok: false, error: err?.message || String(err) };
  }
}

async function main() {
  console.log(
    `Warming cache (rate=${rate}/s, concurrency=${concurrency}) against ${host}`,
  );

  const run = async (t) => limiter.schedule(() => runTask(t));

  const toRun = tasks.slice(0, Math.max(1, Math.min(sample, tasks.length)));

  const results = [];
  for (const t of toRun) {
    // schedule twice to check cache hit on second run
    results.push(await run(t));
    results.push(await run(t));
  }

  // If using direct caching via finraCache, also show a retrieval from cache
  try {
    for (const t of toRun) {
      if (t.type === "search") {
        const baseUrl = "https://api.brokercheck.finra.org/search/individual";
        const keyParts = [baseUrl, `query=${t.q}`];
        const cacheKey = `finra:search:${Buffer.from(keyParts.join("|")).toString("base64")}`;
        const cached = await cachedFetch(cacheKey, 60 * 60 * 24, async () => {
          const r = await axios.get(baseUrl, {
            params: { query: t.q, wt: "json" },
            timeout: 20000,
          });
          return r.data;
        });
        console.log(
          `Direct cachedFetch for query="${t.q}" -> ${cached ? "OK" : "MISS"}`,
        );
      }
    }
  } catch (e) {
    // non-fatal
  }

  console.log("Results:");
  for (const r of results) console.log(r);
  await limiter.stop({ dropWaitingJobs: false });
}

main().catch((e) => {
  console.error(e.stack || e);
  process.exit(1);
});
