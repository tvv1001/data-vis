#!/usr/bin/env node
// Download FINRA and SEC records for all nodes in server/data/national/finra-graph.json
// Saves FINRA individual detail to server/data/national/finra-individual-<crd>.json
// Saves FINRA firm detail to server/data/national/finra-firm-<id>.json
// Saves SEC search results to server/data/national/sec-search-<safe-name>.json

import fs from "node:fs/promises";
import path from "node:path";
// Node 18+ provides global `fetch`; no external dependency required

const ROOT = path.resolve(process.cwd());
const DATA_DIR = path.join(ROOT, "server", "data", "national");
const GRAPH_FILE = path.join(DATA_DIR, "finra-graph.json");

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function safeName(name) {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 120);
}
async function exists(path) {
  try {
    await fs.access(path);
    return true;
  } catch {
    return false;
  }
}

// Fetch with retries + exponential backoff for transient errors (429, 5xx)
async function fetchWithRetry(url, opts = {}, attempts = 4) {
  const timeoutMs = opts.timeout || 20000;
  for (let i = 0; i < attempts; i++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(url, { signal: controller.signal });
      clearTimeout(timer);
      if (res.status === 429 || (res.status >= 500 && res.status < 600)) {
        const wait = Math.min(30000, 500 * Math.pow(2, i));
        await sleep(wait + Math.floor(Math.random() * 200));
        continue;
      }
      return res;
    } catch (err) {
      clearTimeout(timer);
      if (i === attempts - 1) throw err;
      const wait = Math.min(30000, 500 * Math.pow(2, i));
      await sleep(wait + Math.floor(Math.random() * 200));
    }
  }
  throw new Error("fetchWithRetry: exhausted attempts");
}

async function main() {
  console.log("Reading graph:", GRAPH_FILE);
  const raw = await fs.readFile(GRAPH_FILE, "utf8");
  const graph = JSON.parse(raw);
  const nodes = graph.nodes || [];
  console.log(`Found ${nodes.length} nodes`);

  // Ensure output directory exists
  await fs.mkdir(DATA_DIR, { recursive: true });

  // Progress checkpoint so the script can resume after interruptions
  const PROGRESS_FILE = path.join(DATA_DIR, "download-progress.json");
  let progress = { processed: [] };
  try {
    const pRaw = await fs.readFile(PROGRESS_FILE, "utf8");
    progress = JSON.parse(pRaw);
  } catch {
    // no progress yet
  }
  const processedSet = new Set(
    Array.isArray(progress.processed) ? progress.processed : [],
  );

  const BASE = "http://localhost:3001";
  let count = 0;
  // How often to persist progress. Can be overridden via env PROGRESS_SAVE_EVERY
  const SAVE_EVERY = parseInt(process.env.PROGRESS_SAVE_EVERY || "1", 10);
  let writtenSinceSave = 0;
  let consecutive429 = 0;

  async function saveProgress() {
    try {
      progress.processed = Array.from(processedSet);
      await fs.writeFile(
        PROGRESS_FILE,
        JSON.stringify(progress, null, 2),
        "utf8",
      );
      writtenSinceSave = 0;
    } catch (e) {
      // non-fatal
    }
  }

  // Ensure progress is saved on termination
  process.on("SIGINT", async () => {
    console.log("\nReceived SIGINT — saving progress...");
    await saveProgress();
    process.exit(0);
  });
  process.on("SIGTERM", async () => {
    console.log("\nReceived SIGTERM — saving progress...");
    await saveProgress();
    process.exit(0);
  });
  for (const n of nodes) {
    if (processedSet.has(n.id)) {
      count++;
      process.stdout.write(
        `(${count}/${nodes.length}) ${n.id} ${n.label || ""}… SKIP\n`,
      );
      continue;
    }
    count++;
    const label = n.label || n.name || "";
    process.stdout.write(`(${count}/${nodes.length}) ${n.id} ${label}… `);
    try {
      if (n.group === "individual") {
        // FINRA individual by CRD
        const crd =
          n.crd || (n.id && n.id.match(/(\d+)/) && n.id.match(/(\d+)/)[1]);
        if (crd) {
          try {
            const url = `${BASE}/api/finra/individual/${encodeURIComponent(crd)}`;
            const out = path.join(DATA_DIR, `finra-individual-${crd}.json`);
            if (await exists(out)) {
              process.stdout.write("FINRA(crd) exists ");
              consecutive429 = 0;
            } else {
              const r = await fetchWithRetry(url, { timeout: 20000 }, 5);
              if (r.status === 429) consecutive429++;
              else consecutive429 = 0;
              if (r.ok) {
                const data = await r.json();
                await fs.writeFile(out, JSON.stringify(data, null, 2), "utf8");
                process.stdout.write(`FINRA(crd) saved `);
              } else {
                process.stdout.write(`FINRA(crd) ${r.status} `);
              }
            }
          } catch (e) {
            process.stdout.write("FINRA(crd) err ");
          }
        }

        // SEC adviserinfo search by name (proxy)
        if (label) {
          try {
            const params = new URLSearchParams({
              query: label,
              filter:
                "active=true,prev=true,bar=true,broker=true,ia=true,brokeria=true",
              includePrevious: "true",
              hl: "true",
              nrows: "12",
              start: "0",
              r: "25",
              sort: "score desc",
              wt: "json",
            });
            const url = `${BASE}/api/finra/sec-search?${params.toString()}`;
            const out = path.join(
              DATA_DIR,
              `sec-search-${safeName(label)}.json`,
            );
            if (await exists(out)) {
              process.stdout.write("SEC exists ");
              consecutive429 = 0;
            } else {
              const r = await fetchWithRetry(url, { timeout: 25000 }, 5);
              if (r.status === 429) consecutive429++;
              else consecutive429 = 0;
              if (r.ok) {
                const data = await r.json();
                await fs.writeFile(out, JSON.stringify(data, null, 2), "utf8");
                process.stdout.write("SEC saved ");
              } else {
                process.stdout.write(`SEC ${r.status} `);
              }
            }
          } catch (e) {
            process.stdout.write("SEC err ");
          }
        }
      } else if (n.group === "firm") {
        // Try to find numeric firm id in known fields
        let firmId = null;
        if (n.firm_source_id) firmId = String(n.firm_source_id);
        if (!firmId && n.firm_id) firmId = String(n.firm_id);
        if (!firmId && n.id && /firm_(\d+)/.test(n.id))
          firmId = n.id.match(/firm_(\d+)/)[1];
        if (!firmId && /^(\d+)$/.test(n.id)) firmId = n.id;

        if (firmId) {
          try {
            const out = path.join(DATA_DIR, `finra-firm-${firmId}.json`);
            if (await exists(out)) {
              process.stdout.write("FINRA(firm) exists ");
              consecutive429 = 0;
            } else {
              const url = `${BASE}/api/finra/firm/${encodeURIComponent(firmId)}`;
              const r = await fetchWithRetry(url, { timeout: 20000 }, 5);
              if (r.status === 429) consecutive429++;
              else consecutive429 = 0;
              if (r.ok) {
                const data = await r.json();
                await fs.writeFile(out, JSON.stringify(data, null, 2), "utf8");
                process.stdout.write(`FINRA(firm) saved `);
              } else {
                process.stdout.write(`FINRA(firm) ${r.status} `);
              }
            }
          } catch (e) {
            process.stdout.write("FINRA(firm) err ");
          }
        } else {
          process.stdout.write("no-firm-id ");
        }

        // SEC lookup by firm name (best-effort)
        if (label) {
          try {
            const params = new URLSearchParams({
              query: label,
              includePrevious: "true",
              hl: "true",
              nrows: "12",
              start: "0",
              r: "25",
              sort: "score desc",
              wt: "json",
            });
            const url = `${BASE}/api/finra/sec-search?${params.toString()}`; // proxy will forward
            const r = await fetchWithRetry(url, { timeout: 25000 }, 5);
            if (r.status === 429) consecutive429++;
            else consecutive429 = 0;
            if (r.ok) {
              const data = await r.json();
              const out = path.join(
                DATA_DIR,
                `sec-search-${safeName(label)}.json`,
              );
              await fs.writeFile(out, JSON.stringify(data, null, 2), "utf8");
              process.stdout.write("SEC saved ");
            } else {
              process.stdout.write(`SEC ${r.status} `);
            }
          } catch (e) {
            process.stdout.write("SEC err ");
          }
        }
      } else {
        process.stdout.write("skip ");
      }
    } catch (err) {
      process.stdout.write("err ");
    }

    process.stdout.write("\n");
    // Respectful delay to avoid hitting rate limits; increase when many 429s occur
    const extra = Math.min(60000, consecutive429 * 1000);
    await sleep(200 + extra);

    // Mark node processed and persist progress periodically
    try {
      processedSet.add(n.id);
      writtenSinceSave++;
      if (writtenSinceSave >= SAVE_EVERY) {
        await saveProgress();
      }
    } catch (e) {
      // non-fatal
    }
  }
  // Final save before exit
  await saveProgress();
  console.log("Done");
}

main().catch((e) => {
  console.error(e);
  process.exit(2);
});
