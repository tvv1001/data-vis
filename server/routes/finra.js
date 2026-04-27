/**
 * GET  /api/finra/graph          – return the cached finra-graph.json
 * POST /api/finra/run-scraper    – spawn finraScraper.py and return progress
 * GET  /api/finra/individual/:crd – proxy a single individual CRD detail record
 * GET  /api/finra/firm/:id       – proxy a firm detail record (includes Form BD / directOwners)
 */

import { Router } from "express";
import {
  readFile,
  writeFile,
  unlink,
  access,
  constants,
} from "node:fs/promises";
import { createReadStream } from "node:fs";
import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import os from "node:os";
import { logger } from "../utils/logger.js";
import {
  mergedIndividual,
  mergedFirm,
} from "../services/dataMerge/mergeFinraSec.js";
import { cachedFetch } from "../services/finraCache.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = path.resolve(__dirname, "..", "data");
const GRAPH_FILE = path.join(DATA_DIR, "national", "finra-graph.json");
const SEEDS_FILE = path.join(DATA_DIR, "national", "finra-seeds.json");
const SCRAPER = path.resolve(
  __dirname,
  "..",
  "services/crawler/finraScraper.py",
);
const ANTI_SCRAPER = path.resolve(
  __dirname,
  "..",
  "services/crawler/anti_bot_scraper.py",
);

export const finraRouter = Router();

// ---------------------------------------------------------------------------
// GET /api/finra/graph
// ---------------------------------------------------------------------------
finraRouter.get("/graph", async (_req, res) => {
  try {
    await access(GRAPH_FILE, constants.R_OK);
  } catch {
    // Instead of hard failing when the graph file is missing, return an
    // empty graph so the frontend can still boot and use live searches.
    return res.json({
      nodes: [],
      links: [],
      meta: {
        sourceLabel: "(no local graph)",
        generated: new Date().toISOString(),
        totalIndividuals: 0,
        totalFirms: 0,
        totalLinks: 0,
      },
    });
  }
  try {
    const raw = await readFile(GRAPH_FILE, "utf-8");
    res.setHeader("Content-Type", "application/json");
    res.send(raw);
  } catch (err) {
    logger.error("finra /graph read error", { error: err.message });
    res.status(500).json({ error: "Failed to read graph file." });
  }
});

// ---------------------------------------------------------------------------
// GET /api/finra/seeds
// ---------------------------------------------------------------------------
finraRouter.get("/seeds", async (_req, res) => {
  try {
    const raw = await readFile(SEEDS_FILE, "utf-8");
    res.json(JSON.parse(raw));
  } catch {
    res.json([]);
  }
});

// ---------------------------------------------------------------------------
// PUT /api/finra/seeds  –  replace the seeds list
// Body: { "seeds": ["Name One", "Name Two"] }
// ---------------------------------------------------------------------------
finraRouter.put("/seeds", async (req, res) => {
  const { seeds } = req.body;
  if (!Array.isArray(seeds) || seeds.some((s) => typeof s !== "string")) {
    return res.status(400).json({ error: "Body must be { seeds: string[] }" });
  }
  const { writeFile } = await import("node:fs/promises");
  await writeFile(SEEDS_FILE, JSON.stringify(seeds, null, 2), "utf-8");
  res.json({ ok: true, count: seeds.length });
});

// ---------------------------------------------------------------------------
// POST /api/finra/run-scraper  –  scrape only new seeds, then merge into graph
// ---------------------------------------------------------------------------
finraRouter.post("/run-scraper", async (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");

  const send = (type, data) =>
    res.write(`data: ${JSON.stringify({ type, data })}\n\n`);

  // ── Compute new seeds ───────────────────────────────────────────────────
  let allSeeds = [];
  try {
    allSeeds = JSON.parse(await readFile(SEEDS_FILE, "utf-8"));
  } catch {
    send("stderr", "Could not read seeds file.\n");
    send("done", { exitCode: 1 });
    return res.end();
  }

  let alreadyScraped = [];
  try {
    const graph = JSON.parse(await readFile(GRAPH_FILE, "utf-8"));
    alreadyScraped = Array.isArray(graph?.meta?.seedNames)
      ? graph.meta.seedNames
      : [];
  } catch {
    // No graph yet — scrape everything
  }

  const newSeeds = allSeeds.filter((s) => !alreadyScraped.includes(s));

  if (newSeeds.length === 0) {
    send(
      "stdout",
      "Nothing new to scrape — all seeds have already been processed.\n",
    );
    send("done", { exitCode: 0 });
    return res.end();
  }

  // Limit to a batch so each run is fast; remainder scraped on next run
  const BATCH = 10;
  const batch = newSeeds.slice(0, BATCH);
  const remaining = newSeeds.length - batch.length;

  send(
    "stdout",
    `Scraping ${batch.length} new seed(s) (${remaining} more pending after this batch):\n${batch.join(", ")}\n\n`,
  );

  // ── Write temp seeds file ───────────────────────────────────────────────
  const tmpSeeds = path.join(os.tmpdir(), `finra-seeds-${Date.now()}.json`);
  const tmpOut = path.join(os.tmpdir(), `finra-out-${Date.now()}.json`);
  await writeFile(tmpSeeds, JSON.stringify(batch, null, 2), "utf-8");

  // ── Spawn scraper ───────────────────────────────────────────────────────
  const python = process.env.PYTHON_BIN || "python3";
  const child = spawn(python, [SCRAPER, "--seeds", tmpSeeds, "--out", tmpOut], {
    env: { ...process.env },
  });

  child.stdout.on("data", (chunk) => send("stdout", chunk.toString()));
  child.stderr.on("data", (chunk) => send("stderr", chunk.toString()));

  child.on("close", async (code) => {
    if (code === 0) {
      try {
        // ── Merge partial graph into existing graph ────────────────────
        const partial = JSON.parse(await readFile(tmpOut, "utf-8"));
        let base = { nodes: [], links: [], meta: { seedNames: [] } };
        try {
          base = JSON.parse(await readFile(GRAPH_FILE, "utf-8"));
        } catch {
          /* first run */
        }

        const nodeMap = new Map((base.nodes || []).map((n) => [n.id, n]));
        for (const n of partial.nodes || []) nodeMap.set(n.id, n);

        const linkKey = (l) => `${l.source}|${l.target}|${l.relationship}`;
        const linkMap = new Map((base.links || []).map((l) => [linkKey(l), l]));
        for (const l of partial.links || []) linkMap.set(linkKey(l), l);

        const allSeedNames = [
          ...new Set([
            ...(base.meta?.seedNames || []),
            ...(partial.meta?.seedNames || []),
          ]),
        ];

        const mergedNodes = [...nodeMap.values()];
        const individuals = mergedNodes.filter((n) => n.group === "individual");
        const firms = mergedNodes.filter((n) => n.group === "firm");
        const entities = mergedNodes.filter((n) => n.group === "entity");

        const merged = {
          nodes: mergedNodes,
          links: [...linkMap.values()],
          meta: {
            ...partial.meta,
            seedNames: allSeedNames,
            totalIndividuals: individuals.length,
            totalFirms: firms.length,
            totalEntities: entities.length,
            totalLinks: linkMap.size,
          },
        };

        await writeFile(GRAPH_FILE, JSON.stringify(merged, null, 2), "utf-8");
        send(
          "stdout",
          `\nMerge complete: ${individuals.length} individuals, ${firms.length} firms, ${linkMap.size} links.\n`,
        );
      } catch (mergeErr) {
        logger.error("finra merge error", { error: mergeErr.message });
        send("stderr", `Merge error: ${mergeErr.message}\n`);
      } finally {
        await unlink(tmpSeeds).catch(() => {});
        await unlink(tmpOut).catch(() => {});
      }
    } else {
      await unlink(tmpSeeds).catch(() => {});
      await unlink(tmpOut).catch(() => {});
    }
    send("done", { exitCode: code });
    res.end();
  });

  child.on("error", async (err) => {
    logger.error("finra scraper spawn error", { error: err.message });
    send("error", err.message);
    await unlink(tmpSeeds).catch(() => {});
    await unlink(tmpOut).catch(() => {});
    res.end();
  });

  req.on("close", () => child.kill());
});

// ---------------------------------------------------------------------------
// GET /api/finra/individual/:crd  –  proxy detail from FINRA API
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// GET /api/finra/search  –  proxy search queries to FINRA BrokerCheck
// ---------------------------------------------------------------------------
finraRouter.get("/search", async (req, res) => {
  let axios;
  try {
    ({ default: axios } = await import("axios"));
  } catch {
    return res.status(500).json({ error: "axios not available" });
  }

  try {
    // Determine whether to call the individual or firm search endpoint.
    const q = { ...req.query };
    // If `firm` param present, prefer the firm endpoint.
    const useFirm = q.firm || q.firmId || q.firm_id;
    const baseUrl = useFirm
      ? "https://api.brokercheck.finra.org/search/firm"
      : "https://api.brokercheck.finra.org/search/individual";

    // Proxy the request to FINRA, forwarding query params.
    // Build a stable cache key including sorted query params
    const sortedKeys = Object.keys(q).sort();
    const keyParts = [baseUrl];
    for (const k of sortedKeys) keyParts.push(`${k}=${q[k]}`);
    const cacheKey = `finra:search:${Buffer.from(keyParts.join("|")).toString("base64")}`;

    const data = await cachedFetch(cacheKey, 60 * 60 * 24, async () => {
      const r = await axios.get(baseUrl, {
        params: q,
        headers: {
          "User-Agent": "Mozilla/5.0 (compatible; research-tool/1.0)",
          Accept: "application/json",
        },
        timeout: 20000,
      });
      return r.data;
    });

    res.setHeader("Content-Type", "application/json");
    res.send(JSON.stringify(data));
  } catch (err) {
    logger.error("finra search proxy error", {
      error: err.message,
      query: req.query,
    });
    res.status(502).json({ error: "Failed to proxy search to FINRA." });
  }
});

// ---------------------------------------------------------------------------
// GET /api/finra/sec-search  –  proxy SEC adviserinfo search (avoid CORS)
// ---------------------------------------------------------------------------
finraRouter.get("/sec-search", async (req, res) => {
  let axios;
  try {
    ({ default: axios } = await import("axios"));
  } catch {
    return res.status(500).json({ error: "axios not available" });
  }

  try {
    const q = { ...req.query };
    const baseUrl = "https://api.adviserinfo.sec.gov/search/individual";

    // Build cache key stable across param order
    const sortedKeys = Object.keys(q).sort();
    const keyParts = [baseUrl];
    for (const k of sortedKeys) keyParts.push(`${k}=${q[k]}`);
    const cacheKey = `sec:search:${Buffer.from(keyParts.join("|")).toString("base64")}`;

    const data = await cachedFetch(cacheKey, 60 * 60 * 24, async () => {
      const r = await axios.get(baseUrl, {
        params: q,
        headers: {
          "User-Agent": "Mozilla/5.0 (compatible; research-tool/1.0)",
          Accept: "application/json",
        },
        timeout: 20000,
      });
      return r.data;
    });

    res.setHeader("Content-Type", "application/json");
    res.send(JSON.stringify(data));
  } catch (err) {
    logger.error("sec search proxy error", {
      error: err.message,
      query: req.query,
    });
    // Attempt anti-bot scraper fallback when SEC blocks or network fails
    try {
      const q = { ...req.query };
      const baseUrl = "https://api.adviserinfo.sec.gov/search/individual";
      const url =
        baseUrl +
        "?" +
        Object.keys(q)
          .sort()
          .map((k) => `${k}=${encodeURIComponent(q[k])}`)
          .join("&");
      const fallback = await runAntiBot(url);
      if (fallback && fallback.ok) {
        // If scraper returned a peoplePayload, try to expose it under a
        // compatible `response.docs` shape so the frontend can reuse parsing.
        let body = {};
        if (fallback.peoplePayload) {
          try {
            const ppl = JSON.parse(fallback.peoplePayload);
            body = { response: { docs: ppl } };
          } catch {
            body = {
              html: fallback.html || "",
              signals: fallback.signals || {},
            };
          }
        } else {
          body = { html: fallback.html || "", signals: fallback.signals || {} };
        }
        res.setHeader("Content-Type", "application/json");
        return res.send(JSON.stringify(body));
      }
    } catch (fbErr) {
      logger.warn("sec search anti-bot fallback failed", {
        error: fbErr?.message,
      });
    }

    res.status(502).json({ error: "Failed to proxy SEC adviserinfo search." });
  }
});

/**
 * Run the anti-bot scraper Python helper against `url` and return parsed JSON.
 * Returns the parsed JSON object printed to stdout by the script.
 */
function runAntiBot(url, timeoutMs = 30000) {
  return new Promise((resolve, reject) => {
    const python = process.env.PYTHON_BIN || "python3";
    const child = spawn(python, [ANTI_SCRAPER, url], {
      env: { ...process.env },
    });
    let out = "";
    let err = "";
    const killTimer = setTimeout(() => {
      child.kill();
    }, timeoutMs);

    child.stdout.on("data", (c) => (out += c.toString()));
    child.stderr.on("data", (c) => (err += c.toString()));

    child.on("close", (code) => {
      clearTimeout(killTimer);
      if (!out)
        return reject(new Error(`scraper produced no output: ${err || code}`));
      try {
        const parsed = JSON.parse(out);
        resolve(parsed);
      } catch (e) {
        return reject(new Error(`failed to parse scraper JSON: ${e.message}`));
      }
    });

    child.on("error", (e) => {
      clearTimeout(killTimer);
      reject(e);
    });
  });
}

finraRouter.get("/individual/:crd", async (req, res) => {
  const { crd } = req.params;
  if (!/^\d{1,10}$/.test(crd)) {
    return res.status(400).json({ error: "Invalid CRD number." });
  }

  let axios;
  try {
    ({ default: axios } = await import("axios"));
  } catch {
    return res.status(500).json({ error: "axios not available" });
  }

  try {
    const url = `https://api.brokercheck.finra.org/search/individual/${encodeURIComponent(crd)}`;
    const cacheKey = `finra:individual:${crd}`;

    const data = await cachedFetch(cacheKey, 60 * 60 * 24 * 7, async () => {
      const r = await axios.get(url, {
        headers: {
          "User-Agent": "Mozilla/5.0 (compatible; research-tool/1.0)",
          Accept: "application/json",
        },
        timeout: 15000,
      });
      return r.data;
    });

    const hits = data?.hits?.hits ?? [];
    if (!hits.length) return res.status(404).json({ error: "CRD not found." });

    const raw = hits[0]?._source?.content;
    const detail = typeof raw === "string" ? JSON.parse(raw) : raw;
    res.json(detail);
  } catch (err) {
    logger.error("finra proxy error", { crd, error: err.message });
    res.status(502).json({ error: "Failed to fetch from FINRA." });
  }
});

// ---------------------------------------------------------------------------
// GET /api/finra/merged/individual/:crd  –  return local merged FINRA+SEC record
// ---------------------------------------------------------------------------
finraRouter.get("/merged/individual/:crd", async (req, res) => {
  const { crd } = req.params;
  if (!/^[0-9]+$/.test(crd))
    return res.status(400).json({ error: "Invalid CRD" });
  try {
    const data = await mergedIndividual(crd);
    if (!data.found)
      return res.status(404).json({ error: "Merged record not found" });
    res.json(data);
  } catch (err) {
    logger.error("merged individual error", { crd, error: err?.message });
    res.status(500).json({ error: "Failed to compute merged record" });
  }
});

// ---------------------------------------------------------------------------
// GET /api/finra/firm/:id  –  proxy Form BD detail (incl. directOwners)
// ---------------------------------------------------------------------------
finraRouter.get("/firm/:id", async (req, res) => {
  const { id } = req.params;
  if (!/^\d{1,10}$/.test(id)) {
    return res.status(400).json({ error: "Invalid firm ID." });
  }

  let axios;
  try {
    ({ default: axios } = await import("axios"));
  } catch {
    return res.status(500).json({ error: "axios not available" });
  }

  try {
    const url = `https://api.brokercheck.finra.org/search/firm/${encodeURIComponent(id)}`;
    const cacheKey = `finra:firm:${id}`;

    const data = await cachedFetch(cacheKey, 60 * 60 * 24 * 7, async () => {
      const r = await axios.get(url, {
        headers: {
          "User-Agent": "Mozilla/5.0 (compatible; research-tool/1.0)",
          Accept: "application/json",
        },
        timeout: 15000,
      });
      return r.data;
    });

    const hits = data?.hits?.hits ?? [];
    if (!hits.length) return res.status(404).json({ error: "Firm not found." });

    const raw = hits[0]?._source?.content;
    const detail = typeof raw === "string" ? JSON.parse(raw) : raw;
    res.json(detail);
  } catch (err) {
    logger.error("finra firm proxy error", { id, error: err.message });
    res.status(502).json({ error: "Failed to fetch from FINRA." });
  }
});

// ---------------------------------------------------------------------------
// GET /api/finra/merged/firm/:id  –  return local merged FINRA+SEC firm evidence
// ---------------------------------------------------------------------------
finraRouter.get("/merged/firm/:id", async (req, res) => {
  const { id } = req.params;
  if (!/^[0-9]+$/.test(id))
    return res.status(400).json({ error: "Invalid firm id" });
  try {
    const data = await mergedFirm(id);
    if (!data.found)
      return res.status(404).json({ error: "Merged firm not found" });
    res.json(data);
  } catch (err) {
    logger.error("merged firm error", { id, error: err?.message });
    res.status(500).json({ error: "Failed to compute merged firm" });
  }
});
