import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BASE = path.resolve(__dirname, "..", "..", "data", "national");
const FINRA_DIR = path.join(BASE, "brokercheck.finra.org");
const SEC_DIR = path.join(BASE, "adviserinfo.sec.gov");

let _loaded = false;
let finraIndividuals = new Map();
let secIndividuals = new Map();
let finraGraph = null;

async function _loadFinraFiles() {
  try {
    const files = await readdir(FINRA_DIR);
    for (const f of files) {
      if (!f.endsWith(".json")) continue;
      // process known file prefixes that may contain individual docs
      if (
        !f.startsWith("dl_") &&
        !f.startsWith("query_") &&
        !f.startsWith("individual_") &&
        !f.startsWith("summary_")
      )
        continue;
      try {
        const raw = await readFile(path.join(FINRA_DIR, f), "utf-8");
        const json = JSON.parse(raw);
        const hits = json?.hits?.hits || [];
        for (const h of hits) {
          const src = h._source || {};
          // try to derive an individual id from multiple possible places
          let id = src.ind_source_id || src.person?.crd || src.firm_id;
          // sometimes individual detail content is embedded as a string in src.content
          if (src.content) {
            try {
              const parsed =
                typeof src.content === "string"
                  ? JSON.parse(src.content)
                  : src.content;
              // expose parsed employment lists on top-level fields for downstream code
              if (parsed?.currentEmployments)
                src.ind_current_employments = parsed.currentEmployments;
              if (parsed?.previousEmployments)
                src.ind_previous_employments = parsed.previousEmployments;
              if (!id)
                id =
                  parsed?.basicInformation?.individualId ||
                  parsed?.basicInformation?.crd ||
                  id;
            } catch (e) {
              // ignore parse errors
            }
          }
          if (!id) continue;
          if (src.ind_source_id)
            finraIndividuals.set(String(src.ind_source_id), src);
          else finraIndividuals.set(String(id), src);
        }
      } catch (e) {
        // ignore parse errors for individual files
      }
    }
  } catch (e) {
    // directory missing — leave map empty
  }
}

async function _loadSecFiles() {
  try {
    const files = await readdir(SEC_DIR);
    for (const f of files) {
      if (!f.endsWith(".json")) continue;
      try {
        const raw = await readFile(path.join(SEC_DIR, f), "utf-8");
        const json = JSON.parse(raw);
        const hits = json?.hits?.hits || [];
        for (const h of hits) {
          const src = h._source || {};
          // derive possible id and parse embedded content if present
          let id = src.ind_source_id || src.person?.crd || src.firm_id;
          if (src.content) {
            try {
              const parsed =
                typeof src.content === "string"
                  ? JSON.parse(src.content)
                  : src.content;
              if (parsed?.currentEmployments)
                src.ind_current_employments = parsed.currentEmployments;
              if (parsed?.previousEmployments)
                src.ind_previous_employments = parsed.previousEmployments;
              if (!id)
                id =
                  parsed?.basicInformation?.individualId ||
                  parsed?.basicInformation?.crd ||
                  id;
            } catch (e) {
              // ignore parse errors
            }
          }
          if (src.ind_source_id)
            secIndividuals.set(String(src.ind_source_id), src);
          else if (id) secIndividuals.set(String(id), src);
        }
      } catch (e) {
        // ignore parse errors
      }
    }
  } catch (e) {
    // missing dir — ignore
  }
}

async function _loadGraph() {
  try {
    const gRaw = await readFile(path.join(BASE, "finra-graph.json"), "utf-8");
    finraGraph = JSON.parse(gRaw);
  } catch (e) {
    finraGraph = null;
  }
}

async function ensureLoaded() {
  if (_loaded) return;
  _loaded = true;
  await Promise.all([_loadFinraFiles(), _loadSecFiles(), _loadGraph()]);
}

function _pickIndividualFields(src) {
  if (!src) return null;
  // if detailed content is embedded as a string, parse it for employment lists
  let parsed = null;
  if (src.content) {
    try {
      parsed =
        typeof src.content === "string" ? JSON.parse(src.content) : src.content;
    } catch (e) {
      parsed = null;
    }
  }
  return {
    ind_source_id: src.ind_source_id,
    firstName: src.ind_firstname || src.firstName || null,
    middleName: src.ind_middlename || src.middleName || null,
    lastName: src.ind_lastname || src.lastName || null,
    otherNames: src.ind_other_names || src.otherNames || [],
    bcScope: src.ind_bc_scope || src.bcScope || null,
    iaScope: src.ind_ia_scope || src.iaScope || null,
    disclosureFlag: src.ind_bc_disclosure_fl || src.disclosureFlag || null,
    industryCalDate:
      src.ind_industry_cal_date ||
      src.industryCalDate ||
      src.ind_industry_cal_date_iapd ||
      null,
    currentEmployments:
      src.ind_current_employments ||
      src.ind_ia_current_employments ||
      (parsed ? parsed.currentEmployments || [] : []),
    previousEmployments:
      src.ind_previous_employments ||
      src.ind_ia_previous_employments ||
      (parsed ? parsed.previousEmployments || [] : []),
  };
}

function _eq(a, b) {
  return JSON.stringify(a) === JSON.stringify(b);
}

function computeDiffs(finra, sec) {
  const f = _pickIndividualFields(finra);
  const s = _pickIndividualFields(sec);
  const diffs = {};
  const keys = new Set([
    ...(f ? Object.keys(f) : []),
    ...(s ? Object.keys(s) : []),
  ]);
  for (const k of keys) {
    const fv = f ? f[k] : undefined;
    const sv = s ? s[k] : undefined;
    diffs[k] = { finra: fv ?? null, sec: sv ?? null, equal: _eq(fv, sv) };
  }
  return { finra: f, sec: s, diffs };
}

export async function mergedIndividual(crd) {
  await ensureLoaded();
  const id = String(crd);
  const finra = finraIndividuals.get(id) || null;
  const sec = secIndividuals.get(id) || null;
  const computed = computeDiffs(finra, sec);
  return {
    crd: id,
    found: !!(finra || sec),
    sources: { finra: finra || null, sec: sec || null },
    merged: computed,
  };
}

export async function mergedFirm(firmId) {
  await ensureLoaded();
  const id = String(firmId);
  // try to find firm node in finra graph
  let firmNode = null;
  if (finraGraph && Array.isArray(finraGraph.nodes)) {
    firmNode = finraGraph.nodes.find(
      (n) => n.group === "firm" && String(n.firmId) === id,
    );
  }

  // Find finra evidence by scanning finraIndividuals for employments
  const evidence = [];
  for (const [personKey, v] of finraIndividuals.entries()) {
    const emps = [
      ...(v.ind_current_employments || []),
      ...(v.ind_previous_employments || []),
      ...(v.ind_ia_current_employments || []),
      ...(v.ind_ia_previous_employments || []),
    ];
    for (const e of emps) {
      const fid = e?.firm_id || e?.firmId || null;
      if (!fid) continue;
      if (String(fid) === id) {
        evidence.push({ personId: personKey, employment: e });
      }
    }
  }

  return {
    firmId: id,
    found: !!(firmNode || evidence.length),
    finraNode: firmNode || null,
    evidence,
  };
}

export default { mergedIndividual, mergedFirm };
