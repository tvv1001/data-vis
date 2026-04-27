#!/usr/bin/env node
import fs from "node:fs/promises";
import path from "node:path";

const ROOT = path.resolve(path.dirname(process.argv[1]), "..");
const BASE = path.join(ROOT, "server", "data", "national");
const FINRA_DIR = path.join(BASE, "brokercheck.finra.org");
const SEC_DIR = path.join(BASE, "adviserinfo.sec.gov");
const OUT = path.join(BASE, "finra-graph.json");

function personId(crd) {
  return `person_${crd}`;
}
function firmId(firm) {
  return `firm_${firm}`;
}

async function safeReadJSON(file) {
  try {
    const txt = await fs.readFile(file, "utf8");
    return JSON.parse(txt);
  } catch (e) {
    return null;
  }
}

async function main() {
  const nodesById = {};
  const links = [];
  const seenCrds = new Set();

  // Collect files
  let finraFiles = [];
  try {
    finraFiles = await fs.readdir(FINRA_DIR);
  } catch (e) {
    finraFiles = [];
  }
  let secFiles = [];
  try {
    secFiles = await fs.readdir(SEC_DIR);
  } catch (e) {
    secFiles = [];
  }

  // Helper to ensure firm node
  function ensureFirmNode(fid, label, extra) {
    const id = firmId(fid);
    if (!nodesById[id]) {
      nodesById[id] = {
        id,
        firmId: String(fid),
        label: label || String(fid),
        group: "firm",
      };
      if (extra) Object.assign(nodesById[id], extra);
    }
    return nodesById[id];
  }

  // Helper to ensure person node
  function ensurePersonNode(crd, label, extra) {
    const id = personId(crd);
    if (!nodesById[id]) {
      nodesById[id] = {
        id,
        crd: String(crd),
        label: label || String(crd),
        group: "individual",
      };
      if (extra) Object.assign(nodesById[id], extra);
    }
    return nodesById[id];
  }

  // Parse summary files to collect CRDs and create minimal nodes
  for (const f of finraFiles) {
    if (!f.endsWith(".json")) continue;
    const filePath = path.join(FINRA_DIR, f);
    const j = await safeReadJSON(filePath);
    if (!j) continue;
    const hits = j?.hits?.hits || [];
    for (const h of hits) {
      const src = h._source || {};
      const crd = src.ind_source_id || src.person?.crd;
      if (crd) {
        seenCrds.add(String(crd));
        const label =
          [src.ind_firstname, src.ind_middlename, src.ind_lastname]
            .filter(Boolean)
            .join(" ") ||
          src.person?.name ||
          src.ind_name ||
          src.ind_full_name ||
          null;
        ensurePersonNode(crd, label, {
          firstName: src.ind_firstname || null,
          lastName: src.ind_lastname || null,
        });
      }
      // also detect firm ids mentioned at summary level
      const firmIdVal = src.firm_id;
      if (firmIdVal) ensureFirmNode(firmIdVal, src.firm_name || null);
    }
  }

  // Enrich from individual detail files (individual_{crd}.json)
  for (const f of finraFiles) {
    if (!f.startsWith("individual_") || !f.endsWith(".json")) continue;
    const filePath = path.join(FINRA_DIR, f);
    const j = await safeReadJSON(filePath);
    if (!j) continue;
    const hits = j?.hits?.hits || [];
    if (!hits.length) continue;
    const src = hits[0]._source || {};
    // detail content may be in ._source.content (string) or the whole doc
    let detail = null;
    if (src.content) {
      try {
        detail =
          typeof src.content === "string"
            ? JSON.parse(src.content)
            : src.content;
      } catch (e) {
        detail = src.content;
      }
    } else if (j && j.detail) {
      detail = j.detail;
    } else {
      // sometimes the file itself is the detail
      detail = j;
    }

    // derive CRD from several possible locations, preferring explicit fields
    const crd =
      src.ind_source_id ||
      src.person?.crd ||
      detail?.basicInformation?.individualId ||
      detail?.basicInformation?.crd;
    if (!crd) continue;
    seenCrds.add(String(crd));

    const first =
      detail?.basicInformation?.firstName || src.ind_firstname || null;
    const last = detail?.basicInformation?.lastName || src.ind_lastname || null;
    const label =
      [first, last].filter(Boolean).join(" ") ||
      src.ind_name ||
      nodesById[personId(crd)]?.label;
    ensurePersonNode(crd, label, { detail, stub: false });

    // employment lists
    const allJobs = [
      ...(detail?.currentEmployments || []),
      ...(detail?.previousEmployments || []),
      ...(detail?.currentIAEmployments || []),
      ...(detail?.previousIAEmployments || []),
    ];
    for (const job of allJobs) {
      const fid = job?.firmId || job?.firm_id;
      if (!fid) continue;
      ensureFirmNode(fid, job.firmName || null);
      links.push({
        source: personId(crd),
        target: firmId(fid),
        relationship: "employed_by",
        startDate: job.registrationBeginDate || null,
        endDate: job.registrationEndDate || null,
        firmName: job.firmName || job.firmName || null,
      });
    }
  }

  // Fetch firm detail files (firm_{id}.json) to build control links
  for (const f of finraFiles) {
    if (!f.startsWith("firm_") || !f.endsWith(".json")) continue;
    const filePath = path.join(FINRA_DIR, f);
    const j = await safeReadJSON(filePath);
    if (!j) continue;
    const hits = j?.hits?.hits || [];
    if (!hits.length) continue;
    const src = hits[0]._source || {};
    const firmIdVal = src.firm_id || src.bdSecNumber || src.firmId;
    if (!firmIdVal) continue;
    let detail = null;
    if (src.content) {
      try {
        detail =
          typeof src.content === "string"
            ? JSON.parse(src.content)
            : src.content;
      } catch (e) {
        detail = src.content;
      }
    } else {
      detail = j;
    }
    const firmNode = ensureFirmNode(
      firmIdVal,
      detail?.basicInformation?.firmName || null,
      {},
    );
    // enrich firm node
    if (detail?.basicInformation) {
      const b = detail.basicInformation;
      firmNode.firmType = b.firmType || null;
      firmNode.formedDate = b.formedDate || null;
    }
    // build control links from directOwners
    const owners = detail?.directOwners || [];
    for (const owner of owners) {
      const ownerCrd = owner?.crdNumber || owner?.crd;
      const legalName = owner?.legalName || owner?.name || "";
      const position = owner?.position || owner?.title || null;
      if (ownerCrd) {
        ensurePersonNode(ownerCrd, legalName, { stub: true });
        links.push({
          source: personId(ownerCrd),
          target: firmId(firmIdVal),
          relationship: "controls",
          position,
          startDate: null,
          endDate: null,
        });
      } else if (legalName) {
        // entity owner
        const eid = `entity_${legalName
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, "_")
          .slice(0, 48)}`;
        if (!nodesById[eid])
          nodesById[eid] = { id: eid, label: legalName, group: "entity" };
        links.push({
          source: eid,
          target: firmId(firmIdVal),
          relationship: "controls",
          position,
          startDate: null,
          endDate: null,
        });
      }
    }
  }

  // Also process SEC search results to capture additional CRDs
  for (const f of secFiles) {
    if (!f.endsWith(".json")) continue;
    const filePath = path.join(SEC_DIR, f);
    const j = await safeReadJSON(filePath);
    if (!j) continue;
    const hits = j?.hits?.hits || [];
    for (const h of hits) {
      const src = h._source || {};
      const crd = src.ind_source_id || src.person?.crd;
      if (crd) {
        seenCrds.add(String(crd));
        ensurePersonNode(crd, src.ind_name || null);
      }
    }
  }

  const individuals = Object.values(nodesById).filter(
    (n) => n.group === "individual",
  );
  const firms = Object.values(nodesById).filter((n) => n.group === "firm");
  const entities = Object.values(nodesById).filter((n) => n.group === "entity");

  const graph = {
    nodes: Object.values(nodesById),
    links,
    meta: {
      generated: new Date().toISOString(),
      totalIndividuals: individuals.length,
      totalFirms: firms.length,
      totalEntities: entities.length,
      totalLinks: links.length,
    },
  };

  await fs.writeFile(OUT, JSON.stringify(graph, null, 2), "utf8");
  console.log("WROTE", OUT);
  console.log(
    "INDIVIDUALS=",
    individuals.length,
    "FIRMS=",
    firms.length,
    "ENTITIES=",
    entities.length,
    "LINKS=",
    links.length,
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
