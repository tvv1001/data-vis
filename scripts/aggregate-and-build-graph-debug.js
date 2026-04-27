#!/usr/bin/env node
import fs from "node:fs/promises";
import path from "node:path";

const ROOT = path.resolve(path.dirname(process.argv[1]), "..");
const BASE = path.join(ROOT, "server", "data", "national");
const FINRA_DIR = path.join(BASE, "brokercheck.finra.org");
const OUT = path.join(BASE, "finra-graph-debug.json");

function personId(crd) {
  return `person_${crd}`;
}
function firmId(firm) {
  return `firm_${firm}`;
}
async function safeReadJSON(file) {
  try {
    return JSON.parse(await fs.readFile(file, "utf8"));
  } catch (e) {
    return null;
  }
}

async function main() {
  const nodesById = {};
  const links = [];
  let finraFiles = [];
  try {
    finraFiles = await fs.readdir(FINRA_DIR);
  } catch (e) {
    finraFiles = [];
  }
  console.log("finraFiles count=", finraFiles.length);

  function ensureFirmNode(fid, label) {
    const id = firmId(fid);
    if (!nodesById[id])
      nodesById[id] = {
        id,
        firmId: String(fid),
        label: label || String(fid),
        group: "firm",
      };
    return nodesById[id];
  }
  function ensurePersonNode(crd, label) {
    const id = personId(crd);
    if (!nodesById[id])
      nodesById[id] = {
        id,
        crd: String(crd),
        label: label || String(crd),
        group: "individual",
      };
    return nodesById[id];
  }

  // parse individual detail files
  let processed = 0;
  let linkAdds = 0;
  let firmAdds = 0;
  for (const f of finraFiles) {
    if (!f.startsWith("individual_") || !f.endsWith(".json")) continue;
    const p = path.join(FINRA_DIR, f);
    const j = await safeReadJSON(p);
    if (!j) continue;
    const hits = j.hits?.hits || [];
    if (!hits.length) continue;
    const src = hits[0]._source || {};
    const crd = src.ind_source_id || src.person?.crd;
    if (!crd) continue;
    processed++;
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
    } else detail = j;
    const first =
      detail?.basicInformation?.firstName || src.ind_firstname || null;
    const last = detail?.basicInformation?.lastName || src.ind_lastname || null;
    ensurePersonNode(crd, [first, last].filter(Boolean).join(" "));
    const all = [
      ...(detail.currentEmployments || []),
      ...(detail.previousEmployments || []),
      ...(detail.currentIAEmployments || []),
      ...(detail.previousIAEmployments || []),
    ];
    if (all.length > 0) console.log("file", f, "jobs", all.length);
    for (const job of all) {
      const fid = job?.firmId || job?.firm_id;
      if (!fid) continue;
      if (!nodesById[firmId(fid)]) {
        firmAdds++;
        ensureFirmNode(fid, job.firmName || null);
      }
      links.push({
        source: personId(crd),
        target: firmId(fid),
        relationship: "employed_by",
        startDate: job.registrationBeginDate || null,
        endDate: job.registrationEndDate || null,
        firmName: job.firmName || null,
      });
      linkAdds++;
      if (linkAdds % 1000 === 0) console.log("links so far", linkAdds);
    }
  }

  console.log(
    "processed individual detail files=",
    processed,
    "firmAdds=",
    firmAdds,
    "linkAdds=",
    linkAdds,
  );
  await fs.writeFile(
    OUT,
    JSON.stringify(
      {
        nodes: Object.values(nodesById),
        links,
        meta: { generated: new Date().toISOString() },
      },
      null,
      2,
    ),
    "utf8",
  );
  console.log("WROTE debug graph to", OUT);
}
main().catch((e) => {
  console.error(e);
  process.exit(1);
});
