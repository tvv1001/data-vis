/**
 * finra.js  –  FINRA BrokerCheck Network Graph
 *
 * Renders the finra-graph.json as an interactive D3 v7 force-directed graph.
 *
 * Nodes:
 *   individual  – blue circles  (people discovered from seed search)
 *   firm        – amber squares (registered broker-dealer / IA firms)
 *   entity      – grey diamonds (non-individual Form BD control owners)
 *
 * Links:
 *   employed_by – grey line  (person → firm, with date range on hover)
 *   controls    – red line   (person/entity → firm, from Form BD directOwners)
 */

import "./finra.css";

const BASE = import.meta.env.VITE_API_URL || "http://localhost:3001";
const d3 = window.d3;

// ── State ──────────────────────────────────────────────────────────────────
let graphData = null; // { nodes, links, meta }
let simulation = null;
let selectedId = null;
let linkSel = null; // current <line> selection
let nodeSel = null; // current <g.fg-node> selection
let layoutNodes = null; // node objects with x/y positions
let layoutLinks = null; // link objects (source/target resolved to objects)
let spreadAnimId = null; // rAF handle for neighbor spread animation

// ── Bootstrap ──────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {
  // Top toolbar buttons removed: refresh and run-scraper
  document.getElementById("btn-log-close").addEventListener("click", closeLog);
  // Note: inline "Add Person" UI removed from top — keep log close only

  window.addEventListener("resize", onResize);

  // Search input (filters nodes by label, CRD, BD/IA SEC numbers)
  const searchEl = document.getElementById("fg-search");
  if (searchEl) {
    const debounced = debounce((e) => filterGraph(e.target.value), 200);
    searchEl.addEventListener("input", debounced);
    searchEl.addEventListener("keydown", (ev) => {
      if (ev.key === "Escape") {
        ev.preventDefault();
        searchEl.value = "";
        filterGraph("");
      }
    });
  }

  // Add-missing-person/firm UI
  const addInput = document.getElementById("fg-add-name");
  const addBtn = document.getElementById("fg-add-btn");
  if (addBtn) addBtn.addEventListener("click", () => addPersonToSeeds());
  if (addInput)
    addInput.addEventListener("keydown", (ev) => {
      if (ev.key === "Enter") {
        ev.preventDefault();
        addPersonToSeeds();
      }
    });

  // SEC adviserinfo suggestions
  const sugList = document.getElementById("fg-add-suggestions");
  const addStatus = document.getElementById("fg-add-status");
  const secDebounce = debounce((e) => {
    const q = e.target.value.trim();
    if (!q) return clearSuggestions();
    fetchSECSuggestions(q);
  }, 300);
  if (addInput) addInput.addEventListener("input", secDebounce);
  if (addInput) {
    addInput.addEventListener("blur", () => setTimeout(clearSuggestions, 150));
  }

  async function fetchSECSuggestions(q) {
    clearSuggestions();
    if (!q) return;
    const params = {
      query: q,
      filter:
        "active=true,prev=true,bar=true,broker=true,ia=true,brokeria=true",
      includePrevious: "true",
      hl: "true",
      nrows: "12",
      start: "0",
      r: "25",
      sort: "score desc",
      wt: "json",
    };
    try {
      const url = `${BASE}/api/finra/sec-search`;
      const res = await fetch(
        url + `?${new URLSearchParams(params).toString()}`,
      );
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const body = await res.json();
      // Try common locations for result docs
      const docs =
        body?.response?.docs ||
        body?.docs ||
        body?.hits?.hits ||
        body?.results ||
        body?.data ||
        [];
      const items = Array.isArray(docs)
        ? docs.map((d) => {
            const display =
              d.name ||
              d.fullName ||
              d.primaryName ||
              d.label ||
              (d.names && d.names[0]) ||
              (d._source && (d._source.name || d._source.fullName)) ||
              JSON.stringify(d).slice(0, 80);
            return { display, raw: d };
          })
        : [];
      renderSuggestions(items.slice(0, 12));
    } catch (err) {
      console.error("SEC suggestion fetch failed:", err);
      if (addStatus) {
        addStatus.style.color = "#ef4444";
        addStatus.textContent = "Suggestions unavailable (CORS or network).";
        setTimeout(() => (addStatus.textContent = ""), 3000);
      }
    }
  }

  function renderSuggestions(items) {
    if (!sugList) return;
    sugList.innerHTML = "";
    if (!items || items.length === 0) return clearSuggestions();
    items.forEach((it, idx) => {
      const li = document.createElement("li");
      li.setAttribute("role", "option");
      li.className = "fg-add-suggestion";
      li.tabIndex = 0;
      li.dataset.index = idx;
      li.textContent = it.display;
      li.addEventListener("click", () => {
        if (addInput) addInput.value = it.display;
        clearSuggestions();
        addInput?.focus();
      });
      li.addEventListener("keydown", (ev) => {
        if (ev.key === "Enter") {
          ev.preventDefault();
          li.click();
        }
      });
      sugList.appendChild(li);
    });
    sugList.classList.remove("hidden");
  }

  function clearSuggestions() {
    if (!sugList) return;
    sugList.innerHTML = "";
    sugList.classList.add("hidden");
  }

  renderLegend();
  loadGraph();

  // Refresh button removed from UI — graph can be refreshed programmatically
});

// ── Data loading ────────────────────────────────────────────────────────────
async function loadGraph() {
  try {
    // Append a timestamp to ensure we always get the latest graph on reload
    const url = new URL(`${BASE}/api/finra/graph`);
    url.searchParams.set("t", String(Date.now()));
    const res = await fetch(url.toString(), { cache: "no-store" });
    if (!res.ok) {
      if (res.status === 404) {
        showEmpty(true);
        return;
      }
      throw new Error(`HTTP ${res.status}`);
    }
    graphData = await res.json();
    showEmpty(false);
    updateMeta(graphData.meta);
    renderGraph(graphData);
  } catch (err) {
    console.error("loadGraph:", err);
    showEmpty(true);
  }
}

// Debounce helper
function debounce(fn, ms) {
  let t;
  return function (...args) {
    clearTimeout(t);
    t = setTimeout(() => fn.apply(this, args), ms);
  };
}

// Filter rendered graph nodes and links by a query string.
// Supports matching node.label (name/firm), node.crd, node.bdSecNumber, node.iaSecNumber.
function filterGraph(rawQuery) {
  const q = String(rawQuery || "").trim();
  const qlow = q.toLowerCase();
  if (!nodeSel || !linkSel || !layoutNodes || !layoutLinks) return;

  if (!q) {
    // reset
    nodeSel.style("opacity", null).classed("filtered", false);
    linkSel
      .style("stroke-opacity", (d) =>
        d.relationship === "controls" ? 0.6 : 0.6,
      )
      .style("opacity", null);
    return;
  }

  // Helpers to read common fields across slightly different node shapes
  function firstField(obj, keys) {
    for (const k of keys) {
      if (obj[k] != null) return obj[k];
      if (obj._source && obj._source[k] != null) return obj._source[k];
    }
    return null;
  }

  function normalizeDigits(s) {
    return String(s || "").replace(/[^0-9]/g, "");
  }

  const isExactNumeric =
    /^\d+$/.test(q) ||
    /^\d+-\d+$/.test(q) ||
    /^crd:/i.test(q) ||
    /^sec:/i.test(q);

  // determine matching node ids
  const matched = new Set();
  layoutNodes.forEach((n) => {
    // gather candidate values
    const label = String(
      firstField(n, ["label", "firm_name", "firmName"]) || "",
    );
    const labelLow = label.toLowerCase();

    const crd = String(
      firstField(n, ["crd", "ind_source_id", "ind_crd"]) || "",
    );
    const bdSec = String(
      firstField(n, ["bdSecNumber", "bd_sec_number", "firm_bd_sec_number"]) ||
        "",
    );
    const bdFull = String(firstField(n, ["firm_bd_full_sec_number"]) || "");
    const firmSrc = String(firstField(n, ["firm_source_id", "firm_id"]) || "");

    // person name pieces
    const fname = String(firstField(n, ["ind_firstname"]) || "");
    const mname = String(firstField(n, ["ind_middlename"]) || "");
    const lname = String(firstField(n, ["ind_lastname"]) || "");
    const personFull = [fname, mname, lname].filter(Boolean).join(" ");

    // firm address (may be stored as JSON string)
    let addrObj = null;
    const addrRaw = firstField(n, ["firm_address_details", "address_details"]);
    if (addrRaw) {
      try {
        addrObj = typeof addrRaw === "string" ? JSON.parse(addrRaw) : addrRaw;
      } catch (e) {
        addrObj = null;
      }
    }

    // exact numeric match for CRD/SEC/firmsource
    if (isExactNumeric) {
      const qDigits = normalizeDigits(q);
      // check CRD / source ids
      if (
        normalizeDigits(crd) === qDigits ||
        normalizeDigits(firmSrc) === qDigits
      ) {
        matched.add(n.id);
        return;
      }
      // check bd sec numbers: either numeric or full with hyphen
      if (bdFull && bdFull.toLowerCase() === q.toLowerCase()) {
        matched.add(n.id);
        return;
      }
      if (normalizeDigits(bdSec) === qDigits) {
        matched.add(n.id);
        return;
      }
      // also check node._source fields if present
      const src = n._source || {};
      if (src.ind_source_id && normalizeDigits(src.ind_source_id) === qDigits) {
        matched.add(n.id);
        return;
      }
      if (
        src.firm_bd_full_sec_number &&
        String(src.firm_bd_full_sec_number).toLowerCase() === q.toLowerCase()
      ) {
        matched.add(n.id);
        return;
      }
      // no exact match
      return;
    }

    // Non-exact: loose matching for main name/firm only (exclude alternate names)
    const ql = qlow;
    if (labelLow.includes(ql) || personFull.toLowerCase().includes(ql)) {
      matched.add(n.id);
      return;
    }

    // address match for firms: search street/city/state/postal
    if (addrObj) {
      const office = addrObj.officeAddress || addrObj.office || {};
      const mail = addrObj.mailingAddress || addrObj.mailing || {};
      const addrText = [
        office.street1,
        office.street2,
        office.city,
        office.state,
        office.postalCode,
        mail.street1,
        mail.city,
      ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
      if (addrText.includes(ql)) {
        matched.add(n.id);
        return;
      }
    }

    // employment branch match for individuals
    const emp = firstField(n, ["ind_current_employments", "ind_employments"]);
    if (Array.isArray(emp)) {
      for (const e of emp) {
        const city = String(e.branch_city || e.city || "").toLowerCase();
        const state = String(e.branch_state || e.state || "").toLowerCase();
        const zip = String(e.branch_zip || e.postalCode || "").toLowerCase();
        if (city.includes(ql) || state.includes(ql) || zip.includes(ql)) {
          matched.add(n.id);
          return;
        }
      }
    }
  });

  // include direct neighbors of matched nodes for context
  const expanded = new Set(matched);
  matched.forEach((id) => {
    const nb = getNeighborIds(id);
    nb.forEach((x) => expanded.add(x));
  });

  // update node opacity
  nodeSel.style("opacity", (d) => (expanded.has(d.id) ? 1 : 0.08));

  // update links: highlight links connected to any matched node, dim others
  linkSel
    .style("stroke-opacity", (l) => {
      const srcId = l.source?.id ?? l.source;
      const tgtId = l.target?.id ?? l.target;
      if (matched.has(srcId) || matched.has(tgtId)) return 1;
      if (expanded.has(srcId) || expanded.has(tgtId)) return 0.45;
      return 0.05;
    })
    .style("opacity", (l) => {
      const srcId = l.source?.id ?? l.source;
      const tgtId = l.target?.id ?? l.target;
      return matched.has(srcId) ||
        matched.has(tgtId) ||
        expanded.has(srcId) ||
        expanded.has(tgtId)
        ? 1
        : 0.15;
    });
}

function updateMeta(meta = {}) {
  if (!meta) return;
  const el = document.getElementById("fg-meta-label");
  const parts = [];
  if (meta.totalIndividuals != null)
    parts.push(`${meta.totalIndividuals} people`);
  if (meta.totalFirms != null) parts.push(`${meta.totalFirms} firms`);
  if (meta.totalLinks != null) parts.push(`${meta.totalLinks} links`);
  if (meta.generated) {
    const d = new Date(meta.generated);
    parts.push(`built ${d.toLocaleDateString()}`);
  }
  el.textContent = parts.join("  ·  ");
}

function showEmpty(show) {
  document.getElementById("fg-empty")?.classList.toggle("hidden", !show);
  document.getElementById("fg-svg").style.visibility = show
    ? "hidden"
    : "visible";
  document.getElementById("fg-legend").style.display = show ? "none" : "flex";
}

// ── Run scraper ─────────────────────────────────────────────────────────────
function runScraper() {
  const panel = document.getElementById("fg-log-panel");
  const logBody = document.getElementById("fg-log-body");
  panel.classList.remove("hidden");
  logBody.textContent = "";

  function runBatch() {
    fetch(`${BASE}/api/finra/run-scraper`, { method: "POST" })
      .then((res) => {
        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let hasMore = false;

        function pump() {
          reader.read().then(({ done, value }) => {
            if (done) {
              loadGraph();
              return;
            }
            const text = decoder.decode(value, { stream: true });
            // SSE lines: data: {...}\n\n
            text.split("\n").forEach((line) => {
              if (!line.startsWith("data:")) return;
              try {
                const { type, data } = JSON.parse(line.slice(5).trim());
                if (type === "stdout" || type === "stderr") {
                  logBody.textContent += data;
                  logBody.scrollTop = logBody.scrollHeight;
                  if (
                    typeof data === "string" &&
                    /\d+ more pending after this batch/.test(data)
                  ) {
                    hasMore = true;
                  }
                }
                if (type === "done") {
                  logBody.textContent += `\n[exit code ${data.exitCode}]\n`;
                  logBody.scrollTop = logBody.scrollHeight;
                  if (data.exitCode === 0) {
                    loadGraph();
                    if (hasMore) {
                      logBody.textContent += "\nStarting next batch…\n";
                      logBody.scrollTop = logBody.scrollHeight;
                      runBatch();
                    }
                  }
                }
              } catch (e) {
                /* malformed chunk */
              }
            });
            pump();
          });
        }
        pump();
      })
      .catch((err) => {
        logBody.textContent += `\nError: ${err.message}\n`;
      });
  }

  runBatch();
}

function closeLog() {
  document.getElementById("fg-log-panel").classList.add("hidden");
}

// ── Add person to seeds ──────────────────────────────────────────────────────
async function addPersonToSeeds() {
  const input = document.getElementById("fg-add-name");
  const status = document.getElementById("fg-add-status");
  const name = input.value.trim();
  if (!name) return;

  status.textContent = "Saving…";
  status.style.color = "";

  try {
    // Fetch current seeds and fail early if the GET fails
    const getRes = await fetch(`${BASE}/api/finra/seeds`);
    if (!getRes.ok) {
      const txt = await getRes.text().catch(() => "");
      throw new Error(`GET /api/finra/seeds failed ${getRes.status}: ${txt}`);
    }
    const existing = await getRes.json();
    const seeds = Array.isArray(existing) ? existing : [];
    if (seeds.includes(name)) {
      status.textContent = `Already in seeds.`;
      status.style.color = "var(--c-firm, #f59e0b)";
      return;
    }
    const updated = [...seeds, name];
    const res = await fetch(`${BASE}/api/finra/seeds`, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: JSON.stringify({ seeds: updated }),
    });

    // Read response text so we can display helpful diagnostics for browser errors
    const resText = await res.text().catch(() => "");
    let resBody = null;
    try {
      resBody = JSON.parse(resText);
    } catch (e) {
      resBody = null;
    }
    if (!res.ok) {
      console.error("PUT /api/finra/seeds failed:", res.status, resText);
      const msg =
        resBody?.error || resBody?.message || resText || `HTTP ${res.status}`;
      throw new Error(msg);
    }
    input.value = "";
    status.style.color = "var(--c-individual, #3b82f6)";
    status.textContent = `Added "${name}"`;
    // Refresh graph/meta so counts (people/firms) update immediately
    try {
      await loadGraph();
    } catch (e) {
      // non-fatal: leave status but log
      console.error("reload after add failed:", e);
    }
    setTimeout(() => {
      status.textContent = "";
    }, 4000);
  } catch (err) {
    console.error("addPersonToSeeds error:", err);
    status.style.color = "#ef4444";
    // show a compact but descriptive message in the UI
    status.textContent = `Error: ${err.message || "unknown"}`;
  }
}

// ── D3 Rendering ────────────────────────────────────────────────────────────
const NODE_R = { individual: 10, firm: 12, entity: 9 };
const NODE_COLOR = {
  individual: "var(--c-individual)",
  firm: "var(--c-firm)",
  entity: "var(--c-entity)",
};
const LINK_COLOR = {
  employed_by: "var(--c-employed)",
  controls: "var(--c-controls)",
};

function renderGraph(data) {
  if (simulation) simulation.stop();
  if (spreadAnimId) {
    cancelAnimationFrame(spreadAnimId);
    spreadAnimId = null;
  }
  const svg = d3.select("#fg-svg");
  svg.selectAll("*").remove();

  const main = document.getElementById("fg-main");
  const W = main.clientWidth;
  const H = main.clientHeight;

  svg.attr("viewBox", `0 0 ${W} ${H}`);

  // Deep-copy so D3 mutation doesn't corrupt the original
  const nodes = data.nodes.map((n) => ({ ...n }));
  const links = data.links.map((l) => ({ ...l }));
  layoutNodes = nodes;
  layoutLinks = links;

  // ── Per-node degree stats for scaled / tinted firm nodes ─────────────────
  const _degMap = new Map();
  nodes.forEach((n) =>
    _degMap.set(n.id, { total: 0, controls: 0, employed: 0 }),
  );
  links.forEach((l) => {
    const srcId = l.source?.id ?? l.source;
    const tgtId = l.target?.id ?? l.target;
    [[srcId], [tgtId]].forEach(([id]) => {
      const e = _degMap.get(id);
      if (!e) return;
      e.total++;
      if (l.relationship === "controls") e.controls++;
      else e.employed++;
    });
  });
  const _maxFirmDeg = Math.max(
    1,
    ...nodes
      .filter((n) => n.group === "firm")
      .map((n) => _degMap.get(n.id)?.total || 0),
  );
  const _maxIndDeg = Math.max(
    1,
    ...nodes
      .filter((n) => n.group === "individual")
      .map((n) => _degMap.get(n.id)?.total || 0),
  );
  nodes.forEach((n) => {
    n._deg = _degMap.get(n.id) || { total: 0, controls: 0, employed: 0 };
    if (n.group === "firm") {
      // scale: 1× at degree 0 → 2.5× at max degree (sqrt for perceptual linearity)
      const scale =
        1 + (Math.sqrt(n._deg.total) / Math.sqrt(_maxFirmDeg)) * 1.5;
      n._vizHalf = (NODE_R.firm * 1.7 * scale) / 2;
    }
    // Make individual seeds larger when they have more connections
    if (n.group === "individual") {
      const scale = 1 + (Math.sqrt(n._deg.total) / Math.sqrt(_maxIndDeg)) * 2.5;
      n._vizHalf = (NODE_R.individual * 1.7 * scale) / 2;
    }
  });

  // ── Zoom ──────────────────────────────────────────────────────────────────
  const zoom = d3
    .zoom()
    .scaleExtent([0.1, 6])
    .on("zoom", (event) => root.attr("transform", event.transform));

  svg.call(zoom);

  const root = svg.append("g").attr("class", "fg-root");

  // ── Arrow markers ─────────────────────────────────────────────────────────
  const defs = svg.append("defs");

  ["employed_by", "controls"].forEach((rel) => {
    defs
      .append("marker")
      .attr("id", `arrow-${rel}`)
      .attr("viewBox", "0 -4 8 8")
      .attr("refX", 22)
      .attr("refY", 0)
      .attr("markerWidth", 6)
      .attr("markerHeight", 6)
      .attr("orient", "auto")
      .append("path")
      .attr("d", "M0,-4L8,0L0,4")
      .attr("fill", rel === "controls" ? "#ef4444" : "#94a3b8");
  });

  // ── Force simulation ──────────────────────────────────────────────────────
  simulation = d3
    .forceSimulation(nodes)
    .alphaDecay(0.05)
    .force(
      "link",
      d3
        .forceLink(links)
        .id((d) => d.id)
        .distance(180),
    )
    .force("charge", d3.forceManyBody().strength(-700))
    .force("center", d3.forceCenter(W / 2, H / 2))
    // per-node radius so scaled firm squares don't overlap each other
    .force(
      "collision",
      d3
        .forceCollide()
        .radius((d) =>
          d._vizHalf != null ? d._vizHalf + 28 : (NODE_R[d.group] || 10) + 28,
        )
        .strength(0.9),
    );

  // ── Links ─────────────────────────────────────────────────────────────────
  const link = root
    .append("g")
    .attr("class", "fg-links")
    .selectAll("line")
    .data(links)
    .join("line")
    .attr("stroke", (d) => LINK_COLOR[d.relationship] || "#94a3b8")
    .attr("stroke-opacity", 0.6)
    .attr("stroke-width", (d) => (d.relationship === "controls" ? 1.5 : 1))
    .attr("marker-end", (d) => `url(#arrow-${d.relationship})`);
  linkSel = link;

  // ── Nodes ─────────────────────────────────────────────────────────────────
  const node = root
    .append("g")
    .attr("class", "fg-nodes")
    .selectAll("g")
    .data(nodes)
    .join("g")
    .attr("class", "fg-node")
    .call(fluidDrag())
    .on("click", (event, d) => {
      event.stopPropagation();
      selectNode(d);
    });
  nodeSel = node;

  // Shapes
  node.each(function (d) {
    const g = d3.select(this);
    const r = NODE_R[d.group] || 10;
    const color = NODE_COLOR[d.group] || "#94a3b8";

    if (d.group === "firm") {
      const s = (d._vizHalf ?? r * 0.85) * 2;
      // Dominant-link stroke: red = controls, slate = employed_by, white = neutral
      const deg = d._deg || { total: 0, controls: 0, employed: 0 };
      const dominantStroke =
        deg.controls > deg.employed
          ? "#ef4444"
          : deg.employed > deg.controls
            ? "#64748b"
            : "#fff";
      const strokeW = deg.total > 0 ? 2.5 : 1.5;
      // Outer dashed ring shows the minority link type when both are present
      if (deg.controls > 0 && deg.employed > 0) {
        const minorityStroke =
          deg.controls > deg.employed ? "#64748b" : "#ef4444";
        g.append("rect")
          .attr("x", -s / 2 - 4)
          .attr("y", -s / 2 - 4)
          .attr("width", s + 8)
          .attr("height", s + 8)
          .attr("rx", 6)
          .attr("fill", "none")
          .attr("stroke", minorityStroke)
          .attr("stroke-width", 1.5)
          .attr("stroke-dasharray", "4 3")
          .attr("opacity", 0.5);
      }
      g.append("rect")
        .attr("x", -s / 2)
        .attr("y", -s / 2)
        .attr("width", s)
        .attr("height", s)
        .attr("rx", 3)
        .attr("fill", color)
        .attr("stroke", dominantStroke)
        .attr("stroke-width", strokeW)
        .attr("opacity", d.stub ? 0.45 : 0.9);
    } else if (d.group === "entity") {
      const s = r * 1.5;
      g.append("polygon")
        .attr("points", `0,${-s} ${s},0 0,${s} ${-s},0`)
        .attr("fill", color)
        .attr("stroke", "#fff")
        .attr("stroke-width", 1.5)
        .attr("opacity", 0.8);
    } else {
      const rv = d._vizHalf != null ? d._vizHalf : r;
      g.append("circle")
        .attr("r", rv)
        .attr("fill", color)
        .attr("stroke", "#fff")
        .attr("stroke-width", 1.5)
        .attr("opacity", d.stub ? 0.5 : 1);
    }

    // Disclosure indicator ring
    if (d.group === "individual" && d.disclosureCount > 0) {
      const _r = d._vizHalf != null ? d._vizHalf : r;
      g.append("circle")
        .attr("r", _r + 4)
        .attr("fill", "none")
        .attr("stroke", "#f97316")
        .attr("stroke-width", 2)
        .attr("stroke-dasharray", "3 2")
        .attr("opacity", 0.75);
    }
  });

  // Labels — rendered in two passes (stroke halo first, then fill) so text
  // stays readable even when nodes are still close after zooming out.
  // Both elements live inside the <g> so they move with every transform.
  ["halo", "fill"].forEach((pass) => {
    node
      .append("text")
      .attr("class", `fg-label-${pass}`)
      .attr("dy", (d) =>
        d._vizHalf != null ? d._vizHalf + 14 : (NODE_R[d.group] || 10) + 14,
      )
      .attr("text-anchor", "middle")
      .attr("font-size", "10px")
      .attr("font-family", "var(--sans)")
      .attr("font-weight", "500")
      .attr("fill", pass === "halo" ? "none" : "#1e293b")
      .attr("stroke", pass === "halo" ? "rgba(246,248,252,0.92)" : "none")
      .attr("stroke-width", pass === "halo" ? 4 : 0)
      .attr("stroke-linejoin", "round")
      .attr("paint-order", "stroke")
      .attr("pointer-events", "none")
      .text((d) => truncate(capitalize(d.label), 22));
  });
  // Tooltip
  node.append("title").text((d) => {
    const parts = [d.label, d.group.toUpperCase()];
    if (d.crd) parts.push(`CRD: ${d.crd}`);
    return parts.join("\n");
  });

  // ── Tick ──────────────────────────────────────────────────────────────────
  simulation.on("tick", () => {
    link
      .attr("x1", (d) => d.source.x)
      .attr("y1", (d) => d.source.y)
      .attr("x2", (d) => d.target.x)
      .attr("y2", (d) => d.target.y);

    node.attr("transform", (d) => `translate(${d.x},${d.y})`);
  });

  // Freeze all nodes once the initial layout converges — no more jiggling
  simulation.on("end", () => {
    nodes.forEach((d) => {
      d.fx = d.x;
      d.fy = d.y;
    });
  });

  // Deselect on blank click
  svg.on("click", () => {
    selectedId = null;
    node.classed("selected", false);
    highlightLinks(null);
    showSidebarHint();
  });
}

// ── Fluid Drag (simulation-driven neighbor repulsion) ────────────────────
function fluidDrag() {
  return d3
    .drag()
    .on("start", function (event, d) {
      // Cancel any pending click-spread animation
      if (spreadAnimId) {
        cancelAnimationFrame(spreadAnimId);
        spreadAnimId = null;
      }
      // Pin the dragged node
      d.fx = d.x;
      d.fy = d.y;
      // Unfix direct neighbors so the simulation can push them aside
      const neighborIds = getNeighborIds(d.id);
      layoutNodes.forEach((n) => {
        if (neighborIds.has(n.id)) {
          n.fx = null;
          n.fy = null;
        }
      });
      // Reheat just enough for fluid neighbor movement
      simulation.alphaTarget(0.3).restart();
    })
    .on("drag", function (event, d) {
      d.fx = event.x;
      d.fy = event.y;
    })
    .on("end", function (event, d) {
      // Cool down – simulation will coast to rest then the "end" handler re-freezes all
      simulation.alphaTarget(0);
    });
}

// Returns the set of node ids directly connected to the given node id
function getNeighborIds(nodeId) {
  const ids = new Set();
  if (!layoutLinks) return ids;
  layoutLinks.forEach((l) => {
    const srcId = l.source?.id ?? l.source;
    const tgtId = l.target?.id ?? l.target;
    if (srcId === nodeId) ids.add(tgtId);
    if (tgtId === nodeId) ids.add(srcId);
  });
  return ids;
}

// ── Selection & Sidebar ─────────────────────────────────────────────────────
function selectNode(d) {
  selectedId = d.id;
  nodeSel.classed("selected", (n) => n.id === d.id);
  highlightLinks(d.id);
  renderSidebar(d);
  spreadNeighbors(d);
}

function showSidebarHint() {
  document.getElementById("fg-sidebar-inner").innerHTML =
    `<p class="fg-hint">Click a node to inspect it.</p>`;
}

// ── Link highlight on selection ───────────────────────────────────────────────
// activeId = null  → reset all lines to their default appearance
// activeId = id    → brighten connected lines by type; dim unconnected ones
function highlightLinks(activeId) {
  if (!linkSel) return;
  if (activeId == null) {
    linkSel
      .attr("stroke", (d) => LINK_COLOR[d.relationship] || "#94a3b8")
      .attr("stroke-opacity", 0.6)
      .attr("stroke-width", (d) => (d.relationship === "controls" ? 1.5 : 1));
    return;
  }
  linkSel.each(function (d) {
    const srcId = d.source?.id ?? d.source;
    const tgtId = d.target?.id ?? d.target;
    const connected = srcId === activeId || tgtId === activeId;
    const sel = d3.select(this);
    if (connected) {
      // controls → vivid red; employed_by → vivid cyan-blue
      sel
        .attr("stroke", d.relationship === "controls" ? "#ff2222" : "#38bdf8")
        .attr("stroke-opacity", 1)
        .attr("stroke-width", d.relationship === "controls" ? 2.5 : 2);
    } else {
      sel
        .attr("stroke", LINK_COLOR[d.relationship] || "#94a3b8")
        .attr("stroke-opacity", 0.15)
        .attr("stroke-width", 1);
    }
  });
}

// ── Spread neighbors on click ────────────────────────────────────────────────
function spreadNeighbors(clickedNode) {
  if (!layoutNodes || !layoutLinks || !nodeSel || !linkSel) return;
  if (spreadAnimId) {
    cancelAnimationFrame(spreadAnimId);
    spreadAnimId = null;
  }

  const SPREAD = 80;
  const DURATION = 480;

  // Find all direct neighbor IDs
  const neighborIds = new Set();
  layoutLinks.forEach((l) => {
    const srcId = l.source?.id ?? l.source;
    const tgtId = l.target?.id ?? l.target;
    if (srcId === clickedNode.id) neighborIds.add(tgtId);
    if (tgtId === clickedNode.id) neighborIds.add(srcId);
  });

  if (neighborIds.size === 0) return;

  // Fast node lookup
  const nodeById = new Map(layoutNodes.map((d) => [d.id, d]));

  // Capture start and target positions for each neighbor
  const snapshots = new Map();
  neighborIds.forEach((id) => {
    const d = nodeById.get(id);
    if (!d) return;
    const dx = d.x - clickedNode.x;
    const dy = d.y - clickedNode.y;
    const dist = Math.sqrt(dx * dx + dy * dy) || 1;
    snapshots.set(id, {
      x0: d.x,
      y0: d.y,
      x1: d.x + (dx / dist) * SPREAD,
      y1: d.y + (dy / dist) * SPREAD,
    });
  });

  const startTime = performance.now();

  function frame(now) {
    const raw = Math.min((now - startTime) / DURATION, 1);
    const ease = d3.easeCubicOut(raw);

    // Interpolate positions directly in the data objects
    // (link .source.x / .target.y then read naturally)
    snapshots.forEach((snap, id) => {
      const d = nodeById.get(id);
      if (!d) return;
      d.x = snap.x0 + (snap.x1 - snap.x0) * ease;
      d.y = snap.y0 + (snap.y1 - snap.y0) * ease;
    });

    // Re-render affected nodes
    nodeSel
      .filter((d) => neighborIds.has(d.id))
      .attr("transform", (d) => `translate(${d.x},${d.y})`);

    // Re-render all links touching the clicked node or any neighbor
    linkSel
      .filter((l) => {
        const srcId = l.source?.id ?? l.source;
        const tgtId = l.target?.id ?? l.target;
        return (
          srcId === clickedNode.id ||
          tgtId === clickedNode.id ||
          neighborIds.has(srcId) ||
          neighborIds.has(tgtId)
        );
      })
      .attr("x1", (l) => l.source.x)
      .attr("y1", (l) => l.source.y)
      .attr("x2", (l) => l.target.x)
      .attr("y2", (l) => l.target.y);

    if (raw < 1) {
      spreadAnimId = requestAnimationFrame(frame);
    } else {
      spreadAnimId = null;
      // Freeze at final positions
      snapshots.forEach((snap, id) => {
        const d = nodeById.get(id);
        if (!d) return;
        d.x = snap.x1;
        d.y = snap.y1;
        d.fx = d.x;
        d.fy = d.y;
      });
    }
  }

  spreadAnimId = requestAnimationFrame(frame);
}

function renderSidebar(d) {
  const el = document.getElementById("fg-sidebar-inner");
  el.innerHTML =
    d.group === "firm"
      ? renderFirmDetail(d)
      : d.group === "entity"
        ? renderEntityDetail(d)
        : renderPersonDetail(d);
}

// ── Person detail ────────────────────────────────────────────────────────────
function renderPersonDetail(d) {
  const links = (graphData?.links || []).filter(
    (l) =>
      (l.source?.id || l.source) === d.id ||
      (l.target?.id || l.target) === d.id,
  );

  const employmentLinks = links.filter((l) => l.relationship === "employed_by");
  const controlLinks = links.filter((l) => l.relationship === "controls");

  const scopeBadge = (s) =>
    s
      ? `<span class="fg-badge ${s.toLowerCase().includes("active") && !s.toLowerCase().includes("in") ? "active" : "inactive"}">${s}</span>`
      : "";

  const stubBadge = d.stub
    ? `<span class="fg-badge stub">Form BD stub</span>`
    : "";

  // Sort employments: current (no endDate) first, then by startDate desc
  const sorted = [...employmentLinks].sort((a, b) => {
    if (!a.endDate && b.endDate) return -1;
    if (a.endDate && !b.endDate) return 1;
    return (b.startDate || "").localeCompare(a.startDate || "");
  });

  const disclosures = d.disclosures || [];

  return `
    <div class="fg-sb-header individual">
      <div class="fg-sb-title">${esc(d.label)}</div>
      <div class="fg-sb-badges">
        ${scopeBadge(d.bcScope)}
        ${scopeBadge(d.iaScope)}
        ${stubBadge}
        ${disclosures.length ? `<span class="fg-badge inactive">${disclosures.length} disclosure${disclosures.length > 1 ? "s" : ""}</span>` : ""}
      </div>
    </div>
    <div class="fg-sb-body">
      ${d.crd ? row("CRD", `<code>${d.crd}</code>`) : ""}
      ${d.otherNames?.length ? row("Also known as", esc(d.otherNames.join(", "))) : ""}
      ${d.daysInIndustry != null ? row("Days in industry", d.daysInIndustry.toLocaleString()) : ""}
      ${d.examsCount ? row("Exams passed", d.examsCount) : ""}
      ${d.exams?.length ? row("Exams", esc(d.exams.join(", "))) : ""}
      ${d.registeredStates?.length ? row("States", esc(d.registeredStates.join(", "))) : ""}

      ${
        controlLinks.length
          ? `
        <div class="fg-section-title">Control Positions</div>
        ${controlLinks
          .map((l) => {
            const firmNode = graphData.nodes.find(
              (n) => n.id === (l.target?.id || l.target),
            );
            return `<div class="fg-tl-entry">
            <span class="fg-tl-firm">${esc(firmNode?.label || l.firmName || "")}</span>
            <span class="fg-tl-loc">${esc(l.position || "")}</span>
          </div>`;
          })
          .join("")}
      `
          : ""
      }

      ${
        sorted.length
          ? `
        <div class="fg-section-title">Employment Timeline</div>
        <div class="fg-timeline">
          ${sorted
            .map((l) => {
              const firmNode = graphData.nodes.find(
                (n) => n.id === (l.target?.id || l.target),
              );
              const name = firmNode?.label || l.firmName || "";
              const loc = [l.city, l.state].filter(Boolean).join(", ");
              const start = l.startDate || "–";
              const end = l.endDate || "present";
              return `<div class="fg-tl-entry">
              <span class="fg-tl-firm">${esc(name)}</span>
              <span class="fg-tl-dates">${start} → ${end}</span>
              ${loc ? `<span class="fg-tl-loc">${esc(loc)}</span>` : ""}
            </div>`;
            })
            .join("")}
        </div>
      `
          : ""
      }

      ${
        disclosures.length
          ? `
        <div class="fg-section-title">Disclosures</div>
        ${disclosures
          .map(
            (dis) => `
          <div class="fg-disclosure">
            <span class="fg-dis-type">${esc(dis.type || "")}</span>
            ${dis.date ? `<span class="fg-dis-date">${dis.date}</span>` : ""}
            ${dis.resolution ? `<span class="fg-dis-res">${esc(dis.resolution)}</span>` : ""}
            ${dis.detail ? `<div class="fg-dis-detail">${esc(String(dis.detail).slice(0, 300))}${String(dis.detail).length > 300 ? "…" : ""}</div>` : ""}
          </div>
        `,
          )
          .join("")}
      `
          : ""
      }
    </div>
  `;
}

// ── Firm detail ──────────────────────────────────────────────────────────────
function renderFirmDetail(d) {
  const owners = d.directOwners || [];
  const disclosures = d.disclosures || [];

  return `
    <div class="fg-sb-header firm">
      <div class="fg-sb-title">${esc(d.label)}</div>
      <div class="fg-sb-badges">
        ${d.bcScope ? `<span class="fg-badge ${d.bcScope === "ACTIVE" ? "active" : "inactive"}">${d.bcScope}</span>` : ""}
        ${d.firmSize ? `<span class="fg-badge">${esc(d.firmSize)}</span>` : ""}
      </div>
    </div>
    <div class="fg-sb-body">
      ${row("Firm ID", d.firmId)}
      ${d.bdSecNumber ? row("BD SEC #", d.bdSecNumber) : ""}
      ${d.iaSecNumber ? row("IA SEC #", d.iaSecNumber) : ""}
      ${d.firmType ? row("Type", esc(d.firmType)) : ""}
      ${d.regulator ? row("Regulator", esc(d.regulator)) : ""}
      ${d.formedState ? row("Formed in", esc(d.formedState)) : ""}
      ${d.formedDate ? row("Formed", d.formedDate) : ""}
      ${d.otherNames?.length ? row("Other names", esc(d.otherNames.join("; "))) : ""}

      ${
        disclosures.length
          ? `
        <div class="fg-section-title">Disclosure Summary</div>
        ${disclosures
          .map(
            (dis) => `
          <div class="fg-detail-row">
            <span class="fg-label">${esc(dis.type || "")}</span>
            <span>${dis.count ?? ""}</span>
          </div>
        `,
          )
          .join("")}
      `
          : ""
      }

      ${
        owners.length
          ? `
        <div class="fg-section-title">Form BD — Direct Owners &amp; Executive Officers</div>
        ${owners
          .map(
            (o) => `
          <div class="fg-owner-row">
            <span class="fg-owner-name">${esc(o.legalName || "")}</span>
            <span class="fg-owner-pos">${esc(o.position || "")}</span>
            ${o.crdNumber ? `<span class="fg-owner-crd">CRD ${o.crdNumber}</span>` : ""}
          </div>
        `,
          )
          .join("")}
      `
          : ""
      }
    </div>
  `;
}

// ── Entity detail ────────────────────────────────────────────────────────────
function renderEntityDetail(d) {
  return `
    <div class="fg-sb-header entity">
      <div class="fg-sb-title">${esc(d.label)}</div>
      <div class="fg-sb-badges">
        <span class="fg-badge">Entity</span>
        ${d.bcScope ? `<span class="fg-badge">${esc(d.bcScope)}</span>` : ""}
      </div>
    </div>
    <div class="fg-sb-body">
      <p style="font-size:13px;color:var(--text-m);margin-top:8px">
        Non-individual owner listed on Form BD (no CRD number).
      </p>
    </div>
  `;
}

// ── Legend ────────────────────────────────────────────────────────────────────
function renderLegend() {
  const items = [
    {
      color: "var(--c-individual)",
      shape: "circle",
      label: "Individual (seed)",
    },
    {
      color: "var(--c-individual)",
      shape: "circle-s",
      label: "Stub (Form BD only)",
      opacity: 0.45,
    },
    { color: "var(--c-firm)", shape: "rect", label: "Firm" },
    {
      color: "var(--c-entity)",
      shape: "diamond",
      label: "Entity (non-CRD owner)",
    },
    { color: "var(--c-employed)", shape: "line", label: "Employed by" },
    { color: "var(--c-controls)", shape: "line", label: "Controls (Form BD)" },
    { color: "#f97316", shape: "ring", label: "Has disclosures" },
  ];

  const legend = document.getElementById("fg-legend");
  legend.innerHTML = items
    .map(({ color, shape, label, opacity = 1 }) => {
      let svg;
      if (shape === "circle" || shape === "circle-s") {
        svg = `<svg width="16" height="16"><circle cx="8" cy="8" r="7" fill="${color}" opacity="${opacity}" stroke="#fff" stroke-width="1.5"/></svg>`;
      } else if (shape === "rect") {
        svg = `<svg width="16" height="16"><rect x="2" y="2" width="12" height="12" rx="2" fill="${color}" stroke="#fff" stroke-width="1.5" opacity="0.9"/></svg>`;
      } else if (shape === "diamond") {
        svg = `<svg width="16" height="16"><polygon points="8,1 15,8 8,15 1,8" fill="${color}" stroke="#fff" stroke-width="1.5" opacity="0.8"/></svg>`;
      } else if (shape === "ring") {
        svg = `<svg width="16" height="16"><circle cx="8" cy="8" r="6" fill="none" stroke="${color}" stroke-width="2" stroke-dasharray="3 2"/></svg>`;
      } else {
        svg = `<svg width="16" height="4"><line x1="0" y1="2" x2="16" y2="2" stroke="${color}" stroke-width="${color === "var(--c-controls)" ? 2 : 1.5}"/></svg>`;
      }
      return `<div class="fg-legend-item">${svg}<span>${label}</span></div>`;
    })
    .join("");
}

// ── Resize ────────────────────────────────────────────────────────────────────
function onResize() {
  if (!graphData) return;
  // Just update the viewBox — no re-simulation, positions stay frozen
  const main = document.getElementById("fg-main");
  const W = main.clientWidth;
  const H = main.clientHeight;
  d3.select("#fg-svg").attr("viewBox", `0 0 ${W} ${H}`);
}

// ── Utilities ─────────────────────────────────────────────────────────────────
function esc(str) {
  return String(str || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function capitalize(str) {
  const s = String(str || "").trim();
  return s ? s[0].toUpperCase() + s.slice(1) : "";
}

function truncate(str, n) {
  return str && str.length > n ? str.slice(0, n - 1) + "…" : str;
}

function row(label, value) {
  return `<div class="fg-detail-row">
    <span class="fg-label">${label}</span>
    <span>${value}</span>
  </div>`;
}
