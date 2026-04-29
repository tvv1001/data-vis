/**
 * finra.js  –  FINRA BrokerCheck Network Graph
 *
 * Renders the finra-graph.json as an interactive Sigma.js (WebGL) graph
 * backed by Graphology, replacing the previous D3 SVG renderer.
 *
 * Nodes:
 *   individual  – blue circles  (people discovered from seed search)
 *   firm        – amber circles (registered broker-dealer / IA firms)
 *   entity      – grey circles  (non-individual Form BD control owners)
 *
 * Edges:
 *   employed_by – grey arrow  (person → firm, with date range in sidebar)
 *   controls    – red arrow   (person/entity → firm, from Form BD directOwners)
 */

import "./finra.css";
import Graph from "graphology";
import Sigma from "sigma";
import { NodeCircleProgram, EdgeArrowProgram } from "sigma/rendering";
import { circular } from "graphology-layout";
import FA2Layout from "graphology-layout-forceatlas2/worker";
import forceAtlas2 from "graphology-layout-forceatlas2";

const BASE = import.meta.env.VITE_API_URL || "http://localhost:3001";

// ── State ──────────────────────────────────────────────────────────────────
let graphData = null; // { nodes, links, meta } – raw API response
let graph = null; // graphology Graph instance
let sigmaInstance = null;
let fa2Layout = null;
let selectedId = null;

// Visual state – read by sigma reducers every render frame
const filterState = {
  active: false,
  matched: new Set(), // node keys that directly match the search query
  expanded: new Set(), // matched + their direct neighbours (fully visible)
};
const highlightState = {
  active: false,
  selectedNode: null, // graphology node key of selected node
  neighbors: new Set(), // direct neighbours of selectedNode
};

// Drag state
let isDragging = false;
let draggedNode = null;

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
  if (!graph || !sigmaInstance) return;

  if (!q) {
    // Reset filter state – reducers will clear on next refresh
    filterState.active = false;
    filterState.matched = new Set();
    filterState.expanded = new Set();
    sigmaInstance.refresh();
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

  // Determine matching node keys
  const matched = new Set();
  graph.forEachNode((nodeKey, attrs) => {
    // gather candidate values
    const label = String(
      firstField(attrs, ["label", "firm_name", "firmName"]) || "",
    );
    const labelLow = label.toLowerCase();

    const crd = String(
      firstField(attrs, ["crd", "ind_source_id", "ind_crd"]) || "",
    );
    const bdSec = String(
      firstField(attrs, [
        "bdSecNumber",
        "bd_sec_number",
        "firm_bd_sec_number",
      ]) || "",
    );
    const bdFull = String(firstField(attrs, ["firm_bd_full_sec_number"]) || "");
    const firmSrc = String(
      firstField(attrs, ["firm_source_id", "firm_id"]) || "",
    );

    // person name pieces
    const fname = String(firstField(attrs, ["ind_firstname"]) || "");
    const mname = String(firstField(attrs, ["ind_middlename"]) || "");
    const lname = String(firstField(attrs, ["ind_lastname"]) || "");
    const personFull = [fname, mname, lname].filter(Boolean).join(" ");

    // firm address (may be stored as JSON string)
    let addrObj = null;
    const addrRaw = firstField(attrs, [
      "firm_address_details",
      "address_details",
    ]);
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
      if (
        normalizeDigits(crd) === qDigits ||
        normalizeDigits(firmSrc) === qDigits
      ) {
        matched.add(nodeKey);
        return;
      }
      if (bdFull && bdFull.toLowerCase() === q.toLowerCase()) {
        matched.add(nodeKey);
        return;
      }
      if (normalizeDigits(bdSec) === qDigits) {
        matched.add(nodeKey);
        return;
      }
      const src = attrs._source || {};
      if (src.ind_source_id && normalizeDigits(src.ind_source_id) === qDigits) {
        matched.add(nodeKey);
        return;
      }
      if (
        src.firm_bd_full_sec_number &&
        String(src.firm_bd_full_sec_number).toLowerCase() === q.toLowerCase()
      ) {
        matched.add(nodeKey);
        return;
      }
      return;
    }

    // Non-exact: loose matching for main name/firm only
    const ql = qlow;
    if (labelLow.includes(ql) || personFull.toLowerCase().includes(ql)) {
      matched.add(nodeKey);
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
        matched.add(nodeKey);
        return;
      }
    }

    // employment branch match for individuals
    const emp = firstField(attrs, [
      "ind_current_employments",
      "ind_employments",
    ]);
    if (Array.isArray(emp)) {
      for (const e of emp) {
        const city = String(e.branch_city || e.city || "").toLowerCase();
        const state = String(e.branch_state || e.state || "").toLowerCase();
        const zip = String(e.branch_zip || e.postalCode || "").toLowerCase();
        if (city.includes(ql) || state.includes(ql) || zip.includes(ql)) {
          matched.add(nodeKey);
          return;
        }
      }
    }
  });

  // Include direct neighbours of matched nodes for context
  const expanded = new Set(matched);
  matched.forEach((key) => {
    graph.neighbors(key).forEach((nb) => expanded.add(nb));
  });

  filterState.active = true;
  filterState.matched = matched;
  filterState.expanded = expanded;
  sigmaInstance.refresh();
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
  const container = document.getElementById("fg-container");
  if (container) container.style.visibility = show ? "hidden" : "visible";
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

// ── Sigma/Graphology Rendering ───────────────────────────────────────────────

// Base node colours (resolved CSS values, not var() references – sigma WebGL
// cannot resolve CSS custom properties)
const NODE_COLOR = {
  individual: "#2563eb",
  firm: "#d97706",
  entity: "#6b7280",
};
const EDGE_COLOR = {
  employed_by: "#94a3b8",
  controls: "#ef4444",
};

function renderGraph(data) {
  // ── Teardown ──────────────────────────────────────────────────────────────
  if (fa2Layout) {
    fa2Layout.kill();
    fa2Layout = null;
  }
  if (sigmaInstance) {
    sigmaInstance.kill();
    sigmaInstance = null;
  }

  // Reset visual state
  filterState.active = false;
  filterState.matched = new Set();
  filterState.expanded = new Set();
  highlightState.active = false;
  highlightState.selectedNode = null;
  highlightState.neighbors = new Set();
  selectedId = null;

  // ── Build Graphology graph ────────────────────────────────────────────────
  graph = new Graph({ multi: true, allowSelfLoops: false });

  // Per-node degree stats for size scaling
  const degMap = new Map();
  data.nodes.forEach((n) =>
    degMap.set(n.id, { total: 0, controls: 0, employed: 0 }),
  );
  data.links.forEach((l) => {
    const srcId = l.source?.id ?? l.source;
    const tgtId = l.target?.id ?? l.target;
    for (const id of [srcId, tgtId]) {
      const e = degMap.get(id);
      if (!e) continue;
      e.total++;
      if (l.relationship === "controls") e.controls++;
      else e.employed++;
    }
  });

  const maxFirmDeg = Math.max(
    1,
    ...data.nodes
      .filter((n) => n.group === "firm")
      .map((n) => degMap.get(n.id)?.total || 0),
  );
  const maxIndDeg = Math.max(
    1,
    ...data.nodes
      .filter((n) => n.group === "individual")
      .map((n) => degMap.get(n.id)?.total || 0),
  );

  // Add nodes
  data.nodes.forEach((n) => {
    if (graph.hasNode(n.id)) return; // skip duplicates
    const deg = degMap.get(n.id) || { total: 0, controls: 0, employed: 0 };

    let size;
    if (n.group === "firm") {
      size = 6 * (1 + (Math.sqrt(deg.total) / Math.sqrt(maxFirmDeg)) * 1.5);
    } else if (n.group === "individual") {
      size = 4 * (1 + (Math.sqrt(deg.total) / Math.sqrt(maxIndDeg)) * 2.0);
    } else {
      size = 4; // entity
    }
    size = Math.max(2, Math.min(size, 22));

    const color = NODE_COLOR[n.group] || "#6b7280";

    // Spread all original node data into attributes so sidebar + filter
    // functions can access every field (crd, ind_firstname, etc.)
    graph.addNode(n.id, {
      ...n,
      // Override sigma visual attrs after the spread
      label: capitalize(n.label),
      x: Math.random() * 1000 - 500,
      y: Math.random() * 1000 - 500,
      size,
      color,
      _baseSize: size,
      _baseColor: color,
      _deg: deg,
    });
  });

  // Add edges (graphology multi-graph allows parallel edges)
  data.links.forEach((l) => {
    const srcId = l.source?.id ?? l.source;
    const tgtId = l.target?.id ?? l.target;
    if (!graph.hasNode(srcId) || !graph.hasNode(tgtId)) return;
    const color = EDGE_COLOR[l.relationship] || "#94a3b8";
    try {
      graph.addEdge(srcId, tgtId, {
        ...l,
        color,
        size: l.relationship === "controls" ? 1.5 : 1,
        _baseColor: color,
        _baseSize: l.relationship === "controls" ? 1.5 : 1,
      });
    } catch (_) {
      /* skip edges that fail (e.g. self-loop on no-selfloop graph) */
    }
  });

  // Initial layout: place nodes in a circle so FA2 has a clean starting point
  circular.assign(graph);

  // ── Sigma renderer ────────────────────────────────────────────────────────
  const container = document.getElementById("fg-container");

  sigmaInstance = new Sigma(graph, container, {
    nodeProgramClasses: { circle: NodeCircleProgram },
    edgeProgramClasses: { arrow: EdgeArrowProgram },
    defaultNodeType: "circle",
    defaultEdgeType: "arrow",
    renderEdgeLabels: false,
    labelFont: "Manrope, system-ui, sans-serif",
    labelSize: 10,
    labelWeight: "500",
    labelColor: { color: "#1e293b" },
    labelRenderedSizeThreshold: 6, // only show labels when node is ≥6px on screen

    // ── Node reducer: called every frame to compute display attributes ──────
    nodeReducer(nodeKey, data) {
      const res = { ...data };

      // Selection highlight: dim everything except the selected node and its
      // immediate neighbours.
      if (highlightState.active) {
        if (
          nodeKey !== highlightState.selectedNode &&
          !highlightState.neighbors.has(nodeKey)
        ) {
          res.color = "#d1d5db";
          res.size = Math.max(1, data._baseSize * 0.5);
          res.label = "";
        } else if (nodeKey === highlightState.selectedNode) {
          res.size = data._baseSize * 1.3;
        }
      }

      // Search filter: dim nodes outside the matched+expanded set.
      if (filterState.active) {
        if (!filterState.expanded.has(nodeKey)) {
          res.color = "#e5e7eb";
          res.size = Math.max(1, data._baseSize * 0.3);
          res.label = "";
        }
      }

      // Disclosure indicator: tint orange border by making the node slightly
      // lighter so the orange label colour stands out (full ring needs a
      // compound program; this is a lightweight fallback).
      if (data.disclosureCount > 0 && res.color === data._baseColor) {
        res.color = data.group === "individual" ? "#3b82f6" : res.color;
      }

      return res;
    },

    // ── Edge reducer ─────────────────────────────────────────────────────────
    edgeReducer(edgeKey, data) {
      const res = { ...data };
      const src = graph.source(edgeKey);
      const tgt = graph.target(edgeKey);

      // Selection highlight
      if (highlightState.active) {
        if (
          src === highlightState.selectedNode ||
          tgt === highlightState.selectedNode
        ) {
          res.color = data.relationship === "controls" ? "#ff2222" : "#38bdf8";
          res.size = data.relationship === "controls" ? 2.5 : 2;
        } else {
          res.color = "#e5e7eb";
          res.size = 0.5;
        }
      }

      // Search filter
      if (filterState.active) {
        const srcExpanded = filterState.expanded.has(src);
        const tgtExpanded = filterState.expanded.has(tgt);
        if (!srcExpanded && !tgtExpanded) {
          res.hidden = true;
        } else if (
          !filterState.matched.has(src) &&
          !filterState.matched.has(tgt)
        ) {
          res.color = "#d1d5db";
          res.size = 0.5;
        }
      }

      return res;
    },
  });

  // ── Events ────────────────────────────────────────────────────────────────
  sigmaInstance.on("clickNode", ({ node }) => {
    if (isDragging) return;
    const attrs = graph.getNodeAttributes(node);
    selectedId = attrs.id ?? node;
    highlightState.active = true;
    highlightState.selectedNode = node;
    highlightState.neighbors = new Set(graph.neighbors(node));
    renderSidebar(attrs);
    sigmaInstance.refresh();
    spreadNeighbors(node);
  });

  sigmaInstance.on("clickStage", () => {
    if (isDragging) return;
    selectedId = null;
    highlightState.active = false;
    highlightState.selectedNode = null;
    highlightState.neighbors = new Set();
    showSidebarHint();
    sigmaInstance.refresh();
  });

  // ── Drag ──────────────────────────────────────────────────────────────────
  setupSigmaDrag(sigmaInstance);

  // ── ForceAtlas2 layout ────────────────────────────────────────────────────
  const nodeCount = graph.order;
  const fa2Settings = forceAtlas2.inferSettings(graph);

  if (nodeCount > 1500) {
    // Worker-based async layout for large graphs – keeps the UI responsive
    fa2Layout = new FA2Layout(graph, {
      settings: { ...fa2Settings, barnesHutOptimize: true },
    });
    fa2Layout.start();
    // Stop after 5 s; user can still drag nodes freely after that
    setTimeout(() => fa2Layout?.stop(), 5000);
  } else {
    // Synchronous for small/demo graphs
    forceAtlas2.assign(graph, { iterations: 150, settings: fa2Settings });
  }
}

// ── Sigma drag ───────────────────────────────────────────────────────────────
function setupSigmaDrag(renderer) {
  isDragging = false;
  draggedNode = null;

  renderer.on("downNode", ({ node }) => {
    // Stop the layout so manual drag positions aren't immediately overridden
    if (fa2Layout) fa2Layout.stop();
    isDragging = true;
    draggedNode = node;
  });

  renderer.getMouseCaptor().on("mousemovebody", (e) => {
    if (!isDragging || !draggedNode) return;
    const pos = renderer.viewportToGraph(e);
    graph.setNodeAttribute(draggedNode, "x", pos.x);
    graph.setNodeAttribute(draggedNode, "y", pos.y);
    e.preventSigmaDefault();
    e.original.preventDefault();
    e.original.stopPropagation();
  });

  renderer.getMouseCaptor().on("mouseup", () => {
    // Small delay so clickStage doesn't fire right after drag ends
    setTimeout(() => {
      isDragging = false;
      draggedNode = null;
    }, 50);
  });
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// Returns a Set of neighbour node keys for the given node key.
function getNeighborIds(nodeKey) {
  if (!graph || !graph.hasNode(nodeKey)) return new Set();
  return new Set(graph.neighbors(nodeKey));
}

// ── Selection & Sidebar ───────────────────────────────────────────────────────
function selectNode(d) {
  // Called externally (e.g. from addPersonToSeeds reload path).
  // The main selection path is the clickNode event in renderGraph.
  if (!sigmaInstance || !graph) return;
  const nodeKey = d.id ?? d;
  if (!graph.hasNode(nodeKey)) return;
  const attrs = graph.getNodeAttributes(nodeKey);
  selectedId = attrs.id ?? nodeKey;
  highlightState.active = true;
  highlightState.selectedNode = nodeKey;
  highlightState.neighbors = new Set(graph.neighbors(nodeKey));
  renderSidebar(attrs);
  sigmaInstance.refresh();
}

function showSidebarHint() {
  document.getElementById("fg-sidebar-inner").innerHTML =
    `<p class="fg-hint">Click a node to inspect it.</p>`;
}

// ── Edge highlight (thin wrapper kept for API compat) ────────────────────────
function highlightLinks(activeId) {
  if (!sigmaInstance) return;
  if (activeId == null) {
    highlightState.active = false;
    highlightState.selectedNode = null;
    highlightState.neighbors = new Set();
  } else {
    highlightState.active = true;
    highlightState.selectedNode = activeId;
    highlightState.neighbors = new Set(
      graph?.hasNode(activeId) ? graph.neighbors(activeId) : [],
    );
  }
  sigmaInstance.refresh();
}

// ── Spread neighbours on node click ──────────────────────────────────────────
function spreadNeighbors(clickedNodeKey) {
  if (!graph || !sigmaInstance) return;

  const clickedAttrs = graph.getNodeAttributes(clickedNodeKey);
  const cx = clickedAttrs.x;
  const cy = clickedAttrs.y;

  const SPREAD = 80; // graph-coordinate units
  const DURATION = 480; // ms

  const neighborKeys = graph.neighbors(clickedNodeKey);
  if (neighborKeys.length === 0) return;

  // Snapshot each neighbour's starting position and compute target
  const snapshots = new Map();
  neighborKeys.forEach((key) => {
    const a = graph.getNodeAttributes(key);
    const dx = a.x - cx;
    const dy = a.y - cy;
    const dist = Math.sqrt(dx * dx + dy * dy) || 1;
    snapshots.set(key, {
      x0: a.x,
      y0: a.y,
      x1: a.x + (dx / dist) * SPREAD,
      y1: a.y + (dy / dist) * SPREAD,
    });
  });

  const startTime = performance.now();

  function frame(now) {
    const raw = Math.min((now - startTime) / DURATION, 1);
    const ease = 1 - Math.pow(1 - raw, 3); // cubic ease-out
    snapshots.forEach((snap, key) => {
      graph.setNodeAttribute(key, "x", snap.x0 + (snap.x1 - snap.x0) * ease);
      graph.setNodeAttribute(key, "y", snap.y0 + (snap.y1 - snap.y0) * ease);
    });
    if (raw < 1) requestAnimationFrame(frame);
  }

  requestAnimationFrame(frame);
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
  // Sigma listens to ResizeObserver internally; a manual refresh ensures
  // the camera recalculates after any layout shifts.
  if (sigmaInstance) sigmaInstance.refresh();
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
