/**
 * simulation.worker.js  –  Off-main-thread D3 force simulation
 *
 * Inbound messages:
 *   { type: "init",       nodes, links, width, height }
 *   { type: "drag-start", id, neighborIds }
 *   { type: "drag",       id, x, y }
 *   { type: "drag-end" }
 *
 * Outbound messages:
 *   { type: "ready", nodeIds }              – fired once after init (node order)
 *   { type: "tick",  xs: Float64Array, ys: Float64Array }  (transferable)
 *   { type: "end",   xs: Float64Array, ys: Float64Array }  (transferable)
 *
 * Using Float64Array transferables moves ownership to the main thread without
 * copying, so IPC overhead stays low even for 40k+ nodes.
 */

import {
  forceSimulation,
  forceLink,
  forceManyBody,
  forceCenter,
  forceCollide,
} from "d3-force";

let sim = null;
let nodes = null;
let nodeById = null; // Map<id, node> for O(1) drag lookup
let ticker = null;
let tickCount = 0;

const TICK_MS = 16; // ~60 fps interval
const POST_EVERY = 4; // post positions every N ticks to reduce IPC overhead

function snap() {
  const xs = new Float64Array(nodes.length);
  const ys = new Float64Array(nodes.length);
  for (let i = 0; i < nodes.length; i++) {
    xs[i] = nodes[i].x ?? 0;
    ys[i] = nodes[i].y ?? 0;
  }
  return { xs, ys };
}

function startTicker() {
  if (ticker) return; // already running
  ticker = setInterval(() => {
    if (!sim) {
      clearInterval(ticker);
      ticker = null;
      return;
    }
    sim.tick();
    tickCount++;

    if (tickCount % POST_EVERY === 0) {
      const { xs, ys } = snap();
      self.postMessage({ type: "tick", xs, ys }, [xs.buffer, ys.buffer]);
    }

    if (sim.alpha() < sim.alphaMin()) {
      clearInterval(ticker);
      ticker = null;
      // Freeze all node positions
      nodes.forEach((n) => {
        n.fx = n.x;
        n.fy = n.y;
      });
      const { xs, ys } = snap();
      self.postMessage({ type: "end", xs, ys }, [xs.buffer, ys.buffer]);
    }
  }, TICK_MS);
}

self.onmessage = ({ data }) => {
  // ── Init ─────────────────────────────────────────────────────────────────
  if (data.type === "init") {
    // Stop any existing simulation
    if (ticker) {
      clearInterval(ticker);
      ticker = null;
    }
    tickCount = 0;

    nodes = data.nodes.map((n) => ({ ...n }));
    nodeById = new Map(nodes.map((n) => [n.id, n]));

    const links = data.links.map((l) => ({
      source: l.source,
      target: l.target,
    }));

    const { width, height } = data;

    sim = forceSimulation(nodes)
      .alphaDecay(0.05)
      .force(
        "link",
        forceLink(links)
          .id((d) => d.id)
          .distance(180),
      )
      .force("charge", forceManyBody().strength(-700))
      .force("center", forceCenter(width / 2, height / 2))
      .force(
        "collision",
        forceCollide()
          .radius((d) => (d._vizHalf ?? 10) + 28)
          .strength(0.9),
      )
      .stop();

    // Tell the main thread the index order so it can map Float64Array slots → nodes
    self.postMessage({ type: "ready", nodeIds: nodes.map((n) => n.id) });
    startTicker();
    return;
  }

  // ── Drag start ────────────────────────────────────────────────────────────
  if (data.type === "drag-start") {
    const { id, neighborIds } = data;
    const neighborSet = new Set(neighborIds);
    nodes.forEach((n) => {
      if (n.id === id) {
        n.fx = n.x;
        n.fy = n.y;
      } else if (neighborSet.has(n.id)) {
        n.fx = null;
        n.fy = null;
      }
    });
    if (sim) sim.alphaTarget(0.3);
    startTicker();
    return;
  }

  // ── Drag move ─────────────────────────────────────────────────────────────
  if (data.type === "drag") {
    const n = nodeById?.get(data.id);
    if (n) {
      n.fx = data.x;
      n.fy = data.y;
    }
    return;
  }

  // ── Drag end ──────────────────────────────────────────────────────────────
  if (data.type === "drag-end") {
    if (sim) sim.alphaTarget(0);
  }
};
