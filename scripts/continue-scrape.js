#!/usr/bin/env node
// Repeatedly POST to /api/finra/run-scraper until server reports no more seeds.

async function runOnce() {
  const url = "http://localhost:3001/api/finra/run-scraper";
  try {
    const res = await fetch(url, { method: "POST" });
    const text = await res.text();
    // Print SSE data lines for readability
    text.split(/\n/).forEach((line) => {
      if (line.startsWith("data:")) {
        try {
          const obj = JSON.parse(line.slice(5).trim());
          if (obj?.type) console.log(obj.type + ":", obj.data);
          else console.log("data:", obj);
        } catch (e) {
          console.log("data:", line.slice(5).trim());
        }
      }
    });
    if (/Nothing new to scrape/.test(text)) return false;
    return true;
  } catch (e) {
    // Transient network or socket error — log and retry after delay
    console.error("Run failed:", e && e.message ? e.message : e);
    // Wait a bit before next attempt to avoid tight retry loop
    await new Promise((r) => setTimeout(r, 5000));
    return true;
  }
}

(async () => {
  while (true) {
    console.log("--- run", new Date().toISOString(), "---");
    try {
      const cont = await runOnce();
      if (!cont) break;
    } catch (e) {
      console.error("Run failed:", e);
      break;
    }
    // short delay between batches
    await new Promise((r) => setTimeout(r, 1000));
  }
  console.log("All batches complete or stopped.");
})();
