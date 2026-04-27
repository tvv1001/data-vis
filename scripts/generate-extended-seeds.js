#!/usr/bin/env node
import fs from "fs";
import path from "path";

const ROOT = path.resolve(path.dirname(process.argv[1]), "..");
const seedsPath = path.join(
  ROOT,
  "server",
  "data",
  "national",
  "finra-seeds.json",
);
const outPath = path.join(
  ROOT,
  "server",
  "data",
  "national",
  "finra-seeds-extended.json",
);

function letters() {
  const arr = [];
  for (let i = 0; i < 26; i++) arr.push(String.fromCharCode(97 + i));
  return arr;
}

(async () => {
  const existing = JSON.parse(await fs.promises.readFile(seedsPath, "utf8"));
  const single = letters();
  const doubles = [];
  for (const a of single) for (const b of single) doubles.push(a + b);

  // Build list: keep existing, then add single letters and two-letter combos
  const added = [...single, ...doubles];
  const combined = Array.from(new Set([...existing, ...added]));

  await fs.promises.writeFile(
    outPath,
    JSON.stringify(combined, null, 2),
    "utf8",
  );
  console.log("Wrote", combined.length, "seeds to", outPath);
})();
