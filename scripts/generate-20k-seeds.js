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
  "finra-seeds-20k.json",
);

function letters() {
  const arr = [];
  for (let i = 0; i < 26; i++) arr.push(String.fromCharCode(97 + i));
  return arr;
}

(async () => {
  const existing = JSON.parse(await fs.promises.readFile(seedsPath, "utf8"));
  const set = new Set(existing.map((s) => s.trim()));

  const single = letters();
  for (const s of single) set.add(s);

  const doubles = [];
  for (const a of single) for (const b of single) doubles.push(a + b);
  for (const s of doubles) {
    if (set.size >= 20000) break;
    set.add(s);
  }

  // add triples only as needed to reach 20k
  if (set.size < 20000) {
    for (const a of single) {
      for (const b of single) {
        for (const c of single) {
          set.add(a + b + c);
          if (set.size >= 20000) break;
        }
        if (set.size >= 20000) break;
      }
      if (set.size >= 20000) break;
    }
  }

  const combined = Array.from(set);
  await fs.promises.writeFile(
    outPath,
    JSON.stringify(combined, null, 2),
    "utf8",
  );
  // also overwrite the primary seeds file so downloader uses it
  await fs.promises.copyFile(outPath, seedsPath);
  console.log("Wrote", combined.length, "seeds to", outPath);
})();
