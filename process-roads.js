// Process Overpass API road data into clean trajectory files
// NO name-based merging — each OSM way is independent (no bad jumps)
// Douglas-Peucker simplification (preserves road curves, unlike every-Nth)
// Run with: node process-roads.js

import { readFileSync, writeFileSync } from "fs";

const COLORS = [
  [0, 238, 255], [255, 50, 120], [255, 200, 0], [0, 255, 170],
  [140, 80, 255], [255, 120, 0], [0, 191, 255], [255, 70, 70],
  [80, 255, 80], [255, 160, 240], [0, 200, 200], [255, 255, 100],
];

function processCity(inputFile, outputFile) {
  const raw = JSON.parse(readFileSync(inputFile, "utf-8"));
  const ways = raw.elements.filter((e) => e.type === "way" && e.geometry && e.geometry.length >= 2);
  console.log(`${inputFile}: ${ways.length} ways`);

  const trajectories = [];

  for (const way of ways) {
    // Extract and deduplicate consecutive points
    const points = [];
    for (const g of way.geometry) {
      const p = [Math.round(g.lon * 100000) / 100000, Math.round(g.lat * 100000) / 100000];
      if (points.length === 0 || Math.abs(p[0] - points[points.length - 1][0]) > 0.000001 || Math.abs(p[1] - points[points.length - 1][1]) > 0.000001) {
        points.push(p);
      }
    }

    if (points.length < 2) continue;

    // Douglas-Peucker simplification (~2m tolerance, preserves curves)
    const simplified = douglasPeucker(points, 0.00002);
    if (simplified.length < 2) continue;

    // Min path length filter (~80m)
    if (pathLength(simplified) < 0.0007) continue;

    const highway = way.tags?.highway || "primary";
    let colorIdx;
    if (highway === "motorway") colorIdx = 0;
    else if (highway === "trunk") colorIdx = 5;
    else colorIdx = trajectories.length % COLORS.length;

    trajectories.push({ path: simplified, color: COLORS[colorIdx] });
  }

  console.log(`  ${trajectories.length} trajectories`);

  // Add sub-paths for longer roads (denser animation)
  const extras = [];
  for (const t of trajectories) {
    if (t.path.length >= 8) {
      const len = t.path.length;
      const n = Math.min(3, Math.floor(len / 5));
      for (let i = 0; i < n; i++) {
        const s = Math.floor((i * len) / (n + 1));
        const e = Math.min(len, s + Math.floor(len * 0.5));
        if (e - s >= 3) {
          extras.push({ path: t.path.slice(s, e), color: COLORS[(COLORS.indexOf(t.color) + i + 2) % COLORS.length] });
        }
      }
    }
  }

  const all = [...trajectories, ...extras];
  const totalPts = all.reduce((s, t) => s + t.path.length, 0);
  console.log(`  ${all.length} total (${trajectories.length} + ${extras.length} sub-paths), ${totalPts} points`);

  writeFileSync(outputFile, JSON.stringify(all));
  console.log(`  Output: ${(readFileSync(outputFile).length / 1024).toFixed(1)} KB\n`);
}

function douglasPeucker(pts, eps) {
  if (pts.length <= 2) return pts;
  let maxD = 0, maxI = 0;
  for (let i = 1; i < pts.length - 1; i++) {
    const d = perpDist(pts[i], pts[0], pts[pts.length - 1]);
    if (d > maxD) { maxD = d; maxI = i; }
  }
  if (maxD > eps) {
    const l = douglasPeucker(pts.slice(0, maxI + 1), eps);
    const r = douglasPeucker(pts.slice(maxI), eps);
    return [...l.slice(0, -1), ...r];
  }
  return [pts[0], pts[pts.length - 1]];
}

function perpDist(p, a, b) {
  const dx = b[0] - a[0], dy = b[1] - a[1], lenSq = dx * dx + dy * dy;
  if (lenSq === 0) return dist(p, a);
  const t = Math.max(0, Math.min(1, ((p[0] - a[0]) * dx + (p[1] - a[1]) * dy) / lenSq));
  return dist(p, [a[0] + t * dx, a[1] + t * dy]);
}

function pathLength(pts) {
  let l = 0;
  for (let i = 1; i < pts.length; i++) l += dist(pts[i - 1], pts[i]);
  return l;
}

function dist(a, b) {
  const dx = a[0] - b[0], dy = a[1] - b[1];
  return Math.sqrt(dx * dx + dy * dy);
}

processCity("la-raw.json", "data/la-roads.json");
processCity("porto-raw.json", "data/porto-roads.json");
