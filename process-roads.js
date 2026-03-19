// Process Overpass API road data into compact trajectory files
// Run with: node process-roads.js

import { readFileSync, writeFileSync } from "fs";

const COLORS = [
  [0, 238, 255],   // electric cyan
  [255, 50, 120],  // hot pink
  [255, 200, 0],   // gold
  [0, 255, 170],   // spring green
  [140, 80, 255],  // purple
  [255, 120, 0],   // orange
  [0, 191, 255],   // sky blue
  [255, 70, 70],   // coral
  [80, 255, 80],   // neon green
  [255, 160, 240], // pink
  [0, 200, 200],   // teal
  [255, 255, 100], // yellow
];

// Chain tolerance: ~11m — endpoints must be very close
const CHAIN_TOLERANCE = 0.0001;

// Max gap between consecutive points before splitting: ~55m
const MAX_GAP = 0.0005;

// Min path length in degrees to keep (~110m)
const MIN_PATH_LENGTH = 0.001;

function processCity(inputFile, outputFile) {
  const raw = JSON.parse(readFileSync(inputFile, "utf-8"));
  const ways = raw.elements.filter((e) => e.type === "way" && e.geometry);

  console.log(`${inputFile}: ${ways.length} total ways`);

  // Filter out _link (ramps) — shouldn't be present with exact queries,
  // but guard just in case
  const filtered = ways.filter((w) => {
    const hw = w.tags?.highway || "";
    return !hw.includes("_link");
  });

  console.log(`  ${filtered.length} ways after removing _link`);

  // Group by road name for chaining
  const roadGroups = {};
  for (const way of filtered) {
    // Use unique ID for unnamed roads — they won't merge with anything
    const name = way.tags?.name || way.tags?.ref || `_u${way.id}`;
    const highway = way.tags?.highway || "primary";
    if (!roadGroups[name]) roadGroups[name] = { ways: [], highway };
    roadGroups[name].ways.push(way);
  }

  const trajectories = [];
  let gapSplits = 0;
  let tooShort = 0;

  for (const [name, group] of Object.entries(roadGroups)) {
    const segments = group.ways.map((w) =>
      w.geometry.map((g) => [
        Math.round(g.lon * 100000) / 100000,
        Math.round(g.lat * 100000) / 100000,
      ])
    );

    // Chain segments with very tight tolerance (essentially touching)
    const chains = chainSegments(segments);

    for (const chain of chains) {
      if (chain.length < 2) continue;

      // Split at any gaps > 55m (catches bad merges)
      const subChains = splitAtGaps(chain, MAX_GAP);
      if (subChains.length > 1) gapSplits += subChains.length - 1;

      for (const sub of subChains) {
        if (sub.length < 2) continue;

        // Douglas-Peucker simplification (~1.5m tolerance)
        const simplified = douglasPeucker(sub, 0.000015);
        if (simplified.length < 2) continue;

        // Filter out very short paths
        const pathLen = pathLength(simplified);
        if (pathLen < MIN_PATH_LENGTH) {
          tooShort++;
          continue;
        }

        let colorIdx;
        if (group.highway === "motorway") colorIdx = 0;
        else if (group.highway === "trunk") colorIdx = 5;
        else if (group.highway === "primary") colorIdx = trajectories.length % COLORS.length;
        else colorIdx = (trajectories.length * 3 + 1) % COLORS.length;

        trajectories.push({
          path: simplified,
          color: COLORS[colorIdx],
        });
      }
    }
  }

  console.log(`  ${gapSplits} gap-splits, ${tooShort} too-short filtered`);
  console.log(`  ${trajectories.length} base trajectories`);

  // Sub-paths for denser animation on longer roads
  const extraTrajectories = [];
  for (const t of trajectories) {
    if (t.path.length >= 6) {
      const len = t.path.length;
      const subCount = Math.min(4, Math.max(1, Math.floor(len / 5)));
      for (let i = 0; i < subCount; i++) {
        const start = Math.floor((i * len) / (subCount + 1));
        const end = Math.min(len, start + Math.floor(len * 0.5));
        if (end - start >= 3) {
          const altColorIdx = (COLORS.indexOf(t.color) + i + 2) % COLORS.length;
          extraTrajectories.push({
            path: t.path.slice(start, end),
            color: COLORS[altColorIdx],
          });
        }
      }
    }
  }

  const allTrajectories = [...trajectories, ...extraTrajectories];

  console.log(
    `  ${allTrajectories.length} total (${trajectories.length} base + ${extraTrajectories.length} sub-paths)`
  );

  const totalPoints = allTrajectories.reduce((s, t) => s + t.path.length, 0);
  const avgPts = (totalPoints / allTrajectories.length).toFixed(1);
  console.log(`  ${totalPoints} total points (avg ${avgPts} pts/trajectory)`);

  writeFileSync(outputFile, JSON.stringify(allTrajectories));
  const size = readFileSync(outputFile).length;
  console.log(`  Output: ${outputFile} (${(size / 1024).toFixed(1)} KB)\n`);
}

function splitAtGaps(chain, maxGap) {
  const result = [];
  let current = [chain[0]];

  for (let i = 1; i < chain.length; i++) {
    const d = dist(chain[i - 1], chain[i]);
    if (d > maxGap) {
      if (current.length >= 2) result.push(current);
      current = [chain[i]];
    } else {
      current.push(chain[i]);
    }
  }

  if (current.length >= 2) result.push(current);
  return result.length > 0 ? result : [[...chain]];
}

function chainSegments(segments) {
  if (segments.length === 0) return [];
  if (segments.length === 1) return segments;

  const chains = segments.map((s) => [...s]);
  let merged = true;

  while (merged) {
    merged = false;
    for (let i = 0; i < chains.length; i++) {
      if (!chains[i]) continue;
      for (let j = i + 1; j < chains.length; j++) {
        if (!chains[j]) continue;

        const aFirst = chains[i][0];
        const aLast = chains[i][chains[i].length - 1];
        const bFirst = chains[j][0];
        const bLast = chains[j][chains[j].length - 1];

        let joinResult = null;

        if (dist(aLast, bFirst) < CHAIN_TOLERANCE) {
          joinResult = [...chains[i], ...chains[j].slice(1)];
        } else if (dist(aLast, bLast) < CHAIN_TOLERANCE) {
          joinResult = [...chains[i], ...chains[j].reverse().slice(1)];
        } else if (dist(aFirst, bLast) < CHAIN_TOLERANCE) {
          joinResult = [...chains[j], ...chains[i].slice(1)];
        } else if (dist(aFirst, bFirst) < CHAIN_TOLERANCE) {
          joinResult = [...chains[j].reverse(), ...chains[i].slice(1)];
        }

        if (joinResult) {
          chains[i] = joinResult;
          chains[j] = null;
          merged = true;
        }
      }
    }
  }

  return chains.filter(Boolean);
}

function douglasPeucker(points, epsilon) {
  if (points.length <= 2) return points;

  let maxDist = 0;
  let maxIdx = 0;
  const first = points[0];
  const last = points[points.length - 1];

  for (let i = 1; i < points.length - 1; i++) {
    const d = perpendicularDist(points[i], first, last);
    if (d > maxDist) {
      maxDist = d;
      maxIdx = i;
    }
  }

  if (maxDist > epsilon) {
    const left = douglasPeucker(points.slice(0, maxIdx + 1), epsilon);
    const right = douglasPeucker(points.slice(maxIdx), epsilon);
    return [...left.slice(0, -1), ...right];
  } else {
    return [first, last];
  }
}

function perpendicularDist(point, lineStart, lineEnd) {
  const dx = lineEnd[0] - lineStart[0];
  const dy = lineEnd[1] - lineStart[1];
  const lenSq = dx * dx + dy * dy;

  if (lenSq === 0) return dist(point, lineStart);

  let t = ((point[0] - lineStart[0]) * dx + (point[1] - lineStart[1]) * dy) / lenSq;
  t = Math.max(0, Math.min(1, t));

  return dist(point, [lineStart[0] + t * dx, lineStart[1] + t * dy]);
}

function pathLength(points) {
  let len = 0;
  for (let i = 1; i < points.length; i++) {
    len += dist(points[i - 1], points[i]);
  }
  return len;
}

function dist(a, b) {
  const dx = a[0] - b[0];
  const dy = a[1] - b[1];
  return Math.sqrt(dx * dx + dy * dy);
}

processCity("la-raw.json", "data/la-roads.json");
processCity("porto-raw.json", "data/porto-roads.json");
