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

// Maximum allowed distance (in degrees) between consecutive points.
// Anything larger means a bad merge/jump. ~0.001° ≈ 110m
const MAX_POINT_GAP = 0.0015;

// Merge tolerance: endpoints must be within this to chain (~15m)
const CHAIN_TOLERANCE = 0.00015;

function processCity(inputFile, outputFile) {
  const raw = JSON.parse(readFileSync(inputFile, "utf-8"));
  const ways = raw.elements.filter((e) => e.type === "way" && e.geometry);

  console.log(`${inputFile}: ${ways.length} ways found`);

  // Group ways by road name to merge connected segments
  const roadGroups = {};
  for (const way of ways) {
    const name = way.tags?.name || way.tags?.ref || `unnamed_${way.id}`;
    const highway = way.tags?.highway || "primary";
    if (!roadGroups[name]) roadGroups[name] = { ways: [], highway };
    roadGroups[name].ways.push(way);
  }

  console.log(`  ${Object.keys(roadGroups).length} named roads`);

  const trajectories = [];
  let jumpsSplit = 0;

  for (const [name, group] of Object.entries(roadGroups)) {
    const segments = group.ways.map((w) =>
      w.geometry.map((g) => [
        Math.round(g.lon * 100000) / 100000,
        Math.round(g.lat * 100000) / 100000,
      ])
    );

    // Chain segments that share endpoints (tight tolerance)
    const chains = chainSegments(segments);

    for (const chain of chains) {
      if (chain.length < 3) continue;

      // Split chain at any large gaps (bad merges / teleports)
      const subChains = splitAtGaps(chain, MAX_POINT_GAP);
      jumpsSplit += subChains.length - 1;

      for (const sub of subChains) {
        if (sub.length < 3) continue;

        // Douglas-Peucker simplification (preserves road shape)
        const simplified = douglasPeucker(sub, 0.00003); // ~3m tolerance

        if (simplified.length < 3) continue;

        // Color by road type
        let colorIdx;
        if (group.highway === "motorway") colorIdx = 0;
        else if (group.highway === "trunk") colorIdx = 5;
        else colorIdx = trajectories.length % COLORS.length;

        trajectories.push({
          path: simplified,
          color: COLORS[colorIdx],
        });
      }
    }
  }

  console.log(`  ${jumpsSplit} gap-splits removed bad merges`);

  // Create overlapping sub-paths along long roads for denser animation
  const extraTrajectories = [];
  for (const t of trajectories) {
    if (t.path.length >= 6) {
      const len = t.path.length;
      // More sub-paths for longer roads
      const subCount = Math.min(5, Math.max(2, Math.floor(len / 4)));
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
    `  ${allTrajectories.length} trajectories (${trajectories.length} base + ${extraTrajectories.length} extra)`
  );

  const totalPoints = allTrajectories.reduce((s, t) => s + t.path.length, 0);
  console.log(`  ${totalPoints} total points`);

  writeFileSync(outputFile, JSON.stringify(allTrajectories));
  const size = readFileSync(outputFile).length;
  console.log(`  Output: ${outputFile} (${(size / 1024).toFixed(1)} KB)`);
}

// Split a polyline wherever consecutive points are too far apart
function splitAtGaps(chain, maxGap) {
  const result = [];
  let current = [chain[0]];

  for (let i = 1; i < chain.length; i++) {
    const d = dist(chain[i - 1], chain[i]);
    if (d > maxGap) {
      // Gap detected — start a new sub-chain
      if (current.length >= 2) result.push(current);
      current = [chain[i]];
    } else {
      current.push(chain[i]);
    }
  }

  if (current.length >= 2) result.push(current);
  return result;
}

// Douglas-Peucker line simplification (preserves shape better than every-Nth)
function douglasPeucker(points, epsilon) {
  if (points.length <= 2) return points;

  // Find the point with max distance from the line between first and last
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

  const projX = lineStart[0] + t * dx;
  const projY = lineStart[1] + t * dy;

  return dist(point, [projX, projY]);
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

function dist(a, b) {
  const dx = a[0] - b[0];
  const dy = a[1] - b[1];
  return Math.sqrt(dx * dx + dy * dy);
}

// Process both cities
processCity("la-raw.json", "data/la-roads.json");
processCity("porto-raw.json", "data/porto-roads.json");
