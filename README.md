# CityPulse - GPU-Accelerated Urban Traffic Flow

Real-time animated traffic flow visualization using ArcGIS Maps SDK for JavaScript 5.0 with custom WebGL shaders.

**[Live Demo](https://garridolecca.github.io/citypulse-traffic-flow/)**

## Features

- **Custom WebGL 2.0 Rendering** - Animated polyline trails via `BaseLayerViewGL2D` with GLSL vertex/fragment shaders
- **Multi-City Support** - Switch between Porto (Portugal) and Los Angeles (CA)
- **Procedural Trajectory Generation** - Seeded PRNG builds hundreds of realistic routes from a road-network graph
- **Interactive Controls** - Adjust trail speed, length, and line width in real time
- **Additive Blending** - Glowing trail effect with gaussian edge falloff and bright leading heads

## How It Works

### Rendering Pipeline

1. A road-network graph (nodes = intersections, edges = road segments) is defined for each city
2. A seeded random walk algorithm generates 500-600 trajectories through the graph
3. Trajectories are converted to ArcGIS polyline geometries (geographic → Web Mercator)
4. A custom `BaseLayerViewGL2D` subclass triangulates polylines into extruded quads on the GPU
5. A GLSL fragment shader animates a trail head traveling along each polyline using `mod(distance - time * speed, cycle)`
6. Additive blending (`gl.blendFunc(ONE, ONE_MINUS_SRC_ALPHA)`) creates the glow effect

### Shader Animation

The fragment shader creates the moving trail effect:

```glsl
float d = mod(v_distance - u_current_time * u_trail_speed, u_trail_cycle);
float trail = d < u_trail_length ? smoothstep(0.0, 1.0, d / u_trail_length) : 0.0;
float edge = exp(-abs(v_side) * 3.0);
gl_FragColor = v_color * trail * edge;
```

- `v_distance` — cumulative distance along the polyline
- `u_current_time` — `performance.now() / 1000` for continuous animation
- `smoothstep` — smooth fade from tail to head
- `exp(-abs(v_side) * 3.0)` — gaussian falloff from center to edge

## Tech Stack

- [ArcGIS Maps SDK for JavaScript 5.0](https://developers.arcgis.com/javascript/latest/) — Map rendering and spatial utilities
- [gl-matrix](https://glmatrix.net/) — Matrix/vector math for GPU transforms
- WebGL 2.0 — Custom shader pipeline via `BaseLayerViewGL2D`
- Vanilla JavaScript — No build tools required

## Data Sources

| City | Source | Notes |
|------|--------|-------|
| Porto | Procedural graph | 30 nodes, ~60 edges based on real Porto intersections |
| Los Angeles | Procedural graph | 30 nodes, ~60 edges covering Downtown to Santa Monica |

For real trajectory data, the [Porto Taxi Trajectory dataset](https://www.kaggle.com/datasets/crailtap/taxi-trajectory) (1.7M trips with full GPS polylines) can be plugged directly into this visualization.

## Roadmap

- [ ] WebGPU compute shaders for particle advection (100k+ particles)
- [ ] LA Metro GTFS real-time vehicle positions
- [ ] Time-of-day scrubber with real traffic patterns
- [ ] Porto Taxi dataset integration (Kaggle)
- [ ] 3D view with `BaseLayerViewGL3D`

## Local Development

Just serve the `index.html` file:

```bash
# Python
python -m http.server 8080

# Node
npx serve .
```

No build step required — the app uses CDN imports for ArcGIS SDK and gl-matrix.

## License

MIT
