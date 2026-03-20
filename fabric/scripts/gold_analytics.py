# Gold Analytics - Vision Zero LA
# Hot spots, H3 heatmap, danger corridors, stats, time series

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import json
import math
import h3
import os

crashes = spark.table('silver_crashes')
print('Silver rows:', crashes.count())

# ============================================================
# 1. GeoAnalytics: Hot Spot Analysis + Emerging Hot Spots
# ============================================================
try:
    from geoanalytics_fabric.sql import functions as GA

    # Ensure geometry column exists
    if 'geometry' not in crashes.columns:
        crashes = crashes.withColumn('geometry', GA.ST_Point('longitude', 'latitude', 4326))

    # Hot Spot Analysis (Getis-Ord Gi*)
    print('Running Hot Spot Analysis...')
    hotspots = GA.find_hot_spots(
        input_layer=crashes,
        bin_type='hexagon',
        bin_size=200,
        bin_size_unit='meters',
        neighborhood_size=3
    )
    hotspots.write.format('delta').mode('overwrite').saveAsTable('gold_hotspots')
    print('Hot spots:', hotspots.count(), 'bins')

    # Space-Time Emerging Hot Spots
    print('Running Emerging Hot Spot Analysis...')
    try:
        emerging = GA.find_space_time_hot_spots(
            input_layer=crashes,
            time_field='crash_date',
            bin_type='hexagon',
            bin_size=200,
            bin_size_unit='meters',
            time_step_interval=3,
            time_step_interval_unit='months',
            neighborhood_size=3
        )
        emerging.write.format('delta').mode('overwrite').saveAsTable('gold_emerging_hotspots')
        print('Emerging hot spots:', emerging.count(), 'bins')
    except Exception as e:
        print('Emerging hot spots skipped:', e)

    # Kernel Density
    print('Running Kernel Density...')
    try:
        density = GA.calculate_density(
            input_layer=crashes,
            search_radius=500,
            search_radius_unit='meters',
            output_cell_size=100,
            output_cell_size_unit='meters'
        )
        density.write.format('delta').mode('overwrite').saveAsTable('gold_crash_density')
        print('Density surface created')
    except Exception as e:
        print('Kernel density skipped:', e)

except ImportError:
    print('GeoAnalytics not available - skipping advanced spatial analytics')

# ============================================================
# 2. H3 Heatmap Aggregation
# ============================================================
print('Building H3 heatmap...')

@F.udf(StringType())
def h3_boundary(h3_index):
    try:
        boundary = h3.cell_to_boundary(h3_index)
        coords = [[round(lng, 6), round(lat, 6)] for lat, lng in boundary]
        coords.append(coords[0])
        return json.dumps(coords)
    except:
        return None

for res_col, res_label in [('h3_res9', '9'), ('h3_res8', '8')]:
    heatmap = (crashes
        .groupBy(res_col)
        .agg(
            F.count('*').alias('crash_count'),
            F.sum(F.when(F.col('severity') == 'fatal', 5)
                  .when(F.col('severity') == 'pedestrian', 4)
                  .when(F.col('severity') == 'bicycle', 4)
                  .when(F.col('severity') == 'severe', 3)
                  .otherwise(1)).alias('severity_score'),
            F.sum(F.when(F.col('severity') == 'fatal', 1).otherwise(0)).alias('fatal_count'),
            F.sum(F.when(F.col('severity') == 'pedestrian', 1).otherwise(0)).alias('ped_count'),
            F.sum(F.when(F.col('severity') == 'bicycle', 1).otherwise(0)).alias('bike_count'),
            F.avg('latitude').alias('center_lat'),
            F.avg('longitude').alias('center_lng'),
        )
        .withColumn('boundary', h3_boundary(F.col(res_col)))
        .withColumnRenamed(res_col, 'h3_index')
        .withColumn('resolution', F.lit(res_label))
        .filter(F.col('boundary').isNotNull())
    )

    mode = 'overwrite' if res_label == '9' else 'append'
    heatmap.write.format('delta').mode(mode).saveAsTable('gold_h3_heatmap')
    print(f'H3 res {res_label}:', heatmap.count(), 'hexagons')

# ============================================================
# 3. Danger Corridors (road-crash matching)
# ============================================================
print('Building danger corridors...')

road_path = '/lakehouse/default/Files/citypulse_output/la-county.json'
if os.path.exists(road_path):
    with open(road_path) as f:
        roads = json.load(f)
    print('Roads loaded:', len(roads))

    # Build spatial grid index of crashes
    GRID = 0.005
    crash_points = crashes.select('latitude', 'longitude', 'severity').collect()

    grid = {}
    for row in crash_points:
        gx = int(row.longitude / GRID)
        gy = int(row.latitude / GRID)
        for dx in [-1, 0, 1]:
            for dy in [-1, 0, 1]:
                key = (gx + dx, gy + dy)
                if key not in grid:
                    grid[key] = []
                grid[key].append((row.latitude, row.longitude, row.severity))

    # Match crashes to roads
    THRESHOLD = 0.0005  # ~55m
    corridor_data = []
    for idx, road in enumerate(roads):
        path = road['path']
        if len(path) < 2:
            continue

        # Find nearby crashes
        road_cells = set()
        for pt in path:
            road_cells.add((int(pt[0] / GRID), int(pt[1] / GRID)))

        candidates = []
        for cell in road_cells:
            candidates.extend(grid.get(cell, []))

        counts = {'total': 0, 'fatal': 0, 'pedestrian': 0, 'bicycle': 0}
        for clat, clng, sev in candidates:
            for i in range(len(path) - 1):
                dx = path[i+1][0] - path[i][0]
                dy = path[i+1][1] - path[i][1]
                lenSq = dx*dx + dy*dy
                if lenSq == 0:
                    continue
                t = max(0, min(1, ((clng - path[i][0]) * dx + (clat - path[i][1]) * dy) / lenSq))
                px = path[i][0] + t * dx
                py = path[i][1] + t * dy
                d = math.sqrt((clng - px)**2 + (clat - py)**2)
                if d < THRESHOLD:
                    counts['total'] += 1
                    if sev in counts:
                        counts[sev] += 1
                    break

        # Color: green -> yellow -> red
        c = counts['total']
        if c == 0:
            color = [0, 100, 60]
        else:
            t = min(1.0, c / 15.0)
            if t < 0.5:
                s = t * 2
                color = [int(255 * s), int(200 + 55 * (1 - s)), 0]
            else:
                s = (t - 0.5) * 2
                color = [255, int(200 * (1 - s)), 0]

        corridor_data.append({
            'path': path, 'color': color,
            'crashes': counts['total'], 'fatal': counts['fatal'],
            'pedestrian': counts['pedestrian'], 'bicycle': counts['bicycle']
        })

        if idx % 5000 == 0 and idx > 0:
            print(f'  {idx}/{len(roads)} roads processed...')

    print('Corridors:', len(corridor_data), 'roads')
    print('  With crashes:', sum(1 for c in corridor_data if c['crashes'] > 0))
else:
    corridor_data = []
    print('Road file not found at', road_path)

# ============================================================
# 4. Summary Statistics
# ============================================================
print('Computing stats...')

total = crashes.count()
fatal = crashes.filter(F.col('severity') == 'fatal').count()
ped = crashes.filter(F.col('severity') == 'pedestrian').count()
bike = crashes.filter(F.col('severity') == 'bicycle').count()
severe = crashes.filter(F.col('severity') == 'severe').count()

top_intersections = (crashes
    .filter(F.col('intersection').isNotNull())
    .groupBy('intersection')
    .agg(F.count('*').alias('crash_count'),
         F.sum(F.when(F.col('severity') == 'fatal', 1).otherwise(0)).alias('fatal'),
         F.avg('latitude').alias('lat'),
         F.avg('longitude').alias('lng'))
    .orderBy(F.desc('crash_count'))
    .limit(20)
    .collect()
)

stats = {
    'total_crashes': total, 'fatal': fatal, 'severe': severe,
    'pedestrian': ped, 'bicycle': bike,
    'top_intersections': [
        {'name': r.intersection, 'crashes': r.crash_count,
         'fatal': r.fatal, 'lat': round(r.lat, 5), 'lng': round(r.lng, 5)}
        for r in top_intersections
    ]
}

# Time series
timeseries = (crashes
    .groupBy('year_month')
    .agg(F.count('*').alias('total'),
         F.sum(F.when(F.col('severity') == 'fatal', 1).otherwise(0)).alias('fatal'),
         F.sum(F.when(F.col('severity') == 'pedestrian', 1).otherwise(0)).alias('pedestrian'),
         F.sum(F.when(F.col('severity') == 'bicycle', 1).otherwise(0)).alias('bicycle'))
    .orderBy('year_month')
    .collect()
)
stats['timeseries'] = [
    {'month': r.year_month, 'total': r.total, 'fatal': r.fatal,
     'pedestrian': r.pedestrian, 'bicycle': r.bicycle}
    for r in timeseries
]

# ============================================================
# 5. Export JSON files
# ============================================================
print('Exporting JSON...')
output_dir = '/lakehouse/default/Files/citypulse_output'
os.makedirs(output_dir, exist_ok=True)

# Heatmap
hm_data = spark.table('gold_h3_heatmap').collect()
hm_json = [{'h3': r.h3_index, 'boundary': json.loads(r.boundary),
             'count': r.crash_count, 'fatal': r.fatal_count,
             'ped': r.ped_count, 'bike': r.bike_count,
             'score': r.severity_score, 'res': int(r.resolution),
             'lat': round(r.center_lat, 5), 'lng': round(r.center_lng, 5)}
            for r in hm_data]
with open(f'{output_dir}/vz-heatmap.json', 'w') as f:
    json.dump(hm_json, f)
print(f'  vz-heatmap.json: {len(hm_json)} hexagons')

# Corridors
with open(f'{output_dir}/vz-corridors.json', 'w') as f:
    json.dump(corridor_data, f)
print(f'  vz-corridors.json: {len(corridor_data)} roads')

# Hotspots
try:
    hs_data = spark.table('gold_hotspots').collect()
    hs_json = [{'z_score': float(r.gi_z_score) if hasattr(r, 'gi_z_score') else 0,
                'p_value': float(r.gi_p_value) if hasattr(r, 'gi_p_value') else 1,
                'count': int(r.count) if hasattr(r, 'count') else 0}
               for r in hs_data]
    with open(f'{output_dir}/vz-hotspots.json', 'w') as f:
        json.dump(hs_json, f)
    print(f'  vz-hotspots.json: {len(hs_json)} bins')
except:
    print('  vz-hotspots.json: skipped (table not found)')

# Crash points
crash_json = [{'lat': round(r.latitude, 5), 'lng': round(r.longitude, 5),
               'date': str(r.crash_date) if r.crash_date else None,
               'hour': r.crash_hour, 'sev': r.severity,
               'loc': r.location, 'cross': r.cross_street, 'ym': r.year_month}
              for r in crashes.select('latitude','longitude','crash_date',
                                     'crash_hour','severity','location',
                                     'cross_street','year_month').collect()]
with open(f'{output_dir}/vz-crashes.json', 'w') as f:
    json.dump(crash_json, f)
print(f'  vz-crashes.json: {len(crash_json)} points')

# Stats
with open(f'{output_dir}/vz-stats.json', 'w') as f:
    json.dump(stats, f, default=str, indent=2)
print(f'  vz-stats.json')

# Verify
for fname in ['vz-heatmap.json', 'vz-corridors.json', 'vz-crashes.json', 'vz-stats.json']:
    fpath = f'{output_dir}/{fname}'
    if os.path.exists(fpath):
        size = os.path.getsize(fpath) / 1024
        print(f'  {fname}: {size:.1f} KB')

print('\n=== GOLD ANALYTICS + EXPORT COMPLETE ===')
print(f'Total crashes: {total:,}')
print(f'Fatal: {fatal:,} | Pedestrian: {ped:,} | Bicycle: {bike:,}')
print(f'Top intersection: {stats["top_intersections"][0]["name"]} ({stats["top_intersections"][0]["crashes"]} crashes)')
