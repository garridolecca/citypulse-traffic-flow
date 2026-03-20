# Silver Clean & Enrich - Vision Zero LA
# Reads bronze_crashes, cleans, classifies severity, adds H3, uses GeoAnalytics

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import h3

# Read bronze
crashes = spark.table('bronze_crashes')
print('Bronze rows:', crashes.count())

# Clean & type cast
silver = (crashes
    .withColumn('latitude', F.col('lat').cast('double'))
    .withColumn('longitude', F.col('lon').cast('double'))
    .withColumn('crash_date', F.to_date('date_occ'))
    .withColumn('crash_hour', F.substring('time_occ', 1, 2).cast('int'))
    .withColumn('crash_year', F.year(F.to_date('date_occ')))
    .withColumn('crash_month', F.month(F.to_date('date_occ')))
    .withColumn('year_month', F.date_format(F.to_date('date_occ'), 'yyyy-MM'))
    .filter(F.col('latitude').isNotNull())
    .filter(F.col('longitude').isNotNull())
    .filter(F.col('latitude') != 0)
    .filter(F.col('longitude') != 0)
    .filter(F.col('latitude').between(33.5, 35.0))
    .filter(F.col('longitude').between(-119.0, -117.5))
    .dropDuplicates(['dr_no'])
)

# Severity classification
@F.udf(StringType())
def classify_severity(mocodes, crm_cd_desc):
    codes = set((mocodes or '').strip().split())
    desc = (crm_cd_desc or '').upper()
    if '0449' in codes or 'KILLED' in desc:
        return 'fatal'
    elif '1920' in codes or 'PEDESTRIAN' in desc:
        return 'pedestrian'
    elif '1822' in codes or 'BICYCLE' in desc or 'BIKE' in desc:
        return 'bicycle'
    elif '0448' in codes or 'FELONY' in desc:
        return 'severe'
    return 'other'

silver = silver.withColumn('severity', classify_severity('mocodes', 'crm_cd_desc'))

# Intersection label
silver = silver.withColumn('intersection',
    F.when(F.col('cross_street').isNotNull() & (F.col('cross_street') != ''),
           F.concat(F.col('location'), F.lit(' / '), F.col('cross_street')))
    .otherwise(F.col('location')))

# H3 indexing
@F.udf(StringType())
def to_h3_9(lat, lon):
    try:
        return h3.latlng_to_cell(float(lat), float(lon), 9)
    except:
        return None

@F.udf(StringType())
def to_h3_8(lat, lon):
    try:
        return h3.latlng_to_cell(float(lat), float(lon), 8)
    except:
        return None

silver = (silver
    .withColumn('h3_res9', to_h3_9('latitude', 'longitude'))
    .withColumn('h3_res8', to_h3_8('latitude', 'longitude'))
    .filter(F.col('h3_res9').isNotNull())
)

# GeoAnalytics Engine - Create geometry
try:
    from geoanalytics_fabric.sql import functions as GA
    silver = silver.withColumn('geometry', GA.ST_Point('longitude', 'latitude', 4326))
    print('GeoAnalytics: ST_Point created')

    # School zone buffer analysis
    try:
        schools = spark.table('bronze_schools')
        if 'latitude' in schools.columns and 'longitude' in schools.columns:
            schools_geo = schools.withColumn('geom', GA.ST_Point(
                F.col('longitude').cast('double'), F.col('latitude').cast('double'), 4326))
            schools_buffered = schools_geo.withColumn('buffer', GA.ST_Buffer('geom', 300, 'meters'))
            # Note: full spatial join requires matching geometry columns
            print('GeoAnalytics: School buffers created')
    except Exception as e:
        print('School zone analysis skipped:', e)
except ImportError:
    print('GeoAnalytics not available - skipping spatial operations')

# Write silver table
silver.write.format('delta').mode('overwrite').partitionBy('crash_year').saveAsTable('silver_crashes')

print('SILVER COMPLETE:', spark.table('silver_crashes').count(), 'rows')
silver.groupBy('severity').count().orderBy(F.desc('count')).show()
