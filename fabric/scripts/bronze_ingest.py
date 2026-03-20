# Bronze Ingest - Vision Zero LA
# Fetches LAPD crash data from SODA API, writes to bronze_crashes Delta table

import requests

FIELDS = ['dr_no','date_occ','time_occ','area_name','crm_cd_desc',
          'vict_age','vict_sex','vict_descent','premis_desc','location',
          'cross_street','lat','lon','mocodes']
BASE = 'https://data.lacity.org/resource/d5tf-ez2w.json'

# Fetch with 10K batches
all_records = []
offset = 0
batch_size = 10000
select_str = ','.join(FIELDS)
while True:
    url = (BASE + '?$limit=' + str(batch_size) + '&$offset=' + str(offset) +
           '&$select=' + select_str +
           '&$where=date_occ%3E%272022-01-01T00:00:00%27' +
           '&$order=date_occ%20DESC')
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    batch = r.json()
    if not batch:
        break
    all_records.extend(batch)
    print(len(all_records), 'records (batch', offset // batch_size + 1, ')')
    if len(batch) < batch_size:
        break
    offset += batch_size
print('TOTAL:', len(all_records))

# Normalize to strings
rows = [tuple(str(rec.get(f, '') or '') for f in FIELDS) for rec in all_records]

# Create DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
schema = StructType([StructField(f, StringType(), True) for f in FIELDS])
df = spark.createDataFrame(rows, schema)

# Write Delta table
df2 = df.withColumn('crash_year', F.year(F.to_date('date_occ')))
df2.write.format('delta').mode('overwrite').partitionBy('crash_year').saveAsTable('bronze_crashes')
spark.sql('SELECT crash_year, count(*) cnt FROM bronze_crashes GROUP BY 1 ORDER BY 1').show()
print('BRONZE COMPLETE:', spark.table('bronze_crashes').count(), 'rows')
