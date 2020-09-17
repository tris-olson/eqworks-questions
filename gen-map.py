import folium
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]") \
                    .getOrCreate()

# read in poi statistics file produced earlier
df_stats = spark.read.format("csv") \
                .options(header='True',inferSchema='True',delimiter=',') \
                .load("./poi-stats/*.csv")
df_stats.createOrReplaceTempView("stats_data")

# generate map
poi_map = folium.Map(location=[57.4, -96.466667], \
               zoom_start=3)

# iterate over each POI mapping desired features
for f in df_stats.collect(): 
    folium.Circle(
    radius=f["max(Distance)"]*1000.0, # convert from km to m
    location=[f.POILatitude, f.POILongitude],
    popup=f.POIID,
    color='crimson',
    fill=False,
    ).add_to(poi_map)
    
poi_map.save('/tmp/data/map.html')

# output for question 3
#df_stats.show()
#poi_map