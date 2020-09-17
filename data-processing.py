# NOTES ON CSV FILE EDITS
# - header of DataSample.csv manually edited to remove an unexpected space
# - header of POIList.csv manually edited to remove an unexpected space
#   and to provide header names different from DataSample.csv

import math
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from geopy import distance

spark = SparkSession.builder.master("local[*]") \
                    .getOrCreate()

df_data = spark.read.option("header",True) \
                    .csv("/tmp/data/DataSample.csv")
df_data.createOrReplaceTempView("data")

# clean data by collecting duplicates together
# and only keeping the first entry
df_data = spark.sql("SELECT * FROM data x \
                    WHERE x._ID IN \
                      (SELECT _ID FROM \
                         (SELECT _ID, ROW_NUMBER() OVER \
                            (PARTITION BY data.TimeSt, data.Country, data.Province, data.City, \
                                data.Latitude, data.Longitude ORDER BY _ID) dup \
                            FROM data) \
                            WHERE dup < 2)")

# filter out implausible points via a box bounding Canada's extremes
df_data = df_data.filter(df_data["Latitude"]>41.681389)
df_data = df_data.filter(df_data["Latitude"]<83.111389)
df_data = df_data.filter(df_data["Longitude"]>-141.001944)
df_data = df_data.filter(df_data["Longitude"]<-52.619444)

# output for question 1
df_data.write.option("header",True) \
             .csv("/tmp/data/cleaned-data")

df_pois = spark.read.options(header='True',inferSchema='True',delimiter=',') \
                    .csv("/tmp/data/POIList.csv")
df_pois.createOrReplaceTempView("pois")

# remove duplicate POIs from the list of POIs
# depending on what POIs represent, we might understand that
# there could be multiple POIs in one location, and requests
# should be distributed evenly among them, but here I will
# assume that duplicates are just a mistake
df_pois = spark.sql("SELECT * FROM pois x \
                    WHERE x.POIID IN \
                      (SELECT POIID FROM \
                         (SELECT POIID, ROW_NUMBER() OVER \
                            (PARTITION BY pois.POILatitude, \
                            pois.POILongitude ORDER BY POIID) dup \
                            FROM pois) \
                            WHERE dup < 2)")

# redefining distance function so it's usable in dataframe
def dist(a,b,x,y):
    return distance.distance((a,b),(x,y)).km
udf_dist = udf(dist, FloatType())

# create a new dataframe of size count(POIs)*requests, 
# annotated with distance between each request and each POI
distances = df_data.join(df_pois).withColumn('Distance', udf_dist(df_data.Latitude,df_data.Longitude, \
                                                                  df_pois.POILatitude,df_pois.POILongitude))
# find min distance per request
min_distances = distances.groupBy('_ID').min('Distance')
# annotate distances with min distances, then filter out all non-min POIs
# and delete unnecessary columns
distances = distances.join(min_distances, distances._ID == min_distances._ID, 'inner') \
                     .select(distances["*"],min_distances["min(Distance)"])
distances = distances.filter(distances["Distance"]==distances["min(Distance)"])
df_data = distances.drop("min(Distance)", "POILatitude", "POILongitude")

# output for question 2
df_data.write.option("header",True) \
             .csv("/tmp/data/assigned-data")

def density(count, radius):
    return count / (math.pi*(radius**2.0))
udf_density = udf(density, FloatType())

# aggregating desired statistics together in a single dataframe
stats = df_data.groupBy('POIID').count()
poi_stats = df_pois.join(stats, df_pois.POIID == stats.POIID).select(df_pois["*"],stats["count"])
stats = df_data.groupBy('POIID').agg({'Distance': 'mean'})
poi_stats = poi_stats.join(stats, poi_stats.POIID == stats.POIID).select(poi_stats["*"],stats["avg(Distance)"])
stats = df_data.groupBy('POIID').agg({'Distance': 'stddev'})
poi_stats = poi_stats.join(stats, poi_stats.POIID == stats.POIID).select(poi_stats["*"],stats["stddev(Distance)"])
stats = df_data.groupBy('POIID').agg({'Distance': 'max'})
poi_stats = poi_stats.join(stats, poi_stats.POIID == stats.POIID).select(poi_stats["*"],stats["max(Distance)"])
poi_stats = poi_stats.withColumn("Density", udf_density(poi_stats["count"], poi_stats["max(Distance)"]))

# write to file, commented out line produces a single .csv file
# poi_stats.repartition(1).write.option("header",True).csv("poi-stats", sep=',')
poi_stats.write.option("header",True) \
              .csv("/tmp/data/poi-stats")