# NOTES ON CSV FILE EDITS
# - header of DataSample.csv manually edited to remove an unexpected space
# - header of POIList.csv manually edited to remove an unexpected space
#   and to provide header names different from DataSample.csv

import re
import folium
import math
from geopy import distance
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

spark = SparkSession.builder.master("local[*]") \
                    .getOrCreate()
    
# to prevent error                
spark.conf.set("spark.sql.crossJoin.enabled", "true")

##############
# QUESTION 1 #
##############

df_data = spark.read.options(header='True',inferSchema='True',delimiter=',') \
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
             
##############
# QUESTION 2 #
##############

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

# redefining geopy distance function so it's usable in dataframe
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
distances = distances.join(min_distances, distances._ID == min_distances._ID) \
                     .select(distances["*"],min_distances["min(Distance)"])
distances = distances.filter(distances["Distance"]==distances["min(Distance)"])
df_data = distances.drop("min(Distance)", "POILatitude", "POILongitude")

# output for question 2
df_data.write.option("header",True) \
             .csv("/tmp/data/assigned-data")
             
##############
# QUESTION 3 #
##############

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

# ouput for question 3, commented out line produces a single .csv file
# poi_stats.repartition(1).write.option("header",True).csv("/tmp/data/poi-stats", sep=',')
poi_stats.write.option("header",True) \
              .csv("/tmp/data/poi-stats")

# read in poi statistics file produced earlier
# reading in file speeds up execution compared to 
# using the loaded data frame for some reason
df_stats = spark.read.format("csv") \
                .options(header='True',inferSchema='True',delimiter=',') \
                .load("/tmp/data/poi-stats/*.csv")
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
   
# output for question 3
poi_map.save('/tmp/data/map.html')

###############
# QUESTION 4A #
###############

# simple z-score normalization
def standardize(x, mean, sd):
    z = (x - mean)/sd
    return z
udf_standardize = udf(standardize, FloatType())

# rescales z-scores to [-10, 10] scale, with extreme ends
# of the scale mapped to 3+ standard deviations from the mean
# this gives better separation to the majority of the data and
# implies that any point assigned -10 or 10 is a significant outlier
def rescale(pop):
    r = -10.0+(((pop+3.0)*20.0)/(6.0))
    if r < -10.0:
        r = -10.0
    if r > 10.0:
        r = 10.0
    return r
udf_rescale = udf(rescale, FloatType())

# create mean and standard deviation columns for computations
temp = df_stats.agg({'Density':'mean'})
df_stats = df_stats.join(temp).select(df_stats["*"],temp["avg(Density)"])
temp = df_stats.agg({'Density':'stddev'})
df_stats = df_stats.join(temp).select(df_stats["*"],temp["stddev(Density)"])
# compute simple z-score
df_stats = df_stats.withColumn("pop", udf_standardize(df_stats["Density"], df_stats["avg(Density)"], df_stats["stddev(Density)"]))
# rescale z-score
df_stats = df_stats.withColumn("Popularity", udf_rescale(df_stats["pop"]))
# remove columns used for computations
df_stats = df_stats.drop("avg(Density)","stddev(Density)","pop")

# output for question 4a, forced into a single file
df_stats.repartition(1).write.option("header",True).csv("/tmp/data/final-stats", sep=',')
#df_stats.write.option("header",True) \
#              .csv("/tmp/data/final-stats")

###############
# QUESTION 4B #
###############

# load in list of edges
file = open("/tmp/data/relations.txt")  
edges_list = []
for line in file:
    edges_list = edges_list + [re.findall('[0-9]+', line)]  
file.close()

# load in task list
taskIDs = []
file = open("/tmp/data/task_ids.txt")
for line in file:
    taskIDs = taskIDs + re.findall('[0-9]+', line)   
file.close()

# load in question
question = []
file = open("/tmp/data/question.txt")
for line in file:
    question = question + re.findall('[0-9]+', line)   
file.close()

# finds a topological sorting for the graph above the goal node
def graph_traversal(goal, nodes, edges, visited, stack):
    if visited[nodes.index(goal)] == 0:  # if current node hasn't been visited yet
        visited[nodes.index(goal)] = 1   # update visited status
        for x in range(0, len(edges)):
            if edges[x][1] == goal:      # for all prereq nodes connected to current node
                # recursively call graphTraversal to find all earlier prereq nodes
                graph_traversal(edges[x][0], nodes, edges, visited, stack)
        # appending to stack here preserves topological sorting
        stack.append(goal)
    return stack

# computes the target path taking into account start and goal nodes
def find_path(start, goal, nodes, edges):
    visited_list = [0] * len(nodes)  # where 0 is unvisited, 1 is visited
    goal_dependencies = graph_traversal(goal, nodes, edges, visited_list, [])
    visited_list = [0] * len(nodes)
    start_dependencies = graph_traversal(start, nodes, edges, visited_list, [])
    
    # remove any prereqs that have been already satisfied given the start node
    final_path = []
    for x in goal_dependencies:
        if x not in start_dependencies:  # remove any prereqs already satisfied
            final_path.append(x)         # given the start node
        if x == start:                  
            final_path.append(x)         # keep start node if it is a prereq of the goal
    return final_path
  
# output for question 4b
finalString = ', '.join(find_path(question[0], question[1], taskIDs, edges_list))
file = open("/tmp/data/pipeline-answer.txt","w")
file.write(finalString)
file.close() 