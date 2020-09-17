import math
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType

spark = SparkSession.builder.master("local[*]") \
                    .getOrCreate()

# read in poi statistics file produced earlier
df_stats = spark.read.format("csv") \
                .options(header='True',inferSchema='True',delimiter=',') \
                .load("./poi-stats/*.csv")
df_stats.createOrReplaceTempView("stats_data")

# simple z-score normalization
def standardize(x, mean, sd):
    z = (x - mean)/sd
    return z
udf_standardize = udf(standardize, FloatType())

# rescales z-scores to [-10, 10] scale, with the extreme
# ends of the scale locked at 3 standard deviations
# this captures the majority of the data and means that
# any point assigned -10 or 10 is a big outlier
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

# output for question 4. commented out line writes a single file
# df_stats.repartition(1).write.option("header",True).csv("final-stats", sep=',')
df_stats.write.option("header",True) \
              .csv("/tmp/data/final-stats")

df_stats.show()