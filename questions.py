import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]") \
                    .getOrCreate()

df = spark.read.option("header",True) \
    .csv("work/DataSample.csv")
df.createOrReplaceTempView("data")

sqlDF = spark.sql("SELECT * \
FROM data a \
WHERE a._ID IN \
  (SELECT _ID FROM \
     (SELECT \
      _ID, \
      ROW_NUMBER() OVER \
        (PARTITION BY data. TimeSt, data.Country, data.Province, data.City, data.Latitude, data.Longitude ORDER BY _ID) dup \
    FROM data) \
    WHERE dup < 2);")

sqlDF.write.option("header",True) \
 .csv("work/DataEdited")
