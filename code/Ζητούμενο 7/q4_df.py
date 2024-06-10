from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, hour, udf, avg, count
from pyspark.sql.types import DoubleType

import geopy.distance

def get_distance(lat1, long1, lat2, long2):
    return geopy.distance.geodesic((lat1,long1),(lat2,long2)).km

# Initialize Spark session
spark = SparkSession.builder.appName("QUERY 4 WITH DATAFRAME").getOrCreate()
# Sample data
# Create DataFrame
df1 = spark.read.csv('hdfs://master:9000/home/user/data/rows1.csv',header=True)
df2 = spark.read.csv('hdfs://master:9000/home/user/data/rows2.csv',header=True)
df3 = spark.read.csv('hdfs://master:9000/home/user/data/LAPD_Police_Stations.csv',header=True)

df = df1.union(df2)

columns = [
    "DR_NO","Date Rptd","DATE OCC","TIME OCC","AREA NAME",
    "Rpt Dist No","Part 1-2","Crm Cd","Crm Cd Desc","Mocodes","Vict Age","Vict Sex","Vict Descent",
    "Premis Cd","Premis Desc","Weapon Desc","Status","Status Desc","Crm Cd 1","Crm Cd 2",
    "Crm Cd 3","Crm Cd 4","LOCATION","Cross Street"]

df = df.drop(*columns)

df = df.where(col("Weapon Used Cd").like("1__")) #gun crime only
df = df.filter((col("LAT")!='0') & (col("LON")!='0')) #remove null island rows

df=df.withColumn("AREA",col("AREA").cast("integer")) #cast to int

df3=df3.withColumn("PREC",col("PREC").cast("integer")) #cast to int

df = df.join(df3,col("AREA")==col("PREC"),"inner") #join

distance = udf(get_distance, DoubleType()) #define distance udf

df = df.withColumn("Distance", distance(col("LAT"), col("LON"), col("Y"), col("X"))) #add distance as new column

df = df.groupBy("DIVISION").agg(avg("Distance").alias("average_distance"),count("Distance").alias("incidents_total")) #group by division, get avg distance and crime count for each division
df_grouped = df.orderBy(col("incidents_total"),ascending=False) #sort 

df_grouped.show(25)

spark.catalog.clearCache()

