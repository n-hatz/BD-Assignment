from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, hour
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("QUERY2 WITH DF").getOrCreate()

df1 = spark.read.csv('hdfs://master:9000/home/user/data/rows1.csv',header=True)
df2 = spark.read.csv('hdfs://master:9000/home/user/data/rows2.csv',header=True)

columns = [
    "DR_NO","Date Rptd","DATE OCC","AREA","AREA NAME",
    "Rpt Dist No","Part 1-2","Crm Cd","Crm Cd Desc","Mocodes","Vict Age","Vict Sex","Vict Descent",
    "Premis Cd","Weapon Used Cd","Weapon Desc","Status","Status Desc","Crm Cd 1","Crm Cd 2",
    "Crm Cd 3","Crm Cd 4","LOCATION","Cross Street","LAT","LON"
    ]

df1=df1.drop(*columns)
df2=df2.drop(*columns)

df1=df1.union(df2)

df1 = df1.withColumn('TIME OCC', F.lpad(F.col('TIME OCC').cast('string'), 4, '0')) #pad zeroes for consistent format
df1 = df1.withColumn('time', F.to_timestamp(F.col('TIME OCC'), 'HHmm')) #cast to time


df_filtered = df1.filter(col("Premis Desc") == "STREET")

# Define the time periods
df_periods = df_filtered.withColumn("time of day", 
    when((hour(col("time")) >= 5) & (hour(col("time")) < 12), "Morning")
    .when((hour(col("time")) >= 12) & (hour(col("time")) < 17), "Afternoon")
    .when((hour(col("time")) >= 17) & (hour(col("time")) < 21), "Evening")
    .otherwise("Night"))

df_grouped = df_periods.groupBy("time of day").count() #count number of crimes in each period

df_sorted = df_grouped.orderBy(col("count").desc())
df_sorted.show()

spark.catalog.clearCache()
