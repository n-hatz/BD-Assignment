from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType, TimestampType
from datetime import datetime
from pyspark.sql.functions import col, count, desc, to_timestamp, year, month

spark = SparkSession \
    .builder \
    .appName("QUERY 1 WITH SQL AND PARQUET") \
    .getOrCreate()

df1 = spark.read.parquet('hdfs://master:9000/home/user/data/rows1.parquet')
df2 = spark.read.parquet('hdfs://master:9000/home/user/data/rows2.parquet')

columns = [
    "DR_NO","Date Rptd","TIME OCC","AREA","AREA NAME",
    "Rpt Dist No","Part 1-2","Crm Cd","Crm Cd Desc","Mocodes","Vict Age","Vict Sex","Vict Descent",
    "Premis Cd","Premis Desc","Weapon Used Cd","Weapon Desc","Status","Status Desc","Crm Cd 1","Crm Cd 2",
    "Crm Cd 3","Crm Cd 4","LOCATION","Cross Street","LAT","LON"
    ] #columns to drop

df1=df1.drop(*columns) #drop columns
df2=df2.drop(*columns) #drop columns

df1=df1.union(df2) #concatenate data

df1 = df1.withColumn('DATE OCC', to_timestamp('DATE OCC', 'MM/dd/yyyy hh:mm:ss a')) #convert string to timestamp
df1 = df1.withColumn('year',year(col('DATE OCC'))) #year column
df1 = df1.withColumn('month',month(col('DATE OCC'))) #month column

df1 = df1.drop('DATE OCC')

schema = StructType([
    StructField("year", StringType()),
    StructField("month", StringType()),
])

q1_df = spark.createDataFrame(df1.rdd,schema)

q1_df.registerTempTable("q1")

query = f"""
WITH ranked_months AS (
    SELECT 
        year, 
        month, 
        COUNT(*) AS counts,
        ROW_NUMBER() OVER (PARTITION BY year ORDER BY COUNT(*) DESC) AS rank
    FROM q1
    GROUP BY year, month
)
SELECT year, month, counts AS crime_total, rank AS ranking
FROM ranked_months
WHERE rank <= 3
ORDER BY year ASC, counts DESC;
"""
result = spark.sql(query)
result.show(45)

spark.catalog.clearCache()
