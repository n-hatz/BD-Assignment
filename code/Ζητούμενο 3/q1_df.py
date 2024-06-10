from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, to_timestamp, year, month
from pyspark.sql.window import Window
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("QUERY 1 WITH DATAFRAMES AND CSV").getOrCreate()

df1 = spark.read.csv('hdfs://master:9000/home/user/data/rows1.csv',header=True)
df2 = spark.read.csv('hdfs://master:9000/home/user/data/rows2.csv',header=True)

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

df1 = df1.groupBy('year', 'month').agg(count('*').alias('crime_total')) #group by year and month and count

window = Window.partitionBy('year').orderBy(desc('crime_total')) #window

df1 = df1.withColumn('ranking', F.row_number().over(window)) #make ranking of months per year

df1 = df1.filter(col('ranking') <= 3) #get top 3 months per year

df1 = df1.orderBy('year', 'ranking')

df1.show(45)

spark.catalog.clearCache()
