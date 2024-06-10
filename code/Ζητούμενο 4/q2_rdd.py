from pyspark import SparkContext
from pyspark.sql import SparkSession
from datetime import datetime
import csv
from io import StringIO

sc = SparkContext(appName="QUERY 2 WITH RDD")

spark = SparkSession.builder.appName("QUERY 2 WITH RDD").getOrCreate()

rdd1 = sc.textFile('hdfs://master:9000/home/user/data/rows1.csv')
rdd2 = sc.textFile('hdfs://master:9000/home/user/data/rows2.csv')

header1 = rdd1.first()
header2 = rdd2.first()

rdd1 = rdd1.filter(lambda line: line != header1)
rdd2 = rdd2.filter(lambda line: line != header2)

rdd = rdd1.union(rdd2)

def parse_time(row):
    row[0] = row[0].zfill(4)
    row[0] = datetime.strptime(row[0], "%H%M").time()
    return row

def get_period(row):
    hour = row[0].hour
    if 5 <= hour < 12:
        period = "Morning"
    elif 12 <= hour < 17:
        period = "Afternoon"
    elif 17 <= hour < 21:
        period = "Evening"
    else:
        period = "Night"
    return (period, 1)

def parse_line(line): #parse csv line correctly, comma might be inside ""
    text = StringIO(line)
    reader = csv.reader(text, quotechar='"', delimiter=',', escapechar='\\')
    return next(reader)

cols = [3,15]

result = rdd.map(parse_line) \
        .map(lambda row: [row[i] for i in cols]) \ #keep only TIME OCC and Premis Desc
	.filter(lambda row: row[1]=="STREET") \
        .map(parse_time) \ #fix time and convert to timestamp
        .map(get_period) \ #get period based on εκφώνηση
        .reduceByKey(lambda a, b: a+b) \ #count crimes in each time of day period
        .collect()


result_sorted = sorted(result, key=lambda x: x[1], reverse=True) #sort descending based on amount of crimes

for period, count in result_sorted:
    print(f"{period}: {count}")

spark.catalog.clearCache()
