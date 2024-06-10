from pyspark import SparkContext
from pyspark.sql import SparkSession
from datetime import datetime
import csv
from io import StringIO
import geopy.distance

sc = SparkContext(appName="QUERY 4 WITH RDD")

spark = SparkSession.builder.appName("QUERY 4 WITH BROADCAST JOIN").getOrCreate()

rdd1 = sc.textFile('hdfs://master:9000/home/user/data/rows1.csv')
rdd2 = sc.textFile('hdfs://master:9000/home/user/data/rows2.csv')

rdd3 = sc.textFile('hdfs://master:9000/home/user/data/LAPD_Police_Stations.csv')

header1 = rdd1.first()
header2 = rdd2.first()

rdd1 = rdd1.filter(lambda line: line != header1)
rdd2 = rdd2.filter(lambda line: line != header2)

def parse_line(line): #parse csv line correctly, comma might be inside ""
    text = StringIO(line)
    reader = csv.reader(text, quotechar='"', delimiter=',', escapechar='\\')
    return next(reader)

cols=[4,16,26,27]

rdd = rdd1.union(rdd2) \ #concatenate data
      .map(parse_line) \ #parse correctly
      .map(lambda row: [row[i] for i in cols]) \ #keep relevant columns
      .filter(lambda row: row[1].startswith("1") and len(row[1])==3) \ #gun crimes only
      .filter(lambda row: row[2]!='0' and row[3]!=['0']) \ #no null island

header3 = rdd3.first()
rdd3 = rdd3.filter(lambda row: row!=header3) \
       .map(lambda row: row.split(",")) \ 
       .map(lambda row: [int(row[5]),row[0],row[1],row[3]]) \ #keep relevant columns and reorder with division first

stations = sc.broadcast(rdd3.keyBy(lambda x: x[0]).collectAsMap()) # broadcast and conver to hash map

def merge_tables(row):
    area, weapon, lat, lon = row #big table values
    merge_row = stations.value[int(area)]  #find row in small table with hash map 
    _, x, y, division = merge_row #unpack small table row values
    return [int(area), lat, lon, y, x, division] #return merged row with relevant columns only

def get_distance(lat1, long1, lat2, long2):
    return geopy.distance.geodesic((lat1,long1),(lat2,long2)).km

rdd=rdd.map(merge_tables) \
       .map(lambda row: row+[get_distance(row[1],row[2],row[3],row[4])]) \ #create a new distance column
       .map(lambda row: (row[-2],(row[-1],1))) \ #convert row to key-value pair of DIVISION,(distance,1)
       .reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])) \ #group by key (DIVISION) and sum all distances and 1s to count crimes
       .map(lambda x: [x[0], x[1][0]/x[1][1],x[1][1]]) \ #divide sum of distances by crime count to compute average distance 
       .collect()

rdd.sort(key=lambda x: x[2],reverse=True)

print("division","average_distance","incidents total")
for row in rdd:
    print(row[0],row[1],row[2])


spark.catalog.clearCache()

