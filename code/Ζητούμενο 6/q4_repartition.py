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

rdd = rdd1.union(rdd2) \
      .map(parse_line) \
      .map(lambda row: [row[i] for i in cols]) \ #keep relevant rows
      .filter(lambda row: row[1].startswith("1") and len(row[1])==3) \ #gun crimes only
      .filter(lambda row: row[2]!='0' and row[3]!=['0']) \ #remove null island rows

header3 = rdd3.first()
rdd3 = rdd3.filter(lambda row: row!=header3) \
       .map(lambda row: row.split(",")) \
       .map(lambda row: [int(row[5]),row[0],row[1],row[3]]) \

rdd=rdd.map(lambda r: (int(r[0]),["L",r[1],r[2],r[3]])) #tag left table

rdd3=rdd3.map(lambda r: (r[0],["R",r[1],r[2],r[3]])) #tag right table

LIST_V = rdd.union(rdd3) #concatenate tables

def buffers(group):
    join_key = group[0] #join key
    other = group[1] #rest of the columns from either L or R
    buffer_R = []
    buffer_L = []
    for item in other: #go through each item in group and add to different buffer based on tag
        if item[0]=="R":
            buffer_R.append(item[1:])
        elif item[0]=="L":
            buffer_L.append(item[1:])
    new=[]
    for l in buffer_L: #cartesian product
        for r in buffer_R:
            new.append(l+[join_key]+r) #join key in the middle

    return new

result = LIST_V.groupByKey() \
               .flatMap(buffers)

def get_distance(lat1, long1, lat2, long2):
    return geopy.distance.geodesic((lat1,long1),(lat2,long2)).km

result= result.map(lambda row: row+[get_distance(row[1],row[2],row[5],row[4])]) \ #new distance column
       .map(lambda row: (row[-2],(row[-1],1))) \ #key value pair for group by
       .reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])) \ #group by division, sum distances and crime count
       .map(lambda x: [x[0], x[1][0]/x[1][1],x[1][1]]) \ #divide distance sum by crime count to get average
       .collect()

result.sort(key=lambda x: x[2],reverse=True)

print("division","average_distance","incidents total")
for row in result:
    print(row[0],row[1],row[2])

spark.catalog.clearCache()

