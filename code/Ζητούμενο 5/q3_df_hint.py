from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, desc, to_timestamp, year, udf
from pyspark.sql.types import DoubleType, StringType

spark = SparkSession.builder.appName("QUERY 3 HINT").getOrCreate()

df1 = spark.read.csv('hdfs://master:9000/home/user/data/rows1.csv',header=True)
df3 = spark.read.csv('hdfs://master:9000/home/user/data/rows2.csv',header=True)
df2 = spark.read.csv('hdfs://master:9000/home/user/data/revgecoding.csv',header=True)

columns = [
    "DR_NO","Date Rptd","TIME OCC","AREA ","AREA NAME",
    "Rpt Dist No","Part 1-2","Crm Cd","Crm Cd Desc","Mocodes","Vict Age","Vict Sex",
    "Premis Cd","Premis Desc","Weapon Used Cd","Weapon Desc","Status","Status Desc","Crm Cd 1","Crm Cd 2",
    "Crm Cd 3","Crm Cd 4","LOCATION","Cross Street"
    ]

df1=df1.filter((col("Vict Age")!='0') | (col("Vict Age")!="NULL") | (col("Vict Sex")!="NULL"))
df3=df3.filter((col("Vict Age")!='0') | (col("Vict Age")!="NULL") | (col("Vict Sex")!="NULL"))


df1=df1.drop(*columns)
df3=df3.drop(*columns)

df1=df1.union(df3) #concatenate data

df1 = df1.withColumn('DATE OCC', to_timestamp('DATE OCC', 'MM/dd/yyyy hh:mm:ss a')) #convert string to timestamp
df1 = df1.withColumn('year',year(col('DATE OCC'))) #year column

df1 = df1.filter((col('year') == 2015) & (col('Vict Descent') != "NULL") & (col('Vict Descent')!="X"))

df1=df1.hint("broadcast").join(df2,["LAT","LON"],"inner") #join
df1.explain(True)

#drop null values
df1=df1.filter(col('ZIPcode').isNotNull())

df3 = spark.read.csv('hdfs://master:9000/home/user/data/LA_income_2015.csv',header=True)

df1 = df1.hint("merge").join(df3,col("ZIPcode")==col("Zip Code"),"inner") #join
df1.explain(True)

#convert to numeric type to sort afterwards
df1 = df1.withColumn("Estimated Median Income", regexp_replace(col("Estimated Median Income"), "[$,]", "").cast(DoubleType()))

vict_dict = {
    "A":"Other Asian","B":"Black","C":"Chinese","D":"Cambodian",
    "F":"Filipino","G":"Guamanian","H":"Hispanic/Latin/Mexican",
    "I":"American Indian/Alaskan Native","J":"Japanese","K":"Korean",
    "L":"Laotian","O":"Other Pacific Islander","S":"Samoan",
    "U":"Hawaiian","V":"Vietnamese","W":"White","X":"Unknown",
    "Z":"Asian Indian"
}

vict_dict = spark.sparkContext.broadcast(vict_dict) #broadcast dictionary for efficiency

def replace_descent(val):
    return vict_dict.value.get(val,val) #if not found return same

decode = udf(replace_descent,StringType())

df1 = df1.withColumn("Vict Descent",decode(col("Vict Descent")))

top3_communities = df1.select("Community", "Estimated Median Income").distinct().orderBy(col("Estimated Median Income").desc()).limit(3) #top 3 communities 
bottom3_communities = df1.select("Community","Estimated Median Income").distinct().orderBy(col("Estimated Median Income")).limit(3) #bottom 3 communities

top_communities_list = [row["Community"] for row in top3_communities.collect()] #top 3 communities as list
bottom_communities_list = [row["Community"] for row in bottom3_communities.collect()] #bottom 3 communities as list

top_df = df1.filter(col("Community").isin(top_communities_list)) #filter to top 3 community crimes only
bottom_df = df1.filter(col("Community").isin(bottom_communities_list)) #filter to bottom 3 community crimes only

top_df = top_df.groupBy("Vict Descent").count().orderBy(col("count").desc()) #group by victim descent, count crimes and sort
bottom_df = bottom_df.groupBy("Vict Descent").count().orderBy(col("count").desc())

top_df.show()

bottom_df.show()

spark.catalog.clearCache()

