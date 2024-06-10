from pyspark.sql import SparkSession
import sys

read_file=sys.argv[1] #path to csv file
read_path = "hdfs://master:9000/home/user/data/"+read_file #where to read csv file

write_file = sys.argv[2] #name of new file
write_path = "hdfs://master:9000/home/user/data/"+write_file #where to write parquet file

spark = SparkSession.builder.appName("CSV 2 Parquet").getOrCreate()

df = spark.read.csv(read_path,header=True,inferSchema=True)
df.write.parquet(write_path)

