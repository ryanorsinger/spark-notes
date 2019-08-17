import pyspark
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("read").\
    enableHiveSupport().\
    getOrCreate()


# Read any type of delimited file
# df = spark.read.format("csv").\
#     option("sep", ",").\
#     option("header", True).\
#     option("inferSchema", True).\
#     load("data/source.csv")

# read a csv into a spark dataframe
df = spark.read.csv("./source.csv", sep=",",
                    header=True, inferSchema=True)

# show the schema
df.printSchema()

df.write.format('parquet').mode('overwrite').\
    option('header','true').save('data/df_source_parquet')

df = spark.read.parquet("data/df_source_parquet")

df.show()
spark.sql("SHOW DATABASES").show()

spark.sql("USE default")
spark.sql("SHOW TABLES").show()

spark.sql(f"SELECT * FROM {table_name} LIMIT 10").sh


df = spark.sql(f"SELECT * FROM {table_name}")
df.show(5)

spark.range(1000).show(5)