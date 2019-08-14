import pandas as pd
import numpy as np
import pyspark
import pyspark.sql.functions as F


# Super Simple setup of spark session
# spark = pyspark.sql.SparkSession.builder.getOrCreate()

# Basic Spark session with yarn to run on Hadoop cluster
# spark = SparkSession.builder\
#      .master("local")\ # "yarn" to run on hadoop cluster
#      .appName("mylocalconnection")\
#      .getOrCreate()

# Spark session w/ Hive support enabled
# spark = SparkSession.builder.master("local").appName("read").\
#     enableHiveSupport().\
#     getOrCreate()



# More complex/tuneable pyspark session
import multiprocessing
import pyspark

nprocs = multiprocessing.cpu_count()

spark = (pyspark.sql.SparkSession.builder
 .master('local')
 .config('spark.jars.packages', 'mysql:mysql-connector-java:8.0.16')
 .config('spark.driver.memory', '4G')
 .config('spark.driver.cores', nprocs)
 .config('spark.sql.shuffle.partitions', nprocs)
 .appName('MySparkApplication')
 .getOrCreate())

