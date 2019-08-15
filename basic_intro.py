import pandas as pd
import numpy as np
import pyspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import multiprocessing

nprocs = multiprocessing.cpu_count()
nprocs = multiprocessing.cpu_count()

spark = (pyspark.sql.SparkSession.builder
 .master('local')
 .config('spark.jars.packages', 'mysql:mysql-connector-java:8.0.16')
 .config('spark.driver.memory', '4G')
 .config('spark.driver.cores', nprocs)
 .config('spark.sql.shuffle.partitions', nprocs)
 .appName('MySparkApplication')
 .getOrCreate())

df = spark.read.csv("./student_grades.csv", sep=",", header=True, inferSchema=True)
df.count() # produces a count of the number of records
df.show()

df.first()
df.show(11) # show method takes an optional argument

df.describe()

df.withColumn('final_grade_above_90', df.final_grade >= 90).show()

# list of dictionaries
fruits = [
    {
        "name": "apple",
        "price": 1.25,
        "quantity": 5
    },
    {
        "name": "kiwi",
        "price": 3.99,
        "quantity": 7
    },
    {
        "name": "orange",
        "price": 1.50,
        "quantity": 8
    },
]

fruits = spark.createDataFrame(fruits)

fruits.show()