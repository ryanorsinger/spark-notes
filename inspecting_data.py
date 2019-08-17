from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("inspect").getOrCreate()

data = "./dept.csv"
df = spark.read.csv(data, header=True, inferSchema=True)
df.printSchema()
df.dtypes
df.columns
df.schema
df.show()
df.count()
df.describe().show()

pd.options.display.html.table_schema = True
df.describe().toPandas()

df.select("dept_name").describe().show()


# The count in the describe method is a count of non-missing values.
df.describe().show()
df.count()

df.head(5)
rows = df.head(5)
type(rows)
rows[0][0]
rows[0]['dept_division']
df.take(5)

df.show(10)

df.describe("dept_division").show()

from pyspark.sql.functions import count, countDistinct
df.select(count("dept_division"), countDistinct("dept_division")).show()

df.select("dept_name").show(10)

df.select("dept_name").distinct().show()