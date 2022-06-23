from pyspark.sql import SparkSession

from pyspark import SparkContext

spark = SparkSession.builder.appName('myapp').getOrCreate()
print(spark)

df = spark.read.csv('details.csv', inferSchema=True, header=True)
print(df.show())
print(df.columns)

df.withColumn('HV Column', df.groupby('high_price').max()/df.groupby('stock').count())