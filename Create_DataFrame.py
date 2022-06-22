from pyspark.sql import SparkSession
import os
import sys
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Create DataFrame from RDD
spark = SparkSession.builder.appName('myApp').getOrCreate()
columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
rdd = spark.sparkContext.parallelize(data)
dfFromRDD1 = rdd.toDF()
dfFromRDD1.printSchema()
print(rdd)
print(dfFromRDD1)
dfFromRDD1.show()

# If you wanted to provide column names to the DataFrame use toDF() method with column names as arguments as shown below.
dfFromRDDHavingColumns = rdd.toDF(columns)
dfFromRDDHavingColumns.show()

# createDataFrame() from SparkSession
dfFromRDD2 = spark.createDataFrame(rdd).toDF("language", "count")
dfFromRDD2.show()

# Creating a DataFrame with Schema
data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([
    StructField("firstname",StringType(),True),
    StructField("middlename",StringType(),True),
    StructField("lastname",StringType(),True),
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True)
  ])

df = spark.createDataFrame(data=data2,schema=schema)
df.printSchema()
df.show(truncate=False)

# creating dataframe using external files(csv, txt, json)
df2 = spark.read.csv("details.csv")
df2.printSchema()
df2.show()
# reading from text file
df2 = spark.read.text("/src/resources/file.txt")
# reading json data
df2 = spark.read.json("/src/resources/file.json")





