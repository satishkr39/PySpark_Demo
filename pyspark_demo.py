import os
import sys
from pyspark.sql import SparkSession

import pyspark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# creating spark session
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
print(spark)
# creating another spark session
spark2 = SparkSession.newSession
print(spark2)
# Get Existing SparkSession
spark3 = SparkSession.builder.getOrCreate()
print(spark3)
spark.conf.set("myname", "5g") # Set Config
partions = spark.conf.get("myname") # Get a Spark Config
print(partions)
# getting spark context variable
print("Spark Context Variable",spark.sparkContext)
print("Spark Context AppName ",spark.sparkContext.appName)
# SparkContext stop() method
# spark.sparkContext.stop()
# common used spark variables
print(spark.sparkContext.uiWebUrl)
print(spark.sparkContext.version)
print(spark.sparkContext.applicationId)

'''
spark = SparkSession.builder.appName('myfirstapp').getOrCreate()
print(spark)

'''
'''
data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
print(df.show())''''''
'''

