from pyspark.sql import SparkSession
import os
import sys
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


spark = SparkSession.builder.appName('myapp').getOrCreate()
print(spark)
data =[("James ","","Smith","36636","M",3000),
              ("Michael ","Rose","","40288","M",4000),
              ("Robert ","","Williams","42114","M",4000),
              ("Maria ","Anne","Jones","39192","F",4000),
              ("Jen","Mary","Brown","","F",-1)]
columns=["firstname","middlename","lastname","dob","gender","salary"]
df=spark.createDataFrame(data,columns)

df.show()

df.write.parquet("people.parquet")
# parDF1=spark.read.parquet("people.parquet")