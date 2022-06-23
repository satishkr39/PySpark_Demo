import os
import sys
from pyspark.sql import SparkSession


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# creating spark session
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
'''
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
data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]
# working with dataframes
columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
''' 
print(df.show()) # to print the dataframe
print(df.printSchema()) # to print the schema
print(df.columns) # prints list of columns
print(df.describe()) # describes the schema
df.describe().show() # shows the statistics of our numberical columns only
'''
# creating my own schema
'''
from pyspark.sql.types import StructType, StringType, StructField, IntegerType
my_schema = StructType([
    StructField('age', IntegerType(), True), # means Age column of type int and Null is allowed
    StructField('name', StringType(), True) # name column of string type and Null is allowed
])
my_df_with_my_schmea = spark.read.csv('my_schema_data.csv', schema=my_schema) # specifying to use my_schema
print(my_df_with_my_schmea.printSchema())
print(my_df_with_my_schmea.show())
'''
# dataframe operations set 2
'''
print(df.select('firstname').show()) # selecting a column
print(df.select(['firstname', 'gender']).show()) # select 2 columns
print(df.head(2)) # getting first 2 rows only
print(df.withColumn('newage', df['salary']*2).show()) # creating a new column with values = age*2
print(df.withColumnRenamed('salary', 'new_sal_renamed').show())
'''
# using sql queries to interact with spark dataframes. we first need to register df as a temp View and then we can query
'''
df.createOrReplaceTempView('my_temp_view')
results = spark.sql("SELECT * FROM my_temp_view")
print(results.show())
results_filtered = spark.sql("SELECT * FROM my_temp_view where gender = 'M'") # using where queries like SQL
print(results_filtered.show())
'''

# spark dataframe basic operations
'''
print(df.filter("salary < 4000").show()) # to show where salary is < 4000
print(df.filter("salary < 4000").select(["firstname", "lastname"]).show()) # filtering and selecting few columns only
print(df.filter((df['salary'] < 4000) & (df['gender'] == 'M')).show()) # using 2 filters and show all cols
collect_result = df.filter("salary < 4000").collect() # saving the filtered data in a dataframe
print(collect_result)
'''

# group by and aggregate operations
# header = ['GRE Score', 'TOEFL Score', 'Univ Rating', 'SOP', 'LOR', 'CGPA', 'Research', 'Change of Admit']
'''
music_data = spark.read.option("header",True).csv("songs_normalize.csv") # with headers = True
print(music_data.show())
print(music_data.groupby('artist').count().show()) # to get count grouping by artist
music_data.groupby('artist').max()  # max
music_data.groupby('artist').min()  # min

from pyspark.sql.functions import countDistinct, avg, stddev
print(music_data.select(countDistinct('artist').alias('Distinct Sales')).show())  # counting distinct artist and alias

music_data.orderBy('artist')  # order by artish column in ASC
music_data.orderBy(df['artist'].desc()).show()  # to order in DESC
'''
# dealing with missing data
'''
df.na.drop().show() # drop all the rows where data is missing
df.na.drop(thresh=2).show() # atleaset 2 non-null values should be there
df.na.drop(how='all').show() # drop rows only if all columns are having null values
df.na.drop(how='any').show() # drop rows only if any columns are having null values
df.na.drop(subset=['columns']).show() # drop rows only if specifed columns are having null values

print(df.na.fill('FILL VALUES').show())  # fills null string values with specifed string
df.na.fill('No Name', subset=['Name']).show()  # fills Name colmn null values with No Name string
# we can even fill values with min, max values of any columns
'''

# working with time_stamps
from pyspark.sql.functions import dayofyear, dayofmonth, month, hour, year, weekofyear, date_format
print(df.select([dayofmonth(df['dob']), 'dob']).show()) # shows the day of month, DOB for each DOB
print(df.select(year(df['DOB'])).show())  # showing Year for each DOB










