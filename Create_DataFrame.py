from pyspark.sql import SparkSession
import os
import sys
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, ArrayType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Create DataFrame from RDD
spark = SparkSession.builder.appName('myApp').getOrCreate()
'''
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


'''
# CREATING EMPTY RDD AND DATAFRAMES
'''
print(spark.sparkContext.emptyRDD()) # creating empty rdd
print(spark.sparkContext.parallelize([])) #empty RDD by using Parallelize

# creating empty df with schema
my_schema = StructType([StructField('Roll',IntegerType(), True),
                        StructField('Name', StringType(), True),
                        StructField('School', StringType(), True)])

my_empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), my_schema)
print(my_empty_df.printSchema())

# Create Empty DataFrame with Schema
print(spark.createDataFrame([], my_schema))
print(spark.createDataFrame([], StructType([])).printSchema())# Empty DataFrame without Schema (no columns)
'''
# convert pyspark df to pandas df
'''
data = [("James","","Smith","36636","M",60000),
        ("Michael","Rose","","40288","M",70000),
        ("Robert","","Williams","42114","",400000),
        ("Maria","Anne","Jones","39192","F",500000),
        ("Jen","Mary","Brown","","F",0)]

columns = ["first_name","middle_name","last_name","dob","gender","salary"]
pysparkDF = spark.createDataFrame(data = data, schema = columns)
print(pysparkDF.printSchema())
print(pysparkDF.show(truncate=False))
print(type(pysparkDF))
pandas_df = pysparkDF.toPandas()
print(type(pandas_df))
'''

# show() method
'''
# syntax: def show(self, n=20, truncate=True, vertical=False):
df.show(truncate=False) #Display full column contents
df.show(2,truncate=False) # Display 2 rows and full column contents
df.show(2,truncate=25) # Display 2 rows & column values 25 characters
df.show(n=3,truncate=25,vertical=True) # Display DataFrame rows & columns vertically
'''

# structType and StructField
'''
schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])

# Nested Data
structureData = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
  ]

# nested schema
structureSchema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('id', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])

# adding and changing struct of dataframe
from pyspark.sql.functions import col,struct,when
updatedDF = df2.withColumn("OtherInfo",
    struct(col("id").alias("identifier"),
    col("gender").alias("gender"),
    col("salary").alias("salary"),
    when(col("salary").cast(IntegerType()) < 2000,"Low")
      .when(col("salary").cast(IntegerType()) < 4000,"Medium")
      .otherwise("High").alias("Salary_Grade")
  )).drop("id","gender","salary")

updatedDF.printSchema()
updatedDF.show(truncate=False)

#  if a Column Exists in a DataFrame

print(df.schema.fieldNames.contains("firstname")) # checks only columns
print(df.schema.contains(StructField("firstname",StringType,true)))  # check column and datatype
'''

# row type
'''
from pyspark.sql import Row
row = Row('James', 40)
print(type(row))
print(row[1])  # access 1st index item
named_row = Row(name='Satihs', roll='40') # using named arguments
print(named_row.name)  # access using named arguments

# using Row class on RDD
data = [Row(name="James,,Smith",lang=["Java","Scala","C++"],state="CA"),
    Row(name="Michael,Rose,",lang=["Spark","Java","C++"],state="NJ"),
    Row(name="Robert,,Williams",lang=["CSharp","VB"],state="NV")]
rdd=spark.sparkContext.parallelize(data)
print(rdd.collect())

# access data using properties
collData=rdd.collect()
for row in collData:
    print(row.name + "," +str(row.lang))
'''
# column class
'''
from pyspark.sql.functions import lit

colObj = lit('My Column String')
print(type(colObj))  # o/p: <class 'pyspark.sql.column.Column'>
print(colObj) # o/p: Column<'My Column String'>

#accessing column obj
data = [('satish', 'kumar', 40),
        ('KUmar', 'SIngh', 65),
        ('Shuvo', 'Mondal', 76)]

df = spark.createDataFrame(data=data).toDF('firstName', 'lastName', 'ag')
print(df.printSchema())
print(df.select('firstName').show()) # selecting only firstName column
print(df.select(['firstName', 'lastName']).show()) # selecting firstName and lastName column
from pyspark.sql.functions import col
print(df.select(col('firstName')).show()) # selecting firstName using col()

data=[(100,2,1),(200,3,4),(300,4,4)]
df=spark.createDataFrame(data).toDF("col1","col2","col3")

print(df.sort(df.select(['firstName'])))#  Sort the DataFrame columns by Ascending

#Arthmetic operations
df.select(df.col1 + df.col2).show()
df.select(df.col1 - df.col2).show()
df.select(df.col1 * df.col2).show()
df.select(df.col1 / df.col2).show()
df.select(df.col1 % df.col2).show()
'''

from pyspark.sql.functions import col, column, udf

# select()
'''
data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]

df = spark.createDataFrame(data,['firstName', 'lastName', 'Country', 'City'])
print(df.printSchema())
print(df.show())

# selecting sinlge/multiple columns
print(df.select("firstName","lastName").show())
print(df.select(df.firstName,df.lastName).show())
print(df.select(df["firstName"],df["lastName"]).show())
# selecting using col()
print(df.select(col('firstName'), col('lastName')).show())

# Select All columns from List
#df.select(*columns).show()

# Select All columns
df.select([col for col in df.columns]).show()
df.select("*").show()

df.select(df.columns[:3]).show(3) #Selects first 3 columns and top 3 rows

df.select(df.columns[2:4]).show(3) #Selects columns 2 to 4  and top 3 rows
'''

# collect()
'''
dept = [("Finance",10),("Marketing",20), ("Sales",30),("IT",40) ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
print(deptDF.show(truncate=False))
dataCollect = deptDF.collect()  # using collect()
print(dataCollect)
for row in dataCollect:
    print(row['dept_name'] + "," +str(row['dept_id']))
'''
# withColumn()
'''
data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)

print(df.withColumn("salary", col("salary").cast("Integer")).show()) # Change DataType using withColumn()
print(df.withColumn("salary", col("salary") * 100).show()) # Update The Value of an Existing Column
print(df.withColumn("CopiedColumn",col("salary")* -1).show())# Create a Column from an Existing
print(df.withColumnRenamed("gender","sex").show(truncate=False) )# Rename Column Name
print(df.drop("salary").show())  # drop a column
'''

# withColumnRenamed()
'''
dataDF = [(('James','','Smith'),'1991-04-01','M',3000),
  (('Michael','Rose',''),'2000-05-19','M',4000),
  (('Robert','','Williams'),'1978-09-05','M',4000),
  (('Maria','Anne','Jones'),'1967-12-01','F',4000),
  (('Jen','Mary','Brown'),'1980-02-17','F',-1)
]
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
schema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('dob', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('gender', IntegerType(), True)
         ])

df = spark.createDataFrame(data = dataDF, schema = schema)
print(df.printSchema())

print(df.withColumnRenamed("dob","DateOfBirth").printSchema()) # prints schema with modified column name
print(df.printSchema())  # old schema only old column name
df2 = df.withColumnRenamed("dob", "DateOfBirth").withColumnRenamed("salary", "salary_amount") # to rename mulitple columns
print(df2.printSchema()) # with modified column names


newColumns = ["newCol1","newCol2","newCol3","newCol4"]
df.toDF(*newColumns).printSchema()
'''

# filter()
'''
data = [
    (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
    (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
    (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
    (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
    (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
    (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
 ]

schema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('languages', ArrayType(StringType()), True),
    StructField('state', StringType(), True),
    StructField('gender', StringType(), True)
])
df = spark.createDataFrame(data, schema)

schema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('languages', ArrayType(StringType()), True),
    StructField('state', StringType(), True),
    StructField('gender', StringType(), True)
])
print(df.show())

print(df.filter(df.state == "OH").show(truncate=False)) # Using equals condition
print(df.filter(col("state") == "OH").show(truncate=False)) # using col to filter
#Using SQL Expression
print(df.filter("gender != 'M'").show()) #For not equal
print(df.filter("gender <> 'M'").show())
print(df.filter("gender == 'M'").show())
print(df.filter( (df.state  == "OH") & (df.gender  == "M")).show(truncate=False) ) # multiple filter


#Filter IS IN List values
li=["OH","CA","DE"]
print(df.filter(df.state.isin(li)).show())

df.filter(df.state.startswith("N")).show() # Using startswith
df.filter(df.state.endswith("H")).show() #using endswith
df.filter(df.state.contains("H")).show() #contains
df.filter(df.name.like("%rose%")).show() # like - SQL LIKE pattern
df.filter(df.name.rlike("(?i)^*rose$")).show() # rlike - SQL RLIKE pattern (LIKE with Regex):This check case insensitive
df.filter(df.name.lastname == "Williams").show(truncate=False) # filtering on NEsted columns

# distinct and dropDuplicates()
distinctDF = df.distinct()
dropDisDF = df.dropDuplicates(["department","salary"])
'''

# sort and orderBY
'''
simpleData = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Raman","Finance","CA",99000,40,24000), \
    ("Scott","Finance","NY",83000,36,19000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]
columns= ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data = simpleData, schema = columns)
# sort()
df.sort("department","state").show(truncate=False)
df.sort(col("department"),col("state")).show(truncate=False)
#orderBy

df.sort(df.department.asc(),df.state.desc()).show(truncate=False)
df.sort(col("department").asc(),col("state").desc()).show(truncate=False)
df.orderBy(col("department").asc(),col("state").desc()).show(truncate=False)

# using raw sql
df.createOrReplaceTempView("EMP")
spark.sql("select employee_name,department,state,salary,age,bonus from EMP ORDER BY department asc").show(truncate=False)
'''
# group by
'''
simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]

schema = ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data=simpleData, schema = schema)
print(df.groupby('department').count().show())
print(df.groupBy("department").sum("salary").show(truncate=False))
# groupBy and aggregate on two or more DataFrame columns, below example does group by on department,state
# and does sum() on salary and bonus columns.
print(df.groupBy("department","state").sum("salary","bonus").show())
# Using agg() aggregate function we can calculate many aggregations at a time on a single statement using PySpark SQL
# aggregate functions sum(), avg(), min(), max() mean() e.t.c.
print(df.groupBy("department").agg(sum("salary").alias("sum_salary"), sum("bonus").alias("sum_bonus"),
         max("bonus").alias("max_bonus")).show(truncate=False))
'''

# joins
# emp dataset
'''
emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
# department dataset
dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)

print(empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner").show(truncate=False)) # inner join
print(empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"outer").show(truncate=False)) # outer join
print(empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"full").show(truncate=False)) # full join
print(empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"fullouter").show(truncate=False)) # full outer
empDF.join(deptDF, empDF("emp_dept_id") == deptDF("dept_id"), "left").show() # left join
empDF.join(deptDF, empDF("emp_dept_id") == deptDF("dept_id"), "leftouter").show() # left outer
# RIGHT JOIN: right & rightouter

# Joins using RAW SQL
empDF.createOrReplaceTempView("EMP")  # creating table 1
deptDF.createOrReplaceTempView("DEPT") # creating table 2
joinDF = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id").show(truncate=False)
joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id").show(truncate=False)

df1.join(df2,df1.id1 == df2.id2,"inner").join(df3,df1.id1 == df3.id3,"inner") # joining 2 or more tables
'''

# union and unionAll
'''
simpleData = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000) \
  ]
columns= ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data = simpleData, schema = columns)

simpleData2 = [("James","Sales","NY",90000,34,10000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]
columns2= ["employee_name","department","state","salary","age","bonus"]
df2 = spark.createDataFrame(data = simpleData2, schema = columns2)
unionDF = df.union(df2) # union which includes duplicates
print(unionDF.show())

disDF = df.union(df2).distinct() # merge without duplicates
print(disDF.show())

# unionByName
merged_df = df1.unionByName(df2, allowMissingColumns=True)
'''

# UDF
'''
columns = ["Seqno","Name"]
data = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

df = spark.createDataFrame(data=data,schema=columns)
# defining funciton
def convertCase(str):
    resStr=""
    arr = str.split(" ")
    for x in arr:
       resStr= resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr


convertUDF = udf(lambda z: convertCase(z),StringType())

# using UDF
print(df.select(col("Seqno"),convertUDF(col("Name")).alias("Name") ).show(truncate=False))
'''



