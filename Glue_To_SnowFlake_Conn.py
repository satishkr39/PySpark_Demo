import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
print('snow flake job started')
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# we need to place 2 jars files in s3 and paste the arn of 2 jar files in the parameters section of Glue
# jar file names: snowflake-jdbc-3.13.14.jar and spark-snowflake_2.12-2.10.0-spark_3.1.jar

SNOWFLAKE_SOURCE_NAME = 'net.snowflake.spark.snowflake'
SNOWFLAKE_DATABASE = 'DOLPHIN_DB'
SCHEMA = 'PUBLIC'
TABLE_NAME = 'MY_ORDERS'
sfparams = {"sfUrl": "https://pn56976_deloittetraining.snowflakecomputing.com/", "sfUser": "",
                     "sfPassword": "",
                     "sfDatabase": SNOWFLAKE_DATABASE, "sfSchema": SCHEMA, "sfWarehouse": "DOLPHIN_WH"}

df = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfparams).option("dbtable", SNOWFLAKE_DATABASE+"."+SCHEMA+"."+TABLE_NAME).load()

print(df)
df.show()

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()