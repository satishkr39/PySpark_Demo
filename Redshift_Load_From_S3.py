import pg8000
from pg8000 import connect
from awsglue.utils import getResolvedOptions
import sys
import re
import os
import json
import boto3
import pyspark.sql.functions as F
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3

import base64

from botocore.exceptions import ClientError


class RedshiftInit():

    def __init__(self, config={}, args={}):
        self.config = config
        self.args = args

    def get_Conn_Param(self):
        secret = json.loads(self.get_secret())
        RS_USER = secret["username"]
        RS_PASSWORD = secret["password"]
        RS_HOST = secret["host"]
        RS_PORT = secret["port"]
        RS_DATABASE = secret["dbname"]
        return f'{"username":{RS_USER},"password":{RS_PASSWORD}}'

    def get_secret(self):
        secret_name = "mm"
        region_name = "us-east-1"

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'DecryptionFailureException':
                raise e
            elif e.response['Error']['Code'] == 'InternalServiceErrorException':
                raise e
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                raise e
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                raise e
        else:
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
                return secret
            else:
                decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
                return decoded_binary_secret


class RedshiftLoadFromS3(RedshiftInit):

    def __init__(self):
        pass

    def S3_To_redshift_Load(self):
        conn = json.dumps(self.get_Conn_Param)
        print('conn', conn)
        RS_USER = conn["username"]
        print("username", RS_USER)
        # RS_PASSWORD = secret["password"]
        # RS_HOST = secret["host"]
        # RS_PORT = secret["port"]
        # RS_DATABASE = secret["dbname"]


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
obj = RedshiftLoadFromS3
obj.S3_To_redshift_Load
job.commit()