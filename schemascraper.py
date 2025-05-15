from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
import sys 
import json
import os
import boto3
from py4j.java_gateway import java_import

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Get the arguments passed to the job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 
                                    'target_bucket_name', 
                                    'target_prefix', 
                                    'file_format',
                                    'row_tag_optional_xml', 
                                    'source_bucket_raw',
                                    'source_prefix_raw',
                                    'file_datetime',
                                    'source_bucket_prep',
                                    'source_prefix_prep_xfm',
                                    'source_bucket_xfm',
                                    'SF_CONNECTION_INFO',
                                    'sf_db_name',
                                    'sf_schema_name',
                                    'sf_table_name'
                                    ])

target_bucket_name = args['target_bucket_name']
target_prefix = args['target_prefix']
file_format = args['file_format']
row_tag_optional_xml = args['row_tag_optional_xml']
source_bucket_raw = args['source_bucket_raw']
source_prefix_raw = args['source_prefix_raw']
file_datetime = args['file_datetime']
source_bucket_prep = args['source_bucket_prep']
source_prefix_prep_xfm = args['source_prefix_prep_xfm']
source_bucket_xfm = args['source_bucket_xfm']
sf_db_name               = args['sf_db_name']
sf_schema_name           = args['sf_schema_name']
sf_table_name            = args['sf_table_name']

sql_sf_read = f"select * from {sf_db_name}.{sf_schema_name}.{sf_table_name}"

sf_secret_client = boto3.client("secretsmanager", region_name="us-east-1")


def write_output(schema_dict,s3_input_path,s3_output_path):
    schema_txt = str(schema_dict)
    file_name = os.path.splitext(os.path.basename(s3_input_path))[0]
    print("S3 Output Path:", s3_output_path)
    schema_df = spark.createDataFrame([(schema_txt,)], ["schema_text"])
    schema_df = schema_df.coalesce(1)
    schema_df.write.mode('overwrite').text(s3_output_path)
    print("Schema saved to:", s3_output_path)

def snowflake_connect(sf_sql):
    """Establish Snowflake connection and run the specified query against Snowflake database"""
    get_secret_value_response = sf_secret_client.get_secret_value(
        SecretId=args['SF_CONNECTION_INFO']
    )

    sf_secret = get_secret_value_response['SecretString']
    sf_secret = json.loads(sf_secret)

    db_username = sf_secret.get('db_username')
    db_password = sf_secret.get('db_password')
    db_warehouse = sf_secret.get('db_warehouse')
    db_url = sf_secret.get('db_url')
    db_account = sf_secret.get('db_account')
    db_name = sf_secret.get('db_name')
    db_schema = sf_secret.get('db_schema')

    SNOWFLAKE_SOURCE_NAME = "snowflake"

    java_import(spark._jvm, SNOWFLAKE_SOURCE_NAME)

    spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.enablePushdownSession(spark._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())
    sfOptions = {
    "sfURL" : db_url,
    "sfAccount" : db_account,
    "sfUser" : db_username,
    "sfPassword" : db_password,
    "sfSchema" : sf_schema_name,
    "sfDatabase" : sf_db_name,   # EDS
    "sfWarehouse" : db_warehouse  
    }
    print("SNOWFLAKES connection established : SFOptions Read: ")

    try:
        df = spark.read.format("snowflake").options(**sfOptions).option("query", sf_sql).load()
    except Exception as sfe:
        raise(sfe)
    return df

stages = ['Raw','Prep','Xfm','Snowflake']

for stage in stages:
    
    # Read data into DataFrame based on file format
    if stage == 'Raw':
        s3_input_path = 's3://' + source_bucket_raw + '/' + source_prefix_raw
        print("S3 Input Path:", s3_input_path)
        #file_format.lower() == 'json':
        input_df = spark.read.json(s3_input_path)
        input_df.printSchema()
        
        # Initialize struct_column
        struct_column = None
    
        # Iterating over the fields of the DataFrame schema
        for field in input_df.schema.fields:
            # Checking if the field is of struct type
            if str(field.dataType).startswith("StructType"):
                struct_column = field.name
                break
    
        # Check if struct_column is found
        if struct_column:
            print("struct column:", struct_column)
            dictionary_fields = [field.name for field in input_df.schema[str(struct_column)].dataType.fields]
            output_fields = [col(struct_column + "." + field).alias(struct_column + "." + field) for field in dictionary_fields]
            output_df = input_df.select(output_fields)
            output_df.printSchema()
            input_df = input_df.drop(struct_column)
            input_df.printSchema()
            schema_input_df = input_df.schema
            schema_output_df = output_df.schema
            combined_schema = StructType(schema_input_df.fields + schema_output_df.fields)
            input_df = spark.createDataFrame([], schema=combined_schema)
            input_df.printSchema()
            schema_dict = input_df.schema.jsonValue()
            print(schema_dict)
            s3_output_path = 's3://' + target_bucket_name + '/' + target_prefix +'/'+ source_prefix_prep_xfm + '/schema_' +stage + '/'+ file_datetime + '/'             
            write_output(schema_dict,s3_input_path,s3_output_path)
        else:
            print("No struct column found in the DataFrame")
            schema_dict = input_df.schema.jsonValue()
            print(schema_dict)  
            s3_output_path = 's3://' + target_bucket_name + '/' + target_prefix +'/'+ source_prefix_prep_xfm + '/schema_' +stage +'/'+ file_datetime + '/'             
            write_output(schema_dict,s3_input_path,s3_output_path)
    
    elif stage == 'Prep':
        s3_input_path = 's3://' + source_bucket_prep + '/' + source_prefix_prep_xfm + '/' + file_datetime + '/'
        input_df = spark.read.parquet(s3_input_path)
        input_df.printSchema()
        schema_dict = input_df.schema.jsonValue()
        print(schema_dict)
        s3_output_path = 's3://' + target_bucket_name + '/' + target_prefix +'/'+ source_prefix_prep_xfm + '/schema_' +stage + '/'+ file_datetime + '/' 
        write_output(schema_dict,s3_input_path,s3_output_path)
    elif stage == 'Xfm':
        s3_input_path = 's3://' + source_bucket_xfm + '/' + source_prefix_prep_xfm + '/'+ file_datetime + '/'
        input_df = spark.read.parquet(s3_input_path)
        input_df.printSchema()
        schema_dict = input_df.schema.jsonValue()
        print(schema_dict)
        s3_output_path = 's3://' + target_bucket_name + '/' + target_prefix +'/'+ source_prefix_prep_xfm + '/schema_' +stage + '/'+ file_datetime + '/' 
        write_output(schema_dict,s3_input_path,s3_output_path)
        input_df_refined = input_df.drop('year_effective_timestamp','month_effective_timestamp')
        input_df_refined.printSchema()
        schema_dict = input_df_refined.schema.jsonValue()
        print(schema_dict)
        s3_output_path = 's3://' + target_bucket_name + '/' + target_prefix +'/'+ source_prefix_prep_xfm + '/schema_Refined' + '/'+ file_datetime + '/' 
        write_output(schema_dict,s3_input_path,s3_output_path)
    elif stage == 'Snowflake':
        #s3_input_path = 's3://' + source_bucket_xfm + '/' + source_prefix_prep_xfm + '/'+ file_datetime + '/'
        input_df = snowflake_connect(sql_sf_read)
        input_df.printSchema()
        schema_dict = input_df.schema.jsonValue()
        print(schema_dict)
        s3_output_path = 's3://' + target_bucket_name + '/' + target_prefix +'/'+ source_prefix_prep_xfm + '/schema_' +stage + '/'+ file_datetime + '/' 
        write_output(schema_dict,' ',s3_output_path)
    
    else:
        raise ValueError("Unsupported file format: {}".format(file_format))
