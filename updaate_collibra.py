import json
import os
import sys
import boto3
import requests
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from datetime import datetime, timezone
import argparse
s3_client = boto3.client('s3')

args = getResolvedOptions(sys.argv, ['JOB_NAME',
                                    'PROCESS_NAME', 
                                    'file_prefix',
                                    'community_id',
                                    'relation_type_id',
                                    'table_asset_id',
                                    'column_asset_id',
                                    'file_format',
                                    'Technical_Data_Type',
                                    'Is_Nullable',
                                    'CreatedBy_Id',
                                    'CreatedBy_FirstName',
                                    'CreatedBy_UserName',
                                    'CreatedBy_LastName',
                                    'LastModifiedBy_UserName',
                                    'Column_Position',
                                    'secret_name',
                                    'target_bucket_name',
                                    'schema_prefix',
                                    'file_datetime',
                                    'domain_id_raw',
                                    'domain_id_prep',
                                    'domain_id_xfm',
                                    'domain_id_refined',
                                    'raw_table_name',
                                    'prep_table_name',
                                    'xfm_table_name',
                                    'refined_table_name',
                                    'Database_view_to_column',
                                    'Bucket_id',
                                    'Directory_id',
                                    'File_group_id',
                                    'Databse_id',
                                    'Schema_id',
                                    'Database_view_id',
                                    'S3_bucket_to_directory',
                                    'Directory_to_file_group',
                                    'File_group_to_table',
                                    'Database_to_schema',
                                    'Schema_to_database_view',
                                    'source_bucket_raw',
                                    'source_bucket_prep',
                                    'source_bucket_xfm',
                                    'source_prefix_prep_xfm',
                                    'Refine_Database_name',
                                    'Refine_schema_name',
                                    'snowflake_table_name',
                                    'domain_id_snowflake',
                                    'comminity_id_snowflake'
                                    ])

process_name             = args['PROCESS_NAME']
comminity_id_snowflake             = args['comminity_id_snowflake']
snowflake_table_name             = args['snowflake_table_name'].upper()
domain_id_snowflake             = args['domain_id_snowflake']
Refine_Database_name             = args['Refine_Database_name'].upper()
Refine_schema_name             = args['Refine_schema_name'].upper()
source_bucket_xfm             = args['source_bucket_xfm'].upper()
source_bucket_raw             = args['source_bucket_raw'].upper()
source_bucket_prep             = args['source_bucket_prep'].upper()
source_prefix_prep_xfm             = args['source_prefix_prep_xfm'].upper()
Database_to_schema       = args['Database_to_schema']
Schema_to_database_view  = args['Schema_to_database_view']
S3_bucket_to_directory   = args['S3_bucket_to_directory']
Directory_to_file_group  = args['Directory_to_file_group']
File_group_to_table      = args['File_group_to_table']
Bucket_id                = args['Bucket_id']
Directory_id             = args['Directory_id']
File_group_id             = args['File_group_id']
Databse_id             = args['Databse_id']
Schema_id             = args['Schema_id']
Database_view_id             = args['Database_view_id']
Database_view_to_column             = args['Database_view_to_column']
raw_table_name             = args['raw_table_name'].upper()
prep_table_name             = args['prep_table_name'].upper()
xfm_table_name             = args['xfm_table_name'].upper()
refined_table_name             = args['refined_table_name'].upper()
domain_id_xfm             = args['domain_id_xfm']
domain_id_prep             = args['domain_id_prep']
domain_id_raw             = args['domain_id_raw']
domain_id_refined = args['domain_id_refined']
schema_prefix             = args['schema_prefix']
target_bucket_name       = args['target_bucket_name']
file_datetime            = args['file_datetime']
file_prefix              = args['file_prefix'].upper()
COMMUNITY_ID             = args['community_id']
relation_type_id         = args['relation_type_id']
table_asset_id           = args['table_asset_id']
column_asset_id          = args['column_asset_id']
file_format              = args['file_format']
Technical_Data_Type      = args['Technical_Data_Type']
Is_Nullable              = args['Is_Nullable']
CreatedBy_Id             = args['CreatedBy_Id']
CreatedBy_FirstName      = args['CreatedBy_FirstName']
CreatedBy_UserName       = args['CreatedBy_UserName']
CreatedBy_LastName       = args['CreatedBy_LastName']
LastModifiedBy_UserName  = args['LastModifiedBy_UserName']
Column_Position          = args['Column_Position']
secret_name              = args['secret_name']

# Collibra API details

data_info = {
    'Raw': {
        'bucket': source_bucket_raw,
        'directory': file_prefix,
        'file_group': 'ts=YYYY-MM-DD HH:MM:SS'.upper(),
        'table_name': raw_table_name,
        'DOMAIN_ID': domain_id_raw
    },
    'Prep': {
        'bucket': source_bucket_prep,
        'directory': source_prefix_prep_xfm,
        'file_group': 'yyyy_mm_dd_hh_mm_ss'.upper(),
        'table_name': prep_table_name,
        'DOMAIN_ID': domain_id_prep
    },
    'Xfm': {
        'bucket': source_bucket_xfm,
        'directory': source_prefix_prep_xfm,
        'file_group': 'yyyy_mm_dd_hh_mm_ss'.upper(),
        'table_name': xfm_table_name,
        'DOMAIN_ID': domain_id_xfm
    },
    'Refin': {
        'db': Refine_Database_name,
        'schema': Refine_schema_name,
        'db_view': refined_table_name,
        'DOMAIN_ID': domain_id_refined
    }
}

RELATIONSHIP_TYPE_IDS = {
    's3_to_directory': S3_bucket_to_directory,
    'directory_to_file_group': Directory_to_file_group,
    'file_group_to_table': File_group_to_table,
    'db_to_schema': Database_to_schema,
    'schema_to_database_view': Schema_to_database_view
}

client = boto3.client("secretsmanager", region_name="us-east-1")
get_secret_value_response = client.get_secret_value(SecretId=secret_name)
secret = get_secret_value_response['SecretString']
secret = json.loads(secret)
 
collibra_url = secret.get('url')
collibra_auth = secret.get('Authorization')
content_type = secret.get('Content_Type')
 
 
HEADERS = {
    'Authorization': collibra_auth,
    'Content-Type': content_type
}

def get_table_id_from_collibra(table_name,DOMAIN_ID,COMMUNITY_ID):
    params = {
        'offset': '0',
        'limit': '0',
        'countLimit': '-1',
        'name': table_name,
        'nameMatchMode': 'EXACT',
        'domainId': DOMAIN_ID,
        'communityId': COMMUNITY_ID,
        'typeInheritance': 'true',
        'excludeMeta': 'true',
        'sortField': 'NAME',
        'sortOrder': 'ASC',
    }
    print("params:",params)
    response = requests.get(f'{collibra_url}/assets', params=params, headers=HEADERS)
    if response.status_code == 200:
        response_data_find = response.json()
        if response_data_find['results']:
            table_id = response_data_find['results'][0]['id']
            print("table_asset_id_find:", table_id)
            return table_id
        else:
            print(f"No table found with the name: {table_name}")
            return None
    else:
        print(f"Failed to fetch table ID: {response.text}")
        return None

def get_existing_columns_from_collibra(table_id,relation_type_id):
    params = {
        'offset': '0',
        'limit': '0',
        'countLimit': '-1',
        'relationTypeId': relation_type_id,
        'targetId': table_id,
        'sourceTargetLogicalOperator': 'AND',
    }
    response = requests.get(f'{collibra_url}/relations', params=params, headers=HEADERS)
    if response.status_code == 200:
        response_data = response.json()
        existing_columns = [relation['source']['name'] for relation in response_data['results']]
        print("existing_columns_names:", existing_columns)
        return existing_columns
    else:
        print(f"Failed to fetch existing columns: {response.text}")
        return []

def create_table_in_collibra(table_name,DOMAIN_ID,COMMUNITY_ID):
    data = [{
        "name": table_name,
        "domainId": DOMAIN_ID,
        "typeId": table_asset_id,
        "communityId": COMMUNITY_ID
    }]
    response = requests.post(f'{collibra_url}/assets/bulk', headers=HEADERS, json=data)
    if response.status_code in [200, 201]:
        print(f"Successfully created table: {table_name}")
        response_data = response.json()
        table_id = response_data[0]['id']
        return table_id
    else:
        print(f"Failed to create table: {response.text}")
        return None

def add_columns_to_collibra(new_columns, table_id, run_id, existing_columns,DOMAIN_ID,schema_data,relation_type_id,COMMUNITY_ID):
    data = [{
        "name": col,
        "domainId": DOMAIN_ID,
        "typeId": column_asset_id,
        "communityId": COMMUNITY_ID
    } for col in new_columns]
    response = requests.post(f'{collibra_url}/assets/bulk', headers=HEADERS, json=data)
    if response.status_code in [200, 201]:
        print(f"Successfully added new columns: {new_columns}")
        response_data_rel = response.json()
        result_dict = {}
        for i in response_data_rel:
            id_value = i["id"]
            name_value = i["name"]
            result_dict[id_value] = name_value
            print("result_dict:", result_dict)

        #col_att_dict = {column["name"]: {"type": column["type"], "nullable": column.get("nullable")} for column in schema_dict["fields"]}
        
        col_att_dict = {key: {'type': value, 'nullable': True} for key, value in schema_data.items()}
        print("col_att_dict_col_att:",col_att_dict)

        col_datatype_data = []
        count= len(existing_columns)
        #for col_id, col_name in result_dict.items():
        #    attributes = col_att_dict.get(col_name)
        #    print("attributes:", attributes)
        for col_id, col_name in result_dict.items():
            attributes = None
            for field in col_att_dict['fields']['type']:
                if field['name'] == col_name:
                    attributes = field
                    break
            print(f"Column ID: {col_id}, Column Name: {col_name},Attributes: {attributes}")
            if attributes:
                column_data = {
                    "assetId": col_id,
                    "typeId": Technical_Data_Type,
                    "value": attributes["type"]
                }
                col_datatype_data.append(column_data)
                print("col_datatype_data's:", col_datatype_data)

                # Inserting nullable attribute
                nullable_data = {
                    "assetId": col_id,
                    "typeId": Is_Nullable,
                    "value": attributes["nullable"]
                }
                col_datatype_data.append(nullable_data)
                print("col_datatype_data's (with nullable):", col_datatype_data)
                
                # Inserting CreatedBy_Id attribute
                
                CreatedById = {
                    "assetId": col_id,
                    "typeId": CreatedBy_Id,
                    "value": run_id
                }
                col_datatype_data.append(CreatedById)
                print("CreatedById:",CreatedById)
                # CreatedBy_FirstName attribute
                
                CreatedByFirstName = {
                    "assetId": col_id,
                    "typeId": CreatedBy_FirstName,
                    "value": glue_job_name
                }
                col_datatype_data.append(CreatedByFirstName)
                
                # CreatedBy_UserName attribute
                CreatedByUserName = {
                    "assetId": col_id,
                    "typeId": CreatedBy_UserName,
                    "value": "sesdlcollibra"
                }
                col_datatype_data.append(CreatedByUserName)
                
                # CreatedBy_LastName attribute
                
                CreatedByLastName = {
                    "assetId": col_id,
                    "typeId": CreatedBy_LastName,
                    "value": "None"
                }
                col_datatype_data.append(CreatedByLastName)
                
                # LastModifiedBy_UserName attribute
                LastModifiedByUserName = {
                    "assetId": col_id,
                    "typeId": LastModifiedBy_UserName,
                    "value": "sesdlcollibra"
                }
                col_datatype_data.append(LastModifiedByUserName)
                
                # Column_Position attribute
                #for column position number
                count=count+1
                ColumnPosition = {
                     "assetId": col_id,
                     "typeId": Column_Position,
                     "value": count
                }
                col_datatype_data.append(ColumnPosition)
                print("col_datatype_data's:", col_datatype_data)
        # Send the column data to Collibra        
        response_col_data = requests.post(f'{collibra_url}/attributes/bulk', headers=HEADERS, json=col_datatype_data)
        if response_col_data.status_code in [200, 201]:
            print(f"Successfully added column attributes: {col_datatype_data}")
        else:
            print(f"Failed to add column attributes: {response_col_data.text}")
        column_list = []
        for entry in response_data_rel:
            if entry['type']['name'] == "Column":
                asset_id_column = entry['id']
                column_list.append(asset_id_column)
        relation_col = []
        for relation in column_list:
            x = {
                "sourceId": relation,
                "targetId": table_id,
                "typeId": relation_type_id
            }
            relation_col.append(x)
        response = requests.post(f'{collibra_url}/relations/bulk', headers=HEADERS, json=relation_col)
        if response.status_code in [200, 201]:
            print(f"Successfully created relationships for new columns: {new_columns}")
        else:
            print(f"Failed to create relationships: {response.text}")
    else:
        print(f"Failed to fetch columns: {response.text}")
        
        
def drop_columns_from_collibra(columns_to_drop,DOMAIN_ID,COMMUNITY_ID):
    # Fetch the column ID from Collibra
    params = {
        'offset': '0',
        'limit': '0',
        'countLimit': '-1',
        'name': columns_to_drop,
        'nameMatchMode': 'EXACT',
        'domainId': DOMAIN_ID,
        'communityId': COMMUNITY_ID,
        'typeInheritance': 'true',
        'excludeMeta': 'true',
        'sortField': 'NAME',
        'sortOrder': 'ASC',
    }
    response = requests.get(f'{collibra_url}/assets', params=params, headers=HEADERS)
    if response.status_code in [200, 201, 204]:
        response_data_find = response.json()
        if response_data_find['results']:
            column_id = response_data_find['results'][0]['id']
            print("Column ID found:", column_id)

            # Prepare JSON data for deleting the column
            json_data = [column_id]

            # Send JSON data to Collibra API endpoint for deleting the column
            response = requests.delete(f'{collibra_url}/assets/bulk', headers=HEADERS, json=json_data)
            print("delete_col_res:",response)
            if response.status_code in [200, 201, 204]:
                print("Column deleted successfully.")
            else:
                print(f"Failed to delete column: {response.text}")
        else:
            print(f"No column found with the name: {col_to_delete}")
    else:
        print(f"Failed to fetch column ID: {response.text}")
def create_relationship(source_id, target_id, type_id):
    payload = {
        "sourceId": source_id,
        "targetId": target_id,
        "typeId": type_id
    }

    response = requests.post(f'{collibra_url}/relations', headers=HEADERS, json=payload)
    if response.status_code in [200, 201]:
        print(f"Successfully created relationship: {source_id} -> {target_id}")
    else:
        print(f"Failed to create relationship: {response.text}")

def check_or_create_asset(layer_name, name_part, name, DOMAIN_ID, COMMUNITY_ID):
    params = {
        'offset': '0',
        'limit': '0',
        'countLimit': '-1',
        'name': name,
        'nameMatchMode': 'EXACT',
        'domainId': DOMAIN_ID,
        'communityId': COMMUNITY_ID,
        'typeInheritance': 'true',
        'excludeMeta': 'true',
        'sortField': 'NAME',
        'sortOrder': 'ASC'
    }

    response = requests.get(f'{collibra_url}/assets', params=params, headers=HEADERS)
    
    if response.status_code == 200:
        response_data = response.json()
        if response_data['results']:
            existing_id = response_data['results'][0]['id']
            print(f"Asset ID for {name_part}: {existing_id}")
            return existing_id
        else:
            print(f"No asset found with the name: {name}")
            return create_asset(layer_name, name_part, name, DOMAIN_ID, COMMUNITY_ID)
    else:
        print(f"Failed to fetch asset ID for {name_part}: {response.text}")
        return None

def create_asset(layer_name, name_part, name, DOMAIN_ID, COMMUNITY_ID):
    type_id_map = {
        'bucket': Bucket_id,
        'directory': Directory_id,
        'file_group': File_group_id,
        'db': Databse_id,
        'schema': Schema_id,
        'db_view': Database_view_id,
        'table_name': table_asset_id  # Assuming 'table_name' has its own type ID
    }

    payload = {
        'name': name,
        'domainId': DOMAIN_ID,
        'typeId': type_id_map.get(name_part, None),  # Get type ID from map
        'communityId': COMMUNITY_ID
    }

    response = requests.post(f'{collibra_url}/assets', headers=HEADERS, json=payload)

    if response.status_code == 201:
        created_asset = response.json()
        new_id = created_asset['id']
        print(f"Created asset for {name_part}: {new_id}")
        return new_id
    elif response.status_code == 409:
        print(f"Asset already exists for {name_part}. Fetching existing asset ID...")
        # If asset already exists, fetch its ID and return
        return check_or_create_asset(layer_name, name_part, name, DOMAIN_ID, COMMUNITY_ID)
    else:
        print(f"Failed to create asset for {name_part}: {response.text}")
        return None

def get_glue_job_metadata(glue_job_name,*args):
    glue_client = boto3.client('glue')

    try:
        response = glue_client.get_job(JobName=glue_job_name)

        #created_on = format_datetime(response['Job']['CreatedOn'])
        #last_modified_on = format_datetime(response['Job']['LastModifiedOn'])
        #print("last_modified_on:",last_modified_on)
        
        created_by_first_name = None
        if 'CreatedBy' in response['Job']:
            created_by = response['Job']['CreatedBy']
            created_by_datetime = datetime.strptime(created_by, "%Y-%m-%d")
            created_by_first_name = format_datetime(created_by_datetime)

        last_modified_by_first_name = None
        if 'LastModifiedBy' in response['Job']:
            last_modified_by = response['Job']['LastModifiedBy']
            last_modified_by_datetime = datetime.strptime(last_modified_by, "%Y-%m-%d")
            last_modified_by_first_name = format_datetime(last_modified_by_datetime)

        return created_by_first_name,  last_modified_by_first_name

    except glue_client.exceptions.EntityNotFoundException:
        print("Glue job not found.")
        return None, None, None, None

def get_latest_glue_job_run_id(glue_job_name):
    glue_client = boto3.client('glue')

    try:
        # Get the latest job run
        response = glue_client.get_job_runs(JobName=glue_job_name)
        latest_run = response['JobRuns'][0]  # Assuming it's sorted by start time

        # Extracting relevant metadata
        run_id = latest_run['Id']
        started_on = format_datetime(latest_run['StartedOn'])
        #last_modified_on = format_datetime(latest_run['LastModifiedOn'])

        return run_id, started_on

    except glue_client.exceptions.EntityNotFoundException:
        print("Glue job not found.")
        return None, None

def format_datetime(datetime_obj):
    return datetime_obj.strftime('%Y-%m-%d')

def process_layer(layer_name, layer_info):
    DOMAIN_ID = layer_info.get('DOMAIN_ID', None)
    s3_bucket_id = None
    directory_id = None
    file_group_id = None
    db_id = None
    schema_id = None
    db_view_id = None
    table_id = None

    for name_part in ['bucket', 'directory', 'file_group', 'table_name', 'db', 'schema', 'db_view']:
        if name_part in layer_info:
            if name_part == 'bucket':
                s3_bucket_id = check_or_create_asset(layer_name, name_part, layer_info['bucket'], DOMAIN_ID, COMMUNITY_ID)
                if directory_id:
                    create_relationship(s3_bucket_id, directory_id, RELATIONSHIP_TYPE_IDS['s3_to_directory'])
            elif name_part == 'directory':
                directory_id = check_or_create_asset(layer_name, name_part, layer_info['directory'], DOMAIN_ID, COMMUNITY_ID)
                if file_group_id:
                    create_relationship(directory_id, file_group_id, RELATIONSHIP_TYPE_IDS['directory_to_file_group'])
                if s3_bucket_id:
                    create_relationship(s3_bucket_id, directory_id, RELATIONSHIP_TYPE_IDS['s3_to_directory'])
            elif name_part == 'file_group':
                file_group_id = check_or_create_asset(layer_name, name_part, layer_info['file_group'], DOMAIN_ID, COMMUNITY_ID)
                if directory_id:
                    create_relationship(directory_id, file_group_id, RELATIONSHIP_TYPE_IDS['directory_to_file_group'])
                if table_id:  # Handle relationship with table
                    create_relationship(file_group_id, table_id, RELATIONSHIP_TYPE_IDS['file_group_to_table'])
            elif name_part == 'table_name':
                if 'table_name' in layer_info:
                    table_id = check_or_create_asset(layer_name, name_part, layer_info['table_name'], DOMAIN_ID, COMMUNITY_ID)
                    if file_group_id:
                        create_relationship(file_group_id, table_id, RELATIONSHIP_TYPE_IDS['file_group_to_table'])
            elif name_part == 'db':
                db_id = check_or_create_asset(layer_name, name_part, layer_info['db'], DOMAIN_ID, COMMUNITY_ID)
            elif name_part == 'schema':
                schema_id = check_or_create_asset(layer_name, name_part, layer_info['schema'], DOMAIN_ID, COMMUNITY_ID)
                if db_id:
                    create_relationship(db_id, schema_id, RELATIONSHIP_TYPE_IDS['db_to_schema'])
            elif name_part == 'db_view':
                db_view_id = check_or_create_asset(layer_name, name_part, layer_info['db_view'], DOMAIN_ID, COMMUNITY_ID)
                if schema_id:
                    create_relationship(schema_id, db_view_id, RELATIONSHIP_TYPE_IDS['schema_to_database_view'])

    # Handling relationships specific to Refin layer
    if layer_name == 'Refin':
        if db_id and schema_id:
            create_relationship(db_id, schema_id, RELATIONSHIP_TYPE_IDS['db_to_schema'])
            create_relationship(schema_id, db_view_id, RELATIONSHIP_TYPE_IDS['schema_to_database_view'])


def main(glue_job_name, created_by_first_name, last_modified_by_first_name,comminity_id_snowflake,COMMUNITY_ID):
    global schema_data
    global new_columns
    
    raw_bucket = data_info['Raw']['bucket']
    raw_directory = data_info['Raw']['directory']
    raw_file_group = data_info['Raw']['file_group']
    raw_table_id = data_info['Raw'].get('table_name', '')  # Assuming 'table_name' might not always be present

    prep_bucket = data_info['Prep']['bucket']
    prep_directory = data_info['Prep']['directory']
    prep_file_group = data_info['Prep']['file_group']
    prep_table_name = data_info['Prep'].get('table_name', '')  # Assuming 'table_name' might not always be present

    xfm_bucket = data_info['Xfm']['bucket']
    xfm_directory = data_info['Xfm']['directory']
    xfm_file_group = data_info['Xfm']['file_group']
    xfm_table_name = data_info['Xfm'].get('table_name', '')  # Assuming 'table_name' might not always be present

    refin_db = data_info['Refin']['db']
    refin_schema = data_info['Refin']['schema']
    refin_db_view = data_info['Refin']['db_view']

    # Print or use these variables as needed for Glue parameters
    print(f"Raw layer: bucket={raw_bucket}, directory={raw_directory}, file_group={raw_file_group}, table_name={raw_table_name}")
    print(f"Prep layer: bucket={prep_bucket}, directory={prep_directory}, file_group={prep_file_group}, table_name={prep_table_name}")
    print(f"Xfm layer: bucket={xfm_bucket}, directory={xfm_directory}, file_group={xfm_file_group}, table_name={xfm_table_name}")
    print(f"Refin layer: db={refin_db}, schema={refin_schema}, db_view={refin_db_view}")
    
    for layer_name in ['Raw', 'Prep', 'Xfm', 'Refin']:
        layer_info = data_info.get(layer_name, None)
        print(f"Processing operations for {layer_name} layer...")
        
        if not layer_info:
            print(f"No information found for {layer_name} layer. Skipping.")
            continue
        
        process_layer(layer_name, layer_info)
        
    
    # Get the arguments passed to the job
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    

    # Extract existing columns
    layers = ['Raw','Prep','Xfm','Refined','Snowflake']
    for layer in layers:
        if layer == 'Raw':
            s3_key = f"{schema_prefix}schema_{layer}/{file_datetime}/"
            print("s3_key:",s3_key)
            print("target_bucket_name:",target_bucket_name)
            response = s3_client.list_objects_v2(Bucket=target_bucket_name, Prefix=s3_key)
            if 'Contents' in response:
                for obj in response['Contents']:
                    print("obj:",obj['Key'])
                    response_1 = s3_client.get_object(Bucket=target_bucket_name, Key=obj['Key'])
                    print("response_1:",response_1)
                    response_2 = response_1['Body'].read().decode('utf-8')
                    print("The variable, name is of type:", type(response_2))
                    print("response_2:",response_2)
                    schema_data_json = response_2.replace("'", '"')
                    print("The variable, name is of schema:", type(schema_data_json))
                    schema_data_json_1 = schema_data_json.replace('True', 'true').replace('False', 'false')
                    print("schema_data_json:",schema_data_json_1)
                    schema_data_1 = json.loads(schema_data_json_1)
                    print("data_json:", schema_data_1)
                    for field in schema_data_1['fields']:
                        field['name'] = field['name'].upper()
                    schema_data = schema_data_1
                    print("upper_casedata_json:", schema_data)
                    column_names = [field['name'] for field in schema_data['fields']]
                    print("column_names_urgrnt:",column_names)
            print("schema_data_raw:",schema_data)
            DOMAIN_ID = domain_id_raw
            COMMUNITY_ID = COMMUNITY_ID
            table_name = raw_table_name
            print("DOMAIN_ID_RAW:",DOMAIN_ID)
            #getting table_id
            table_id = get_table_id_from_collibra(table_name,DOMAIN_ID,COMMUNITY_ID)
            if not table_id:
                print(f"Table '{table_name}' not found in Collibra. Creating...")
                table_id = create_table_in_collibra(table_name,DOMAIN_ID,COMMUNITY_ID)
                if not table_id:
                    print("Failed to create table in Collibra.")
                    return
                
            # Add new columns if any
            existing_columns = get_existing_columns_from_collibra(table_id,relation_type_id)
            print("existing_columns_raw:",existing_columns)
            columns_to_add = [col for col in column_names if col not in existing_columns]
            if columns_to_add:
                print(f"Adding new columns to table '{table_name}': {columns_to_add}")
                run_id, _ = get_latest_glue_job_run_id(glue_job_name)  # Fetching the run_id
                add_columns_to_collibra(columns_to_add, table_id, run_id, existing_columns,DOMAIN_ID,schema_data,relation_type_id,COMMUNITY_ID)  # Pass run_id here
            else:
                print(f"No new columns to add to table '{table_name}'.")
            columns_to_drop = [col for col in existing_columns if col not in column_names]
            if columns_to_drop:
                print(f"Delete columns to table '{table_name}': {columns_to_drop}")
                drop_columns_from_collibra(columns_to_drop,DOMAIN_ID,COMMUNITY_ID)
            else:
                print(f"No delete columns in table '{table_name}'.")
        elif layer == 'Prep':
            s3_key = f"{schema_prefix}schema_{layer}/{file_datetime}/"
            print("s3_key:",s3_key)
            print("target_bucket_name:",target_bucket_name)
            response = s3_client.list_objects_v2(Bucket=target_bucket_name, Prefix=s3_key)
            if 'Contents' in response:
                for obj in response['Contents']:
                    print("obj:",obj['Key'])
                    response_1 = s3_client.get_object(Bucket=target_bucket_name, Key=obj['Key'])
                    print("response_1:",response_1)
                    response_2 = response_1['Body'].read().decode('utf-8')
                    print("The variable, name is of type:", type(response_2))
                    print("response_2:",response_2)
                    schema_data_json = response_2.replace("'", '"')
                    print("The variable, name is of schema:", type(schema_data_json))
                    schema_data_json_1 = schema_data_json.replace('True', 'true').replace('False', 'false')
                    print("schema_data_json:",schema_data_json_1)
                    schema_data_1 = json.loads(schema_data_json_1)
                    print("data_json:", schema_data_1)
                    for field in schema_data_1['fields']:
                        field['name'] = field['name'].upper()
                    schema_data = schema_data_1
                    print("upper_casedata_json:", schema_data)
                    column_names = [field['name'] for field in schema_data['fields']]
                    print("column_names_urgrnt:",column_names)
            print("schema_data_raw:",schema_data)
            DOMAIN_ID = domain_id_prep
            COMMUNITY_ID = COMMUNITY_ID
            table_name = prep_table_name
            print("DOMAIN_ID_PREP:",DOMAIN_ID)
            #getting table_id
            table_id = get_table_id_from_collibra(table_name,DOMAIN_ID,COMMUNITY_ID)
            if not table_id:
                print(f"Table '{table_name}' not found in Collibra. Creating...")
                table_id = create_table_in_collibra(table_name,DOMAIN_ID,COMMUNITY_ID)
                if not table_id:
                    print("Failed to create table in Collibra.")
                    return
            # Add new columns if any
            existing_columns = get_existing_columns_from_collibra(table_id,relation_type_id)
            columns_to_add = [col for col in column_names if col not in existing_columns]
            if columns_to_add:
                print(f"Adding new columns to table '{table_name}': {columns_to_add}")
                run_id, _ = get_latest_glue_job_run_id(glue_job_name)  # Fetching the run_id
                add_columns_to_collibra(columns_to_add, table_id, run_id, existing_columns,DOMAIN_ID,schema_data,relation_type_id,COMMUNITY_ID)  # Pass run_id here
            else:
                print(f"No new columns to add to table '{table_name}'.")
            columns_to_drop = [col for col in existing_columns if col not in column_names]
            if columns_to_drop:
                print(f"Delete columns to table '{table_name}': {columns_to_drop}")
                drop_columns_from_collibra(columns_to_drop,DOMAIN_ID,COMMUNITY_ID)
            else:
                print(f"No delete columns in table '{table_name}'.")
        elif layer == 'Xfm':
            s3_key = f"{schema_prefix}schema_{layer}/{file_datetime}/"
            print("s3_key:",s3_key)
            print("target_bucket_name:",target_bucket_name)
            response = s3_client.list_objects_v2(Bucket=target_bucket_name, Prefix=s3_key)
            if 'Contents' in response:
                for obj in response['Contents']:
                    print("obj:",obj['Key'])
                    response_1 = s3_client.get_object(Bucket=target_bucket_name, Key=obj['Key'])
                    print("response_1:",response_1)
                    response_2 = response_1['Body'].read().decode('utf-8')
                    print("The variable, name is of type:", type(response_2))
                    print("response_2:",response_2)
                    schema_data_json = response_2.replace("'", '"')
                    print("The variable, name is of schema:", type(schema_data_json))
                    schema_data_json_1 = schema_data_json.replace('True', 'true').replace('False', 'false')
                    print("schema_data_json:",schema_data_json_1)
                    schema_data_1 = json.loads(schema_data_json_1)
                    print("data_json:", schema_data_1)
                    for field in schema_data_1['fields']:
                        field['name'] = field['name'].upper()
                    schema_data = schema_data_1
                    print("upper_casedata_json:", schema_data)
                    column_names = [field['name'] for field in schema_data['fields']]
                    print("column_names_urgrnt:",column_names)
            print("schema_data_xfm:",schema_data)
            DOMAIN_ID = domain_id_xfm
            COMMUNITY_ID = COMMUNITY_ID
            table_name = xfm_table_name
            print("DOMAIN_ID_XFM:",DOMAIN_ID)
            
            #Getting table_id 
            table_id = get_table_id_from_collibra(table_name,DOMAIN_ID,COMMUNITY_ID)
            if not table_id:
                print(f"Table '{table_name}' not found in Collibra. Creating...")
                table_id = create_table_in_collibra(table_name,DOMAIN_ID,COMMUNITY_ID)
                if not table_id:
                    print("Failed to create table in Collibra.")
                    return
            # Add new columns if any
            existing_columns = get_existing_columns_from_collibra(table_id,relation_type_id)
            columns_to_add = [col for col in column_names if col not in existing_columns]
            if columns_to_add:
                print(f"Adding new columns to table '{table_name}': {columns_to_add}")
                run_id, _ = get_latest_glue_job_run_id(glue_job_name)  # Fetching the run_id
                add_columns_to_collibra(columns_to_add, table_id, run_id, existing_columns,DOMAIN_ID,schema_data,relation_type_id,COMMUNITY_ID)  # Pass run_id here
            else:
                print(f"No new columns to add to table '{table_name}'.")
            columns_to_drop = [col for col in existing_columns if col not in column_names]
            if columns_to_drop:
                print(f"Delete columns to table '{table_name}': {columns_to_drop}")
                drop_columns_from_collibra(columns_to_drop,DOMAIN_ID,COMMUNITY_ID)
            else:
                print(f"No delete columns in table '{table_name}'.")
        
        elif layer == 'Refined':
            s3_key = f"{schema_prefix}schema_Xfm/{file_datetime}/"
            print("s3_key:",s3_key)
            print("target_bucket_name:",target_bucket_name)
            response = s3_client.list_objects_v2(Bucket=target_bucket_name, Prefix=s3_key)
            if 'Contents' in response:
                for obj in response['Contents']:
                    print("obj:",obj['Key'])
                    response_1 = s3_client.get_object(Bucket=target_bucket_name, Key=obj['Key'])
                    print("response_1:",response_1)
                    response_2 = response_1['Body'].read().decode('utf-8')
                    print("The variable, name is of type:", type(response_2))
                    print("response_2:",response_2)
                    schema_data_json = response_2.replace("'", '"')
                    print("The variable, name is of schema:", type(schema_data_json))
                    schema_data_json_1 = schema_data_json.replace('True', 'true').replace('False', 'false')
                    print("schema_data_json:",schema_data_json_1)
                    schema_data_1 = json.loads(schema_data_json_1)
                    print("data_json:", schema_data_1)
                    for field in schema_data_1['fields']:
                        field['name'] = field['name'].upper()
                    schema_data = schema_data_1
                    print("upper_casedata_json:", schema_data)
                    column_names = [field['name'] for field in schema_data['fields']]
                    print("column_names_urgrnt:",column_names)
            print("schema_data_xfm:",schema_data)
            DOMAIN_ID = domain_id_refined
            COMMUNITY_ID = COMMUNITY_ID
            table_name = refined_table_name
            print("DOMAIN_ID_XFM:",DOMAIN_ID)
            
            #Getting table_id 
            table_id = get_table_id_from_collibra(table_name,DOMAIN_ID,COMMUNITY_ID)
            if not table_id:
                print(f"Table '{table_name}' not found in Collibra. Creating...")
                table_id = create_table_in_collibra(table_name,DOMAIN_ID,COMMUNITY_ID)
                if not table_id:
                    print("Failed to create table in Collibra.")
                    return
            # Add new columns if any
            existing_columns = get_existing_columns_from_collibra(table_id,Database_view_to_column)
            print("existing_columns_refined:",existing_columns)
            columns_to_add = [col for col in column_names if col not in existing_columns]
            if columns_to_add:
                print(f"Adding new columns to table '{table_name}': {columns_to_add}")
                run_id, _ = get_latest_glue_job_run_id(glue_job_name)  # Fetching the run_id
                add_columns_to_collibra(columns_to_add, table_id, run_id, existing_columns,DOMAIN_ID,schema_data,Database_view_to_column,COMMUNITY_ID)  # Pass run_id here
            else:
                print(f"No new columns to add to table '{table_name}'.")
            columns_to_drop = [col for col in existing_columns if col not in column_names]
            if columns_to_drop:
                print(f"Delete columns to table '{table_name}': {columns_to_drop}")
                drop_columns_from_collibra(columns_to_drop,DOMAIN_ID,COMMUNITY_ID)
            else:
                print(f"No delete columns in table '{table_name}'.")
    
        elif layer == 'Snowflake':
            s3_key = f"{schema_prefix}schema_{layer}/{file_datetime}/"
            print("s3_key:",s3_key)
            print("target_bucket_name:",target_bucket_name)
            response = s3_client.list_objects_v2(Bucket=target_bucket_name, Prefix=s3_key)
            if 'Contents' in response:
                for obj in response['Contents']:
                    print("obj:",obj['Key'])
                    response_1 = s3_client.get_object(Bucket=target_bucket_name, Key=obj['Key'])
                    print("response_1:",response_1)
                    response_2 = response_1['Body'].read().decode('utf-8')
                    print("The variable, name is of type:", type(response_2))
                    print("response_2:",response_2)
                    schema_data_json = response_2.replace("'", '"')
                    print("The variable, name is of schema:", type(schema_data_json))
                    schema_data_json_1 = schema_data_json.replace('True', 'true').replace('False', 'false')
                    print("schema_data_json:",schema_data_json_1)
                    schema_data_1 = json.loads(schema_data_json_1)
                    print("data_json:", schema_data_1)
                    for field in schema_data_1['fields']:
                        field['name'] = field['name'].upper()
                    schema_data = schema_data_1
                    print("upper_casedata_json:", schema_data)
                    column_names = [field['name'] for field in schema_data['fields']]
                    print("column_names_urgrnt:",column_names)
            print("schema_data_snowflake:",schema_data)
            DOMAIN_ID = domain_id_snowflake
            COMMUNITY_ID = comminity_id_snowflake
            table_name = snowflake_table_name
            print("DOMAIN_ID_SNOWFLAKE:",DOMAIN_ID)
            #getting table_id
            table_id = get_table_id_from_collibra(table_name,DOMAIN_ID,COMMUNITY_ID)
            if not table_id:
                print(f"Table '{table_name}' not found in Collibra. Creating...")
                table_id = create_table_in_collibra(table_name,DOMAIN_ID,COMMUNITY_ID)
                if not table_id:
                    print("Failed to create table in Collibra.")
                    return
                
            # Add new columns if any
            existing_columns = get_existing_columns_from_collibra(table_id,relation_type_id)
            print("existing_columns_snowflake:",existing_columns)
            columns_to_add = [col for col in column_names if col not in existing_columns]
            if columns_to_add:
                print(f"Adding new columns to table '{table_name}': {columns_to_add}")
                run_id, _ = get_latest_glue_job_run_id(glue_job_name)  # Fetching the run_id
                add_columns_to_collibra(columns_to_add, table_id, run_id, existing_columns,DOMAIN_ID,schema_data,relation_type_id,COMMUNITY_ID)  # Pass run_id here
            else:
                print(f"No new columns to add to table '{table_name}'.")
            columns_to_drop = [col for col in existing_columns if col not in column_names]
            if columns_to_drop:
                print(f"Delete columns to table '{table_name}': {columns_to_drop}")
                drop_columns_from_collibra(columns_to_drop,DOMAIN_ID,COMMUNITY_ID)
            else:
                print(f"No delete columns in table '{table_name}'.")
                
            
            
if __name__ == "__main__":
    if '--JOB_NAME' not in sys.argv:
        parser = argparse.ArgumentParser()
        parser.add_argument('--JOB_NAME', required=True, help='Name of the Glue job')
        args = parser.parse_args()
        job_name = args.JOB_NAME
    else:
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        glue_job_name = args['JOB_NAME']
        print("Job glue_job_name:", glue_job_name)  # Replace 'your_glue_job_name' with the actual Glue job name
    created_by_first_name, last_modified_by_first_name = get_glue_job_metadata(glue_job_name)
    main(glue_job_name, created_by_first_name, last_modified_by_first_name,comminity_id_snowflake,COMMUNITY_ID)
