import sys 
import json
import boto3
import requests
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
import argparse
 
args = getResolvedOptions(sys.argv, [
    'mapp_spec_relationTypeId',
    'target_relationTypeId',
    'source_relationTypeId',
    'complexRelationTypeId',
    'secret_name',
    'map_spec_id',
    'refined_table_name',
    'xfm_table_name',
    'prep_table_name',
    'raw_table_name',
    'domain_id_raw',
    'domain_id_prep',
    'domain_id_xfm',
    'domain_id_refined',
    'Database_view_to_column',
    'relation_type_id',
    'community_id',
    'map_spec_name',
    'domain_mapp_spec_id',
    'snowflake_table_name',
    'domain_id_snowflake',
    'Manifest_bucket_name',
    'Description',
    'Transformation_Logic',
    'schema_prefix',
    'file_datetime'
])

file_datetime = args['file_datetime']
schema_prefix = args['schema_prefix']
relation_type_id = args['relation_type_id']
Description = args['Description']
Transformation_Logic = args['Transformation_Logic']
Manifest_bucket_name = args['Manifest_bucket_name']
snowflake_table_name = args['snowflake_table_name'].upper()
domain_id_snowflake = args['domain_id_snowflake']
domain_mapp_spec_id = args['domain_mapp_spec_id']
map_spec_name = args['map_spec_name']
Database_view_to_column = args['Database_view_to_column']
domain_id_refined = args['domain_id_refined']
domain_id_xfm = args['domain_id_xfm']
domain_id_prep = args['domain_id_prep']
domain_id_raw = args['domain_id_raw']
raw_table_name = args['raw_table_name'].upper()
prep_table_name = args['prep_table_name'].upper()
xfm_table_name = args['xfm_table_name'].upper()
refined_table_name = args['refined_table_name'].upper()
map_spec_id = args['map_spec_id']
mapp_spec_relationTypeId = args['mapp_spec_relationTypeId']
target_relationTypeId = args['target_relationTypeId']
source_relationTypeId = args['source_relationTypeId']
complexRelationTypeId = args['complexRelationTypeId']
secret_name = args['secret_name']
COMMUNITY_ID = args['community_id']

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

# JSON data containing mapping information


def get_column_id_from_collibra(column_name, COMMUNITY_ID):
    descrip_trans_logic_col_ids = ''
    params = {
        'offset': '0',
        'limit': '0',
        'countLimit': '-1',
        'name': column_name,
        'nameMatchMode': 'EXACT',
        'communityId': COMMUNITY_ID,
        'typeInheritance': 'true',
        'excludeMeta': 'true',
        'sortField': 'NAME',
        'sortOrder': 'ASC',
    }
    
    # Assuming HEADERS is defined somewhere in your code
    response = requests.get(f'{collibra_url}/assets', params=params, headers=HEADERS)
    
    if response.status_code == 200:
        response_data_find = response.json()
        
        # Assuming response_data_find is similar to the JSON structure you provided earlier
        results = response_data_find.get('results', [])
        
        for result in results:
            # Assuming you want to check the domain names as per your previous example
            domain_names_to_check = ["S3_PREP", "S3_RAW", "S3_REFINED", "S3_XFM"]
            
            if 'domain' in result and result['domain']['name'] in domain_names_to_check:
                descrip_trans_logic_col_ids= result['id']
                break  # Stop further iteration once a match is found
    
    return descrip_trans_logic_col_ids

def get_table_id_from_collibra(table_name, DOMAIN_ID):
    table_id = ''
    selected_id = ''
    params = {
        'offset': '0',
        'limit': '0',
        'countLimit': '-1',
        'name': table_name,
        'nameMatchMode': 'EXACT',
        'domainId': DOMAIN_ID,
        'typeInheritance': 'true',
        'excludeMeta': 'true',
        'sortField': 'NAME',
        'sortOrder': 'ASC',
    }
    response = requests.get(f'{collibra_url}/assets', params=params, headers=HEADERS)
    if response.status_code == 200:
        response_data_find = response.json()
        if response_data_find['results']:
            table_id = response_data_find['results'][0]['id']
            relation_type = response_data_find['results'][0]['type']['name']
            if relation_type.strip() == "Table":
                selected_id = relation_type_id
                print("table_rel_selected_id:", selected_id)
            elif relation_type.strip() == "Database View":
                selected_id = Database_view_to_column
                print("database_view_selected_id:", selected_id)
            elif relation_type.strip() == "Mapping Specification":
                selected_id = complexRelationTypeId
                print("mapp_selected_id:", selected_id)
        else:
            print(f"No asset found with the name: {table_name}")
            return None
    else:
        print(f"Failed to fetch asset details: {response.text}")
        return None
    return table_id, selected_id

def get_mapping_columns_from_collibra(table_id, selected_id):
    params = {
        'offset': '0',
        'limit': '0',
        'countLimit': '-1',
        'relationTypeId': selected_id,
        'targetId': table_id,
        'sourceTargetLogicalOperator': 'AND',
    }
    print("168params::",params)
    response = requests.get(f'{collibra_url}/relations', params=params, headers=HEADERS)
    if response.status_code == 200:
        response_data = response.json()
        print("172_response_data:::",response_data)
        existing_columns = {relation['source']['name']: relation['source']['id'] for relation in response_data['results']}
        print("174_existing_columns:::",existing_columns)
        return existing_columns
    else:
        print(f"Failed to fetch existing columns: {response.text}")
        return {}

def create_complex_relation(source_id, target_id, description, transformlogic):
    payload = {
    'complexRelationTypeId': complexRelationTypeId,
    'legs': [
        {
            'relationTypeId': source_relationTypeId,
            'assetId': source_id,
        },
        {
            'relationTypeId': target_relationTypeId,
            'assetId': target_id,
        },
        {
            'relationTypeId': mapp_spec_relationTypeId,
            'assetId': map_spec_id,
        },
    ],
    'attributes': {
        Description: [ #Description
            {
                'value': description,
            },
        ],
        Transformation_Logic: [ # Transformation_Logic
            {
                'values': [
                    transformlogic,
                ],
            },
        ],
    },
}
    print("mapping_id's:",payload)
    response = requests.post(f'{collibra_url}/complexRelations', json=payload, headers=HEADERS)
    if response.status_code == 201:
        return response.json()
    else:
        print(f"Failed to create complex relation: {response.text}")
        return None

def get_common_columns(source_columns, target_columns):
    common_columns = {}

    for source_column_name, source_column_id in source_columns.items():
        if '.' in source_column_name:
            source_column_parts = source_column_name.split('.')
            source_column_name = source_column_parts[-1] 
            for target_column_name, target_column_id in target_columns.items():
                if source_column_name == target_column_name:  
                    common_columns[source_column_name] = {'source_id': source_column_id, 'target_id': target_column_id}
                    #print("common_columns:", common_columns)
                    break
        elif source_column_name in target_columns:  
            common_columns[source_column_name] = {'source_id': source_column_id, 'target_id': target_columns[source_column_name]}
            #print("common_columns:", common_columns)

    # If common_columns is empty, populate it by matching columns from source_columns and target_columns using Prep2XFM data
    if not common_columns:
        for source_name, target_names in Prep2XFM.items():
            source_id = source_columns.get(source_name)
            if source_id:
                for target_name in target_names:
                    target_id = target_columns.get(target_name)
                    if target_id:
                        if source_name in common_columns:
                            if isinstance(common_columns[source_name], list):
                                common_columns[source_name].append({'source_id': source_id, 'target_id': target_id})
                                #print("196_common_columns:", common_columns)
                            else:
                                common_columns[source_name] = [common_columns[source_name], {'source_id': source_id, 'target_id': target_id}]
                                #print("198_common_columns:", common_columns)
                        else:
                            common_columns[source_name] = {'source_id': source_id, 'target_id': target_id}
                            #print("202_common_columns:", common_columns)
    print("203_final_common_columns:", common_columns)
    return common_columns

def get_mapp_spec_columns_from_collibra(map_spec_id):
    params = {
        'offset': '0',
        'limit': '0',
        'countLimit': '-1',
        'assetId': map_spec_id,
        'typeId': complexRelationTypeId
    }

    response = requests.get(f'{collibra_url}/complexRelations', params=params, headers=HEADERS)
    if response.status_code == 200:
        response_data = response.json()
        #print("get_mapp_spec_columns_from_collibra::",response_data)
        raw_prep = []
        prep_xfm = []
        xfm_refine = []
        xfm_snowflake =[]
        sourceid = None
        target_wait = None
        
        for result in response_data.get('results', []):
            legs = result.get('legs', [])
            for leg in legs:
                asset_reference = leg.get('assetReference', {})
                display_name = asset_reference.get('displayName', '')
                asset_id = asset_reference.get('id', '')
                role = leg.get('role', '')
                
                if role == 'source':
                    sourceid = asset_id
                    if target_wait:
                        # Process the waiting target with this source
                        layer_name = layers_ids_setup(asset_id)
                        print("source_layer_name::",layer_name)
                        if layer_name == 'S3_RAW':
                            raw_prep.append((asset_id, target_wait))
                            print("source_raw_prep::",raw_prep)
                        elif layer_name == 'S3_PREP':
                            prep_xfm.append((asset_id, target_wait))
                        elif layer_name == 'S3_XFM':
                            layer_name = layers_ids_setup(target_wait)
                            if layer_name == 'S3_REFINED':
                                xfm_refine.append((asset_id, target_wait))
                            else:
                                xfm_snowflake.append((asset_id, target_wait))
                        #elif layer_name == 'S3_REFINED':
                        #    xfm_snowflake.append((asset_id, target_wait))
                        target_wait = None
                        sourceid = None
                elif role == 'target':
                    target_wait=asset_id
                    if sourceid:
                        layer_name = layers_ids_setup(asset_id)
                        print("target_layer_name::",layer_name)
                        if layer_name == 'S3_PREP':
                            raw_prep.append((sourceid, target_wait))
                            print("target_raw_prep::",raw_prep)
                        elif layer_name == 'S3_XFM':
                            prep_xfm.append((sourceid, target_wait))
                        elif layer_name == 'S3_REFINED':
                            xfm_refine.append((sourceid, target_wait))
                        elif layer_name == 'CORE_BASE_NEW':
                            xfm_snowflake.append((sourceid, target_wait))
                        sourceid = None
                        target_wait = None
        return raw_prep, prep_xfm, xfm_refine, xfm_snowflake
    else:
        print(f"Failed to fetch complex relations for map spec: {response.text}")
        return [], [], [], []

def layers_ids_setup(source):
    response = requests.get(f'{collibra_url}/assets/{source}', headers=HEADERS)
    if response.status_code == 200:
        response_data = response.json()
        domain_id = response_data['domain']['id']
        #print("domain_id",domain_id)
        domain_name = response_data['domain']['name']
        #print("domain_name",domain_name)
        return domain_name
    else:
        print(f"Failed to fetch asset details for {source}: {response.text}")
        return None

def mapp_spec_check(mapp_spec):
    source_table_id, source_selected_id = get_table_id_from_collibra(mapp_spec['source_table'], mapp_spec['source_DOMAIN_ID'])
    if not source_table_id:
        return [], [], [], []
    raw_prep, prep_xfm, xfm_refine, xfm_snowflake = get_mapp_spec_columns_from_collibra(source_table_id)
    return raw_prep, prep_xfm, xfm_refine, xfm_snowflake
    
def Get_column_name_id(source_id):
    response = requests.get(f'{collibra_url}/assets/{source_id}', headers=HEADERS)
    #print("339_status_code:",response.status_code)
    if response.status_code == 200:
        response_data = response.json()
        #print("341_response_data:",response_data)
        col_name_comp = response_data['name']
        #print("col_name_comp:",col_name_comp)
        return col_name_comp
    else:
        print("im in get column name")
        
def desc_trns(data,col_name):
    for data in data:
        if data['transformed_column_name'].upper()==col_name:
            #print("data['description'],data['transformation_logic']::",data['description'],data['transformation_logic'])
            return data['description'],data['transformation_logic']

def mapp_layers(source_table_col, target_table_col, checkid_layers ,data):
    source_table_id, selected_id = get_table_id_from_collibra(source_table_col['source_table'], source_table_col['source_DOMAIN_ID'])
    source_columns = get_mapping_columns_from_collibra(source_table_id, selected_id)
    target_table_id, selected_id = get_table_id_from_collibra(target_table_col['target_table'], target_table_col['target_DOMAIN_ID'])
    target_columns = get_mapping_columns_from_collibra(target_table_id, selected_id)

    common_columns = get_common_columns(source_columns, target_columns)
    
    compare_col_list = []
    for column_name, ids in common_columns.items():
        if isinstance(ids, list):  # Handle case where there are duplicates
            for id_pair in ids:
                source_id = id_pair['source_id']
                #print("307_source_id::::", source_id)
                target_id = id_pair['target_id']
                #print("309_target_id::::", target_id)
                if (source_id, target_id) not in checkid_layers:
                    #print("290_main_source_id",source_id)
                    #print("291_main_target_id",target_id)
                    #print("292_checkid_layers:",checkid_layers)
                    #compare_col_list.append((source_id, target_id))
                    col_name=Get_column_name_id(source_id)
                    column_names = [item["transformed_column_name"].upper() for item in data]
                    print("column_names:", column_names)
                    if col_name in column_names:
                        description,transformlogic=desc_trns(data,col_name)
                        #print("description,transformlogic:",description,transformlogic)
                    else:
                        description,transformlogic= 'Null','Null'
                    complex_relation_response = create_complex_relation(source_id, target_id,description, transformlogic)
                    print("main_complex_relation_response:",complex_relation_response)
                 #compare_col_list.append((source_id, target_id))
        else:
            source_id = ids['source_id']
            print("else_source_id::", source_id)
            target_id = ids['target_id']
            print("else_target_id::", target_id)
            if (source_id, target_id) not in checkid_layers:
                #print("309_main_source_id",source_id)
                #print("310_main_target_id",target_id)
                #print("311_checkid_layers:",checkid_layers)
                col_name=Get_column_name_id(source_id)
                column_names = [item["transformed_column_name"].upper() for item in data]
                #print("column_names:", column_names)
                if col_name in column_names:
                    description,transformlogic=desc_trns(data,col_name)
                    #print("description,transformlogic:",description,transformlogic)
                else:
                    description,transformlogic='Null','Null'
                complex_relation_response = create_complex_relation(source_id, target_id,description, transformlogic)
                print("else_complex_relation_response:",complex_relation_response)
                #compare_col_list.append((source_id, target_id))
    #print("compare_col_list", compare_col_list)
           

if __name__ == "__main__":
    s3 = boto3.client('s3')
    bucket_name = Manifest_bucket_name
    #filenames=['cust_comm_preference_manifest.json','Manifest_file_prep_layer.json','Manifest_file_raw_layer.json','Manifest_file_refined&snowflake_layer.json']
    
    filenames = ['raw','prep','xfm','refine','snowflake']
    for file_name in filenames:
        json_file_key=f'{schema_prefix}Manifest_files/{file_datetime}/cust_comm_preference_manifest_{file_name}.json'
        print("426json_file_key:",json_file_key)
        # Download JSON file from S3
        try:
            response = s3.get_object(Bucket=bucket_name, Key=json_file_key)
            #response = s3_client.list_objects_v2(Bucket=bucket_name, Key=json_file_key)
            data = json.loads(response['Body'].read().decode('utf-8'))
            if file_name=='raw':
               raw_data= data
               #print("434_raw_data:",raw_data)
            elif file_name=='prep':
                prep_data = data
                #print("437_prep_data:",prep_data)
            elif file_name=='xfm':
                xfm_data = data
                print("440_xfm_data:",xfm_data)
            elif file_name=='refine':
                refine_data = data
                print("443_refine_data:",refine_data)
            elif file_name=='snowflake':
                snowflake_data = data
                #print("446_snowflake_data:",snowflake_data)
                
                
        except Exception as e:
            print(f"Error reading JSON file from S3: {e}")
            exit()
        
    input_data = None
    
    for item in xfm_data:
        if item.get('transformed_column_name') == 'lineage':
            transformation_logic = item.get('transformation_logic')
            lineage_part = transformation_logic.split('=')[-1].strip()
            #input_data = lineage_part.replace('return lineage', '').strip()
            lineage_part = lineage_part.replace("\nreturn lineage", "")
            input_data = eval(lineage_part)
            break
    #print('100_input_data:: ', input_data)
    #print('101_Type_of_input_data::', type(input_data))
    
    original_dict = {}
    
    # Iterate through input_data and construct transformed_data
    for item in input_data:
        #source_col = item['source_col'].lower().replace('_', '')
        source_col = item['source_col'].upper()
        target_col = item['target_col'].upper()
        
        if isinstance(target_col, list):
            for col in target_col:
                if source_col in original_dict:
                    original_dict[source_col].append(col.upper())  
                else:
                    original_dict[source_col] = [col.upper()]  
        else:
            if source_col in original_dict:
                original_dict[source_col].append(target_col.upper())  
            else:
                original_dict[source_col] = [target_col.upper()]  


    #print(json.dumps(original_dict, indent=4))
    #print('Type of transformed_data : ', type(original_dict))
    #print('transformed_data : ', original_dict)


    converted_dict = {}
    
    for key, value in original_dict.items():
        converted_value = []
        for item in value:
            converted_value.extend(item.split(','))
        converted_dict[key] = converted_value
    
        
    print('Type of transformed_data : ', type(converted_dict))
    print('transformed_data : ', converted_dict)
    Prep2XFM = converted_dict
    
    #descrip_trans_logic = []
    #column_names = [item["transformed_column_name"] for item in xfm_data]
    #print("column_names:", column_names)
    
    #for column_name in column_names:
    #    column_id = get_column_id_from_collibra(column_name, COMMUNITY_ID)
        
     #   if column_id:
      #      descrip_trans_logic.append(column_id)
    
    #print("descrip_trans_logic:", descrip_trans_logic)
    
    
    

    # Fetch source and target assets for mapp_spec_check
    mapp_spec_src_trg = {'source_table': map_spec_name, 'source_DOMAIN_ID': domain_mapp_spec_id}
    raw_prep, prep_xfm, xfm_refine, xfm_snowflake = mapp_spec_check(mapp_spec_src_trg)
    print("raw_prep_existing_mapp:", raw_prep)
    print("prep_xfm_existing_mapp:", prep_xfm)
    print("xfm_refine_existing_mapp:", xfm_refine)
    print("xfm_snowflake_existing_mapp:", xfm_snowflake)
    
    # Details source and target domain ids and table names
    layers = ['Raw-prep', 'Prep-xfm', 'Xfm-refin','xfm-snowflake']
    for layer in layers:
        if layer == 'Raw-prep':
            source_table_col = {'source_table': raw_table_name, 'source_DOMAIN_ID': domain_id_raw}
            target_table_col = {'target_table': prep_table_name, 'target_DOMAIN_ID': domain_id_prep}
            mapp_layers(source_table_col, target_table_col, raw_prep,raw_data)
        elif layer == 'Prep-xfm':
            source_table_col = {'source_table': prep_table_name, 'source_DOMAIN_ID': domain_id_prep}
            target_table_col = {'target_table': xfm_table_name, 'target_DOMAIN_ID': domain_id_xfm}
            mapp_layers(source_table_col, target_table_col, prep_xfm,prep_data)
        elif layer == 'Xfm-refin':
            source_table_col = {'source_table': xfm_table_name, 'source_DOMAIN_ID': domain_id_xfm}
            target_table_col = {'target_table': refined_table_name, 'target_DOMAIN_ID': domain_id_refined}
            mapp_layers(source_table_col, target_table_col, xfm_refine,xfm_data)
        elif layer == 'xfm-snowflake':
            #source_table_col = {'source_table': refined_table_name, 'source_DOMAIN_ID': domain_id_refined}
            source_table_col = {'source_table': xfm_table_name, 'source_DOMAIN_ID': domain_id_xfm}
            target_table_col = {'target_table': snowflake_table_name, 'target_DOMAIN_ID': domain_id_snowflake}
            mapp_layers(source_table_col, target_table_col, xfm_snowflake,refine_data)
