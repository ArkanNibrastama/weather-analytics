from credentials import *
import boto3
import pandas as pd
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas
import re
from datetime import date
import json

# extract
# get all the files from s3 bucket
def extract_2():

    today = date.today()

    conn = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_ACCESS_SECRET_KEY)
    contents = conn.list_objects(Bucket='arkan-datalake-weather-analytics', Prefix=f'{today}/')['Contents']
    table_list = [content['Key'] for content in contents]
    # replace
    tables_name = [re.sub(f"{today}/|.json", "", name) for name in table_list]
    json_files = []

   

    for table in tables_name:

        json_file = json.loads(conn.get_object(Bucket = 'arkan-datalake-weather-analytics', Key=f'{today}/{table}.json')['Body'].read())
        # append serialize (json->str) json file
        json_files.append(json.dumps(json_file))

    return json_files

# transform
def transform_2(json_files):

    json_files = [json.loads(j) for j in json_files]

    # do some transformation
    # convert json to dataframe
    temperature_per_province = []

    for province_data in json_files:

        temperature = []
        temperature.append(province_data['@domain'])

        parameter = province_data['parameter']
        t = parameter[5]['timerange'][0:8]
        celcius = [c['value'][0]['#text'] for c in t]
        [temperature.append(c) for c in celcius]
        
        temperature_per_province.append(temperature)


    dwh_tables_name = ['tabel_prediksi_suhu_seluruh_provinsi_indonesia']
    dfs = []

    df1 = pd.DataFrame(temperature_per_province, columns=['nama provinsi', 'suhu pada 00.00 hari ini (°C)',
    'suhu pada 00.06 hari ini (°C)', 'suhu pada 12.00 hari ini (°C)', 'suhu pada 18.00 hari ini (°C)',
    'suhu pada 00.00 besok (°C)', 'suhu pada 06.00 besok (°C)', 'suhu pada 12.00 besok (°C)', 'suhu pada 18.00 besok (°C)'])

    # cvt df into string so thus can trough xcom
    dfs.append(json.dumps(df1.to_json()))

    return (dfs, dwh_tables_name)

# load
def load_2(data):

    dfs = data[0]
    dfs = [pd.read_json(json.loads(df)) for df in dfs]
    tables_name = data[1]

    conn = snow.connect(

        user = SNOWFLAKE_USERNAME,
        password = SNOWFLAKE_PASSWORD,
        account = SNOWFLAKE_ACCOUNT,
        warehouse = SNOWFLAKE_WAREHOUSE,
        database = SNOWFLAKE_DATABASE,
        schema = SNOWFLAKE_SCHEMA,
        role = SNOWFLAKE_ROLE

    )

    for df, table in zip(dfs, tables_name):

        write_pandas(conn=conn, df=df, table_name=table, auto_create_table=True)


