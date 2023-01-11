from bmkg_to_s3 import extract_1, transform_1, load_1
from s3_to_snowflake import extract_2, transform_2, load_2
from airflow.decorators import task, dag, task_group
from datetime import datetime, timedelta

# task group
@task_group
def bmkg_to_s3_bucket():

    @task 
    def extract_data_from_bmkg():

        return extract_1()

    @task 
    def transform_into_json(xml):

        return transform_1(xml)

    @task 
    def load_into_s3(data):

        return load_1(data)

    
    return load_into_s3(transform_into_json(extract_data_from_bmkg()))

@task_group
def s3_bucket_to_snowflake():

    @task 
    def extract_json_from_s3():

        return extract_2()

    @task 
    def transform_into_dataframe(json_files):

        return transform_2(json_files)

    @task 
    def load_into_snowflake(data):

        return load_2(data)

    load_into_snowflake(transform_into_dataframe(extract_json_from_s3()))


# dag
@dag(

    'weather_analytics',
    start_date=datetime(2023,1,11),
    schedule="0 12 * * *"

)
def dag():

    group1 = bmkg_to_s3_bucket()
    group2 = s3_bucket_to_snowflake()

    group1 >> group2

dag()