from credentials import *
import requests
import xmltodict
import json
import boto3

def extract_1():
    # get xml file from bmkg website
    url = "https://data.bmkg.go.id/DataMKG/MEWS/DigitalForecast/DigitalForecast-Indonesia.xml"
    response = requests.get(url=url)
    xml = response.text

    return xml

def transform_1(xml):
    # convert xml to json
    json_format = xmltodict.parse(xml)
    data = json_format['data']['forecast']

    # get issue date
    issue_date = data['issue']['timestamp']
    formatted_issue_date = issue_date[0:4]+"-"+issue_date[4:6]+"-"+issue_date[6:8]

    # separate data for each province
    provinces = data['area']

    return (formatted_issue_date, provinces)

def load_1(data):

    formatted_issue_date = data[0]
    provinces = data[1]

    # set up connection to s3 bucket
    conn = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_ACCESS_SECRET_KEY)
    for province in provinces:

        # get the json file name
        file_name = province['@domain'].lower().replace(" ", "-")

        if "." in file_name:

            file_name = file_name.replace(".", "")

        # load into s3 bucket
        conn.put_object(

            Body = json.dumps(province),
            Bucket = 'arkan-datalake-weather-analytics',
            Key = f'{formatted_issue_date}/{file_name}.json'

        )

