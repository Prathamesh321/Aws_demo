#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import boto3
import io
from io import StringIO
def lambda_handler(event, context):
    key1=process.env.Access_key
    key2=process.env.Secret_access_key
    s3_file_key = event['Records'][0]['s3']['object']['key'];
    print(s3_file_key)
    print(event)
    bucket = 'sourceetlbucket';
    s3 = boto3.client('s3', aws_access_key_id=key1,  aws_secret_access_key=key2)
    obj = s3.get_object(Bucket=bucket, Key=s3_file_key)
    df1 = pd.read_csv(io.BytesIO(obj['Body'].read()));

    service_name = 's3'
    region_name = 'ap-south-1'
    aws_access_key_id = key1
    aws_secret_access_key = key2

    s3_resource = boto3.resource(
        service_name=service_name,
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    
    if (s3_file_key.endswith('-imdb.csv')==True):
        bucket='targetbucketimdb';
    else:
        bucket='targetetlbucket';
        df1['type'].replace({0: "movie", 1: "series"}, inplace=True);
        
    
    
    df1 = df1.loc[:, ~df1.columns.isin(['Released', 'Awards','Poster','Metascore','imdbID',
                                                    'Production','Website','Response'])];
    csv_buffer = StringIO()
    df1.to_csv(csv_buffer,index=False);
    s3_resource.Object(bucket, s3_file_key).put(Body=csv_buffer.getvalue())

