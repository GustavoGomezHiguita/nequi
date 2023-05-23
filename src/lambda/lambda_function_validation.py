import pandas as pd
import boto3
import json


def lambda_handler(event, context):
    # Obtener el nombre del bucket el archivo desde el evento
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # Validar extensión
    file_extension = key.split('.')[-1]
    fn_check_extension(bucket, key, file_extension)
    
    # Validar periodo
    fn_check_period(bucket, key, file_extension)
    
  
    # LLAMAR AL JOB DE GLUE, PENDIENTE
    
    return{
        'statusCode':200,
        'body':'Validations passed'
    }
    
def fn_check_extension(bucket, key, file_extension):
    if file_extension not in ['xlsx']:
        raise ValueError("Invalid file format. Only .xlsx and .txt files are supported.")
    print('File extension correct.')
    
def fn_check_period(bucket, key, file_extension):
    
    period = float(key.split('_')[-1].split('.')[0])
    
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    data = response['Body'].read()
    
    df = pd.read_excel(data,usecols=['MES'])
    period_avg = df.MES.mean()
        
    if period != period_avg:
        raise ValueError("Invalid period values. The period data does not match the period in the file name")
    print('Period correct.')