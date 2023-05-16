import boto3
import configparser
import os


config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.ini'))

config = configparser.ConfigParser()
config.read(config_path)

AWS_ACCESS_KEY_ID = config['aws']['aws_access_key_id']
AWS_SECRET_ACCESS_KEY = config['aws']['aws_secret_access_key']
BUCKET_NAME = 'prueba-s3-gustavo-gomez'

client = boto3.client('s3',aws_access_key_id=AWS_ACCESS_KEY_ID,aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

def upload_file():
    data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data'))
    file_list = os.listdir(data_path)
    for file_name in file_list:
        full_path = f"{data_path}\{file_name}"
        with open(full_path,'rb') as file:
            object_key = f"data/{file_name}"
            client.upload_fileobj(file, BUCKET_NAME, object_key)
            print('file_uploaded')

if __name__=='__main__':
    upload_file()