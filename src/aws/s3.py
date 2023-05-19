import boto3
import configparser
import os


config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.ini'))

config = configparser.ConfigParser()
config.read(config_path)

AWS_ACCESS_KEY_ID = config['aws']['aws_access_key_id']
AWS_SECRET_ACCESS_KEY = config['aws']['aws_secret_access_key']
BUCKET_NAME = 'rndc-raw'
base_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))


def fn_upload_files(folder):
    client = boto3.client('s3',aws_access_key_id=AWS_ACCESS_KEY_ID,aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    data_path = os.path.join(base_directory, 'data', folder)
    file_list = os.listdir(data_path)
    s3_file_list = fn_get_bucket_file_names(BUCKET_NAME,folder)
    for file_name in file_list:
        if file_name not in s3_file_list:
            full_path = f"{data_path}\{file_name}"
            with open(full_path,'rb') as file:
                object_key = f"{folder}/{file_name}"
                client.upload_fileobj(file, BUCKET_NAME, object_key)
                print(f'folder: {folder} | file: {file_name} | uploaded')

def fn_get_bucket_file_names(bucket_name,folder):
    session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket_name)
    file_names = []
    for obj in bucket.objects.filter(Prefix=folder):
        file_name = obj.key.split('/')[-1]
        file_names.append(file_name)
    return file_names

def fn_upload_folder_files():

    fn_upload_files('estadisticas')
    fn_upload_files('tiempos_logisticos')
    fn_upload_files('antiguedad_vehiculos_y_combustible')

if __name__=='__main__':
    fn_upload_folder_files()