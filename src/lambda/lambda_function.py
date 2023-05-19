import boto3
import pandas as pd
from unidecode import unidecode
import logging

dict_fields = {
            'MES': int,
            'COD_CONFIG_VEHICULO': str,
            'CONFIG_VEHICULO': str,
            'CODOPERACIONTRANSPORTE': str,
            'CODTIPOCONTENEDOR': str,
            'CODMUNICIPIOORIGEN': int,
            'CODMUNICIPIODESTINO': int,
            'CODMERCANCIA': str,
            'NATURALEZACARGA': str,
            'VIAJESTOTALES': int,
            'KILOGRAMOS': float,
            'GALONES': float,
            'VIAJESLIQUIDOS': int,
            'VIAJESVALORCERO': int,
            'KILOMETROS': float,
            'VALORESPAGADOS': float,
            'CODMUNICIPIOINTERMEDIO': int,
            'KILOMETROSREGRESO': float,
            'KILOGRAMOSREGRESO': float,
            'GALONESREGRESO': float
            }

ls_fields = list(dict_fields.keys())

def lambda_handler(event):
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    source_key = event['Records'][0]['s3']['object']['key']
    destination_bucket = 'rndc-refined'
    s3 = boto3.client('s3')

    try:
        response = s3.get_object(Bucket=source_bucket, Key=source_key)
        file = response['Body'].read()

        validation = fn_validation(source_key,file)

        if validation:
            df_transformed = fn_transformation(source_key,file)
            destination_key = source_key
            s3.put_object(Body=df_transformed, Bucket=destination_bucket, Key=destination_key)

        else:
            raise Exception(f"file:{source_key} | error: No se cumple")

    except Exception as e:
        logging.error(f"file:{source_key} | error: {str(e)}")


def fn_transformation(key,file):
    """
    Transformar los datos del dataset validado de la siguiente manera:
    
    1. Cargar solo las columnas definidas en el modelo de datos.
    2. Eliminar registros duplicados.
    3. Convertir a minúsculas los valores tipo str.
    4. Quitar acentos a valores tipo str.

    Retorna un dataframe con las transformaciones aplicadas.
    
    """

    try:
        df = pd.read_excel(file,usecols=ls_fields,dtype=dict_fields)
        df.drop_duplicates(inplace=True)
        df = df.applymap(lambda x: x.lower() if isinstance(x, str) else x)
        df = df.applymap(lambda x: unidecode(x) if isinstance(x, str) else x)
    except Exception as e:
        logging.error(f"file:{key} | error: {str(e)}")



def fn_validation(key,file):
    """
    Validar el estado de los datos del archivo en 3 etapas:

    1. Validar que la extensión del archivo sea ".xlsx".
    2. Validar que el archivo contenga las columnas del modelo de datos.
    3. Validar que los datos del periodo en el archivo correspondan con el nombre del archivo.

    En caso de no cumplir con alguna de las validaciones no se procede con la transformación de los datos y se registra el log.
    """

    try:

        val = True

        if not key.endswith('.xlsx'):
            val = False
            return val
        
        df = pd.read_excel(file,nrows=0)
        val = all(elem in df.columns.to_list() for elem in ls_fields)

        if val == False:
            return val

        period = float(file.split('_')[-1].split('.')[0])
        df = pd.read_excel(file,usecols=['MES'])
        period_avg = df.MES.mean()
        val = period == period_avg

        if val == False:
            return val
        
        return val
    
    except Exception as e:
        logging.error(f"file:{key} | error: {str(e)}")