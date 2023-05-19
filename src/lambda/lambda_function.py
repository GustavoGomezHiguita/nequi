import boto3
import pandas as pd
from unidecode import unidecode

file_encoding = 'latin-1'

def fn_definitions(key):
    folder = key.split('/')[0]
    if folder == 'estadisticas':
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
        extension = '.xlsx'
        period_col = 'MES'
        dict_rename = {'MES':'ANOMES'}


    elif folder == 'tiempos_logisticos':
        dict_fields = {
                    'AÑOMES': int,
                    'NATURALEZA': str,
                    'CODIGO_CARGUE': int,
                    'CODIGO_DESCARGUE': int,
                    'HORAS_VIAJE': float,
                    'HORAS_ESPERA_CARGUE': float,
                    'HORAS_CARGUE': float,
                    'HORAS_ESPERA_DESCARGUE': float,
                    'HORAS_DESCARGUE': float,
                    'CONFIGURACION': str}
        extension = '.txt'
        period_col = 'AÑOMES'
        dict_rename = {
                    'AÑOMES': 'ANOMES',
                    'NATURALEZA':'NATURALEZACARGA',
                    'CONFIGURACION':'COD_CONFIG_VEHICULO',
                    'CODIGO_CARGUE':'CODMUNICIPIOORIGEN',
                    'CODIGO_DESCARGUE':'CODMUNICIPIODESTINO'}

    elif folder == 'antiguedad_vehiculos_y_combustible':
        dict_fields = {
                    'ANOMES': int,
                    'CONFIGURACION': str,
                    'RANGOMODELO': str,
                    'COMBUSTIBLE': str,
                    'PLACAS': int,
                    'VIAJES': int,
                    'VIAJESVACIOS': int,
                    'VALORPACTADO': int,
                    'KILOGRAMOS': int,
                    'GALONES': int,
                    'VIAJESVALORCERO': int,
                    'VIAJESLIQUIDOS': int}
        extension = '.xlsx'
        period_col = 'ANOMES'
        dict_rename = {'CONFIGURACION':'COD_CONFIG_VEHICULO'}

    ls_fields = list(dict_fields.keys())

    return ls_fields, extension, period_col, dict_fields, dict_rename

def lambda_handler(event, context):
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
        raise Exception(f"file:{source_key} | error: {str(e)}")


def fn_transformation(key,file):
    """
    Transformar los datos del dataset validado de la siguiente manera:
    
    1. Cargar solo las columnas definidas en el modelo de datos.
    2. Eliminar registros duplicados.
    3. Convertir a mayúsculas los valores tipo str.
    4. Quitar acentos a valores tipo str.

    Retorna un dataframe con las transformaciones aplicadas.
    
    """
    ls_fields, extension, period_col, dict_fields, dict_rename = fn_definitions(key)

    if extension == '.txt':
        df = pd.read_csv(file,usecols=ls_fields,delimiter='|',encoding=file_encoding,dtype=dict_fields)
    else:
        df = pd.read_excel(file,usecols=ls_fields,dtype=dict_fields)
    df.rename(columns=dict_rename,inplace=True)
    df.drop_duplicates(inplace=True)
    df = df.applymap(lambda x: x.upper() if isinstance(x, str) else x)
    df = df.applymap(lambda x: unidecode(x) if isinstance(x, str) else x)
    if extension == '.txt':
         df = df.groupby(by=['ANOMES','NATURALEZACARGA','CODMUNICIPIOORIGEN','CODMUNICIPIODESTINO','COD_CONFIG_VEHICULO'],as_index=False).mean()

    return df


def fn_validation(key,file):
    """
    Validar el estado de los datos del archivo en 3 etapas:

    1. Validar que la extensión del archivo sea ".xlsx".
    2. Validar que el archivo contenga las columnas del modelo de datos.
    3. Validar que los datos del periodo en el archivo correspondan con el nombre del archivo.

    En caso de no cumplir con alguna de las validaciones no se procede con la transformación de los datos y se registra el log.
    """

    ls_fields, extension, period_col, dict_fields, dict_rename = fn_definitions(key)

    val = True

    # 1.
    if not key.endswith(extension):
        val = False
        return val
    # 2.
    if extension == '.txt':
        df = pd.read_csv(file,nrows=0,delimiter='|',encoding=file_encoding)
    else:
        df = pd.read_excel(file,nrows=0)
    val = all(elem in df.columns.to_list() for elem in ls_fields)

    if val == False:
        return val
    
    # 3.
    if extension == '.txt':
        df = pd.read_csv(file,usecols=[period_col],delimiter='|',encoding=file_encoding)
    else:
        df = pd.read_excel(file,usecols=['period_col'])
    period = float(file.split('_')[-1].split('.')[0])
    period_avg = df.MES.mean()
    val = period == period_avg

    if val == False:
        return val
    
    return val