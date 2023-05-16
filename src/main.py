from data.download_rndc_files import download_rndc_statistics_file
from aws.s3 import upload_file


download_rndc_statistics_file()
upload_file()