from data.download_rndc_files import download_rndc_statistics_file
from aws.s3 import fn_upload_folder_files


download_rndc_statistics_file()
fn_upload_folder_files()