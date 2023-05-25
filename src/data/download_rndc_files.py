from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import os
from datetime import datetime, timedelta


URL = r"https://rndc.mintransporte.gov.co/MenuPrincipal/tabid/204/language/es-MX/Default.aspx?returnurl=%2FDefault.aspx"
ID_SUM = "dnn_ctr678_VigiaPublico_Cat"
ID_SUM_RES ="dnn_ctr678_VigiaPublico_Resultado"
ID_ANNO_MES = "dnn_ctr678_VigiaPublico_AnoMesInicial"
ID_EST = "dnn_ctr678_VigiaPublico_btEstadisticas"
ID_TIE = "dnn_ctr678_VigiaPublico_btRemesas"
ID_ANT = "dnn_ctr678_VigiaPublico_btModeloAno"
ANT_YEAR = '2021'
#NUM_PERIODS = 10
SLEEP_SHORT = 5
SLEEP_LONG = 50
base_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))


def fn_chrome_options(folder):

    download_directory = os.path.join(base_directory, 'data', folder)
    chrome_options = Options()
    chrome_options.add_experimental_option('prefs', {
        'download.default_directory': download_directory,
        'download.prompt_for_download': False,
        'download.directory_upgrade': True,
        'safebrowsing.enabled': True
    })

    return chrome_options

def fn_get_downloaded_file_names(folder):
    download_directory = os.path.join(base_directory, 'data', folder)
    file_names = []
    for file_name in os.listdir(download_directory):
        file_names.append(file_name)
    return file_names

def fn_list_period(folder,periods_amount=0,just_year=False):
    ls_periods = []
    if just_year == False:
        current_date = datetime.now()
        previous_date = current_date - timedelta(days=30)

        for _ in range(periods_amount):
            year_month = previous_date.strftime("%Y%m")
            ls_periods.append(year_month)
            previous_date -= timedelta(days=30)
    else:
        ls_periods.append(ANT_YEAR)

    ls_downloaded_files = fn_get_downloaded_file_names(folder)

    ls_result = []

    # for item in ls_downloaded_files:
    #     exclude = False
    #     for value in ls_periods:
    #         if str(value) in item:
    #             exclude = True
    #             break
    #     if not exclude:
    #         ls_result.append(value)

    for period in ls_periods:
        exclude = False
        for file_name in ls_downloaded_files:
            if str(period) in file_name:
                exclude = True
                break
        if not exclude:
            ls_result.append(period)

    print(f'folder: {folder} | periodos a consulta: {ls_result}')

    return ls_result

def fn_sum_check(driver):

    sum_text = driver.find_element("id", ID_SUM).text
    sum_list = sum_text.split(" + ")
    sum_list = [int(x) for x in sum_list]
    sum_result = sum(sum_list)

    search = driver.find_element("id", ID_SUM_RES)
    search.clear()
    search.send_keys(sum_result)

    time.sleep(SLEEP_SHORT)

def fn_create_drive(folder):
    chrome_options = fn_chrome_options(folder)
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(URL)
    driver.maximize_window()
    return driver

def fn_download_file(folder,number_periods):

    if folder == 'estadisticas':
        id_boton = ID_EST
        ls_period = fn_list_period(folder,number_periods)
    elif folder == 'tiempos_logisticos':
        id_boton = ID_TIE
        ls_period = fn_list_period(folder,number_periods)
    elif folder == 'antiguedad_vehiculos_y_combustible':
        id_boton = ID_ANT
        ls_period = fn_list_period(folder,just_year=True)
    else:
        return f"Error: no hay folder llamado {folder}"
    
    if len(ls_period)==0:
        print (f'folder: {folder } | sin archivos por descargar')
        return 
    
    driver = fn_create_drive(folder)

    time.sleep(SLEEP_SHORT)

    fn_sum_check(driver)

    period = driver.find_element("id", ID_ANNO_MES)

    index=0
    while index<len(ls_period):
        period_value = ls_period[index]
        try:
            period = driver.find_element("id", ID_ANNO_MES)
            period.clear()
            period.send_keys(period_value)

            time.sleep(SLEEP_SHORT)

            tiempos_log = driver.find_element("id", id_boton)
            tiempos_log.click()

            time.sleep(SLEEP_LONG)
            index += 1
        except:
            driver.quit()
            driver = fn_create_drive(folder)
            fn_sum_check(driver)

    driver.quit()

def fn_download_files(number_periods:int):

    fn_download_file('estadisticas',number_periods)
    fn_download_file('tiempos_logisticos',number_periods)
    fn_download_file('antiguedad_vehiculos_y_combustible',0)

if __name__=='__main__':
    fn_download_files(11)