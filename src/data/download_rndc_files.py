from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import os

def download_rndc_statistics_file():

    URL = r"https://rndc.mintransporte.gov.co/MenuPrincipal/tabid/204/language/es-MX/Default.aspx?returnurl=%2FDefault.aspx"
    ID_SUM = "dnn_ctr678_VigiaPublico_Cat"
    ID_SUM_RES ="dnn_ctr678_VigiaPublico_Resultado"
    ID_ANNO_MES = "dnn_ctr678_VigiaPublico_AnoMesInicial"
    ID_D1 = "dnn_ctr678_VigiaPublico_btEstadisticas"
    SLEEP = 5

    chrome_options = Options()
    #chrome_options.add_argument("--headless") # Run Chrome in headless mode
    #chrome_options.add_argument("--disable-gpu") # Disable GPU acceleration
    chrome_options.add_experimental_option('prefs', {
        'download.default_directory': os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data')),
        'download.prompt_for_download': False,
        'download.directory_upgrade': True,
        'safebrowsing.enabled': True
    })

    driver = webdriver.Chrome(options=chrome_options)
    driver.get(URL)
    driver.maximize_window()

    time.sleep(SLEEP)

    sum_text = driver.find_element("id", ID_SUM).text
    sum_list = sum_text.split(" + ")
    sum_list = [int(x) for x in sum_list]
    sum_result = sum(sum_list)

    search = driver.find_element("id", ID_SUM_RES)
    search.send_keys(sum_result)

    time.sleep(SLEEP)

    period = driver.find_element("id", ID_ANNO_MES)
    period.clear()
    anno_mes = '202302'
    period.send_keys(anno_mes)

    time.sleep(SLEEP)

    estadisticas = driver.find_element("id", ID_D1)
    estadisticas.click()

    time.sleep(50)

    driver.quit()

if __name__=='__main__':
    download_rndc_statistics_file()