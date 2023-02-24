import time
import pandas as pd
import os
import glob


from selenium import webdriver

# chrome driver
from selenium.webdriver.chrome.service import Service

# -- Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.wait import WebDriverWait

# Provide the path where you want the data to be downloaded
file_path_CourierOps = "C:/Users/ramy.abdallah/Desktop/Courier Ops.xlsx"
# Provide the path where you want the data to be downloaded
file_path_AmazonPull = "C:/Users/ramy.abdallah/Desktop/AmazonPull.xlsx"
# Provide the path to chromerdriver.exe
service_obj = Service(
    "C:\\Users\\ramy.abdallah\\Documents\\1- Forecasting Files\\11- Improvements to Forecasting\\2-Amazon_Data_ES\\chromedriver_win32\\chromedriver.exe"
)

# The function returns a flag (true: means data was successfully downloaded)
# returns (second argument) data frame of courrier ops
def CourrierOps_DataGrapper(
    start_day, start_month, start_year, end_day, end_month, end_year
):
    driver = webdriver.Chrome(service=service_obj)
    driver.implicitly_wait(60)
    driver.get(
        "https://puro-analytics.cpggpc.ca/QvAJAXZfc/opendoc.htm?document=operations%5Ccourier%20ops%20data%20ver%202.5.qvw&lang=en-US&host=QVS%40ClusterPRD"
    )
    driver.find_element(By.CSS_SELECTOR, "li[rel='DocumentSH120']").click()
    time.sleep(4)
    driver.find_element(By.CSS_SELECTOR, "input[id='inp_33']").click()
    driver.find_element(By.CSS_SELECTOR, "select[class='ui-datepicker-month']").click()
    time.sleep(0.2)
    driver.find_element(
        By.CSS_SELECTOR, "select[class='ui-datepicker-month']"
    ).send_keys(str(start_month))
    driver.find_element(By.CSS_SELECTOR, "select[class='ui-datepicker-month']").click()
    time.sleep(0.4)
    driver.find_element(By.CSS_SELECTOR, "select[class='ui-datepicker-year']").click()
    time.sleep(0.2)
    driver.find_element(
        By.CSS_SELECTOR, "select[class='ui-datepicker-year']"
    ).send_keys(str(start_year))
    time.sleep(0.2)
    driver.find_element(By.CSS_SELECTOR, "select[class='ui-datepicker-year']").click()
    time.sleep(2)
    if start_day == 1:
        driver.find_element(
            By.XPATH, "//a[@class='ui-state-default ui-state-active']"
        ).click()
    else:
        driver.find_element(
            By.XPATH,
            "//a[@class='ui-state-default'][normalize-space()='"
            + str(start_day)
            + "']",
        ).click()

    driver.find_element(By.CSS_SELECTOR, "input[id='inp_28']").click()
    driver.find_element(By.CSS_SELECTOR, "select[class='ui-datepicker-month']").click()
    time.sleep(0.2)
    driver.find_element(
        By.CSS_SELECTOR, "select[class='ui-datepicker-month']"
    ).send_keys(str(end_month))
    driver.find_element(By.CSS_SELECTOR, "select[class='ui-datepicker-month']").click()
    time.sleep(0.4)
    driver.find_element(By.CSS_SELECTOR, "select[class='ui-datepicker-year']").click()
    time.sleep(0.2)
    driver.find_element(
        By.CSS_SELECTOR, "select[class='ui-datepicker-year']"
    ).send_keys(str(end_year))
    driver.find_element(By.CSS_SELECTOR, "select[class='ui-datepicker-year']").click()
    time.sleep(2)
    if end_day == 1:
        driver.find_element(
            By.XPATH, "//a[@class='ui-state-default ui-state-active']"
        ).click()
    else:
        driver.find_element(
            By.XPATH,
            "//a[@class='ui-state-default'][normalize-space()='" + str(end_day) + "']",
        ).click()
    time.sleep(15)
    driver.find_element(By.CSS_SELECTOR, "div[title='Send to Excel']").click()
    wait = WebDriverWait(driver, 180)
    wait.until(
        expected_conditions.presence_of_element_located(
            (By.CSS_SELECTOR, "div[class='ModalDialog_Text']")
        )
    )

    message = driver.find_element(By.CSS_SELECTOR, "div[class='ModalDialog_Text']").text
    # time.sleep(30)
    wait.until(
        expected_conditions.invisibility_of_element_located(
            (By.CSS_SELECTOR, "div[class='ModalDialog_Text']")
        )
    )
    home = os.path.expanduser("~")
    downloadspath = os.path.join(home, "Downloads")
    list_of_files = glob.glob(
        downloadspath + "\*.xlsx"
    )  # * means all if need specific format then *.csv
    latest_file = max(list_of_files, key=os.path.getctime)
    df = pd.read_excel(latest_file)
    df.to_excel(str(file_path_CourierOps))
    driver.close()
    if ("The requested content has been opened in another window" in message) | (
        "Exporting" in message
    ):
        Flag = True
    else:
        Flag = False
    return Flag, df


def amazonDataGrapper():

    driver = webdriver.Chrome(service=service_obj)
    driver.implicitly_wait(20)
    driver.get(
        "https://puro-analytics.cpggpc.ca/QvAJAXZfc/opendoc.htm?document=operations%5Camazon%20performance.qvw&lang=en-US&host=QVS%40ClusterPRD"
    )
    time.sleep(1)
    driver.maximize_window()
    time.sleep(1)
    driver.find_element(By.CSS_SELECTOR, "a[class='qvtr-scroll-right']").click()
    driver.find_element(By.CSS_SELECTOR, "a[class='qvtr-scroll-right']").click()
    time.sleep(3)
    driver.find_element(By.CSS_SELECTOR, "li[order='12']").click()
    time.sleep(2)
    driver.find_element(By.CSS_SELECTOR, "div[title='STOP CLOCK DATE']").click()
    time.sleep(1)
    driver.find_element(By.CSS_SELECTOR, "div[title='STOP CLOCK TERMINAL#']").click()
    time.sleep(1)
    driver.find_element(By.CSS_SELECTOR, "div[title='TIER 2 FLAG']").click()
    time.sleep(1)
    driver.find_element(By.CSS_SELECTOR, "div[title='DELIVERY PIECES']").click()
    time.sleep(3)
    driver.find_element(By.CSS_SELECTOR, "div[title='Send to Excel']").click()
    wait = WebDriverWait(driver, 15)
    wait.until(
        expected_conditions.presence_of_element_located(
            (By.CSS_SELECTOR, "div[class='ModalDialog_Text']")
        )
    )
    time.sleep(3)
    message = driver.find_element(By.CSS_SELECTOR, "div[class='ModalDialog_Text']").text
    home = os.path.expanduser("~")
    downloadspath = os.path.join(home, "Downloads")
    list_of_files = glob.glob(
        downloadspath + "\*.xlsx"
    )  # * means all if need specific format then *.csv
    latest_file = max(list_of_files, key=os.path.getctime)
    df = pd.read_excel(latest_file)
    df.to_excel(str(file_path_AmazonPull))
    if ("The requested content has been opened in another window" in message) | (
        "Exporting" in message
    ):
        Flag = True
    else:
        Flag = False
    return Flag, df


Flag, RecentCourierOpsdf = CourrierOps_DataGrapper(
    start_day=18,
    start_month="Feb",
    start_year=2022,
    end_day=23,
    end_month="Feb",
    end_year=2023,
)
print(Flag)
RecentCourierOpsdf.head()

Flag, amazondf = amazonDataGrapper()
print(Flag)
amazondf.head()
