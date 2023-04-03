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
from selenium.webdriver import ActionChains
from seleniumbase import BaseCase

# Important: the file should be already in the folder
Upload_file_path = "File_Path"
Your_Email = "Email"
Your_Password = "Password"
# Provide the path to chromerdriver.exe
service_obj = Service(
    "C:\\Users\\ramy.abdallah\\Documents\\1- Forecasting Files\\11- Improvements to Forecasting\\2-Amazon_Data_ES\\chromedriver_win32\\chromedriver.exe"
)


def teamFileUpload(file_path, Email, Password):
    driver = webdriver.Chrome(service=service_obj)
    driver.implicitly_wait(30)
    # 30 seconds is max time out.. 2 seconds (3 seconds save)
    driver.get(
        "https://purolator.sharepoint.com/sites/OperationsForecasts-OpsDashboard/Shared%20Documents/Forms/AllItems.aspx?id=%2Fsites%2FOperationsForecasts%2DOpsDashboard%2FShared%20Documents%2FInput%20Files&viewid=cfe9b4f8%2D081a%2D4715%2Daedc%2D4b38b888a919"
    )
    # Set Login Info
    driver.find_element(By.XPATH, "//input[@type='email']").send_keys(Email)
    driver.find_element(By.XPATH, "//input[@type='submit']").click()
    time.sleep(2)
    wait = WebDriverWait(driver, 20)
    wait.until(
        expected_conditions.presence_of_element_located(
            (By.XPATH, "//input[@id='input28']")
        )
    )
    wait.until(
        expected_conditions.presence_of_element_located(
            (By.XPATH, "//input[@id='input36']")
        )
    )
    driver.find_element(By.XPATH, "//input[@id='input28']").clear()
    driver.find_element(By.XPATH, "//input[@id='input28']").send_keys(Email)
    driver.find_element(By.XPATH, "//input[@id='input36']").send_keys(Password)
    time.sleep(2)
    driver.find_element(By.XPATH, "//input[@type='submit']").click()
    time.sleep(2)
    driver.find_element(By.XPATH, "//input[@id='idBtn_Back']").click()
    time.sleep(4)
    driver.maximize_window()
    time.sleep(2)
    driver.find_element(
        By.XPATH, "//a[@aria-label='Click or enter to return to classic SharePoint']"
    ).click()
    time.sleep(2)
    driver.find_element(By.ID, "QCB1_Button2").click()
    time.sleep(1.5)
    winHandleBefore = driver.window_handles[0]
    iframe = driver.find_element(By.XPATH, "//iframe[@class='ms-dlgFrame']")
    driver.switch_to.frame(iframe)
    # file_path = "C:/Users/ramy.abdallah/Desktop/recentactuals.xlsx"
    driver.find_element(By.ID, "ctl00_PlaceHolderMain_ctl02_ctl04_InputFile").send_keys(
        file_path
    )
    driver.find_element(
        By.ID, "ctl00_PlaceHolderMain_ctl02_ctl04_OverwriteSingle"
    ).click()
    driver.find_element(By.XPATH, "//input[@value='OK']").click()
    driver.switch_to.default_content()
    driver.find_element(
        By.XPATH, "//button[@id='ms-conflictDlgReplaceBtn']").click()
    wait = WebDriverWait(driver, 20)
    wait.until(expected_conditions.presence_of_element_located(
        (By.ID, "QCB1_Button2")))
    # Upload time
    time.sleep(15)
    return print("File Uploaded Successfully")


print(
    teamFileUpload(file_path=Upload_file_path,
                   Email=Your_Email, Password=Your_Password)
)
