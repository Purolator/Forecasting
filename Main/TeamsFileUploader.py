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
Upload_file_path = "C:/Users/ramy.abdallah/Desktop/Courier Ops.xlsx"
Your_Email = "ramy.abdallah@purolator.com"
Your_Password = "Enter Your Password"
# Provide the path to chromerdriver.exe
service_obj = Service(
    "C:\\Users\\ramy.abdallah\\Documents\\1- Forecasting Files\\11- Improvements to Forecasting\\2-Amazon_Data_ES\\chromedriver_win32\\chromedriver.exe"
)


def teamFileUpload(file_path, Email, Password):
    driver = webdriver.Chrome(service=service_obj)
    driver.implicitly_wait(30)
    # 30 seconds is max time out.. 2 seconds (3 seconds save)
    driver.get("https://purolator.sharepoint.com/:f:/r/sites/OperationsNetworkStrategy")
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
    # driver.find_element(By.XPATH, "//input[@type='password']").send_keys("Graace%55")
    driver.find_element(By.XPATH, "//input[@id='input28']").clear()
    driver.find_element(By.XPATH, "//input[@id='input28']").send_keys(Email)
    driver.find_element(By.XPATH, "//input[@id='input36']").send_keys(Password)
    time.sleep(2)
    driver.find_element(By.XPATH, "//input[@type='submit']").click()
    time.sleep(2)
    driver.find_element(By.XPATH, "//input[@id='idBtn_Back']").click()
    time.sleep(4)
    driver.maximize_window()
    driver.find_element(By.XPATH, "//div[@name='Documents']").click()
    time.sleep(2)
    driver.find_element(By.XPATH, "//button[@title='General']").click()
    time.sleep(2)
    driver.find_element(By.XPATH, "//button[@title='Input Files']").click()
    time.sleep(2)

    driver.find_element(
        By.XPATH, "//button[@data-automation-id='returnToClassicButton']"
    ).click()
    time.sleep(2)
    driver.find_element(By.ID, "QCB1_Button2").click()
    time.sleep(2)
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
    driver.find_element(By.XPATH, "//button[@id='ms-conflictDlgReplaceBtn']").click()
    wait = WebDriverWait(driver, 20)
    wait.until(expected_conditions.presence_of_element_located((By.ID, "QCB1_Button2")))
    # Upload time
    time.sleep(15)
    return print("File Uploaded Successfully")


print(
    teamFileUpload(file_path=Upload_file_path, Email=Your_Email, Password=Your_Password)
)
