# -*- coding: utf-8 -*-
#!/usr/bin/python3

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys


import os
import time
import argparse


url = 'http://www.eoddata.com/'
stock_exchange = ['NASDAQ', 'NYSE']


def check_by_xpath(driver, xpath):
    try:
        t = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, xpath)))
        return t
    except NoSuchElementException:
        return None


def create_browser(stock_exchange):
    dir_name, file_name = os.path.split(os.path.abspath(__file__))
    
    if stock_exchange=='NASDAQ':
        download_dir = os.path.join(dir_name, 'downloads/NASDAQ_15')
    elif stock_exchange=='NYSE':
        download_dir = os.path.join(dir_name, 'downloads/NYSE_15')

    print("Downloads directory:", download_dir)

    fp = webdriver.FirefoxProfile()
    fp.set_preference("browser.preferences.instantApply", True)
    fp.set_preference("browser.download.folderList", 2)
    fp.set_preference("browser.download.manager.showWhenStarting", False)
    fp.set_preference("browser.helperApps.alwaysAsk.force", False)
    fp.set_preference("browser.download.dir", download_dir)
    fp.set_preference("browser.helperApps.neverAsk.saveToDisk",
                      "text/plain, application/octet-stream, application/binary, text/csv, application/csv, text/comma-separated-values")
    firefox_capabilities = DesiredCapabilities.FIREFOX
    firefox_capabilities['marionette'] = True
    options = FirefoxOptions()
    options.profile = fp
    options.headless = True
    browser = webdriver.Firefox(options=options, capabilities=firefox_capabilities)
    return browser


def close_alert(driver):
    alert = check_by_xpath(driver, "//iframe[contains(@src,'platinum.aspx')]")
    if alert:
        ActionChains(driver).send_keys(Keys.ESCAPE).perform()
        time.sleep(5)


def get_data(url, **credentials):
    browser = create_browser(stock_exchange=credentials['stock_exchange'])
    browser.get(url)
    login = check_by_xpath(browser, "//input[@id='ctl00_cph1_lg1_txtEmail']")
    login.send_keys(credentials['email'])
    password = check_by_xpath(browser, "//input[@id='ctl00_cph1_lg1_txtPassword']")
    password.send_keys(credentials['password'])
    submit_btn = check_by_xpath(browser, "//input[@id='ctl00_cph1_lg1_btnLogin']")
    submit_btn.click()

    download_tab = check_by_xpath(browser, "//a[@title='Download']")
    download_tab.click()
    close_alert(browser)
    
    if stock_exchange=='NASDAQ':
        option = check_by_xpath(browser, f"//select[@id='ctl00_cph1_d1_cboExchange']//option[@value='NASDAQ']")
    elif stock_exchange=='NYSE':
        option = check_by_xpath(browser, f"//select[@id='ctl00_cph1_d1_cboExchange']//option[@value='NYSE']")
    option.click()
    csv_option = check_by_xpath(browser, "//select[@id='ctl00_cph1_d1_cboDataFormat']//option[normalize-space()='Standard CSV']")
    csv_option.click()
    download_btn = check_by_xpath(browser, "//input[@id='ctl00_cph1_d1_btnDownload']")
    download_btn.click()
    time.sleep(10)

    browser.close()
    browser.quit()


def main():
    
    global stock_exchange
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--email", dest="email",
                        type=str, required=True)
    parser.add_argument("-p", "--password", dest="password",
                        type=str, required=True)
    parser.add_argument("-s", "--stock", dest="stock_exchange",
                        type=str, required=True)
    args = parser.parse_args()
    credentials = args.__dict__
    stock_exchange = credentials['stock_exchange']
    get_data(url, **credentials)


if __name__ == '__main__':
    main()

