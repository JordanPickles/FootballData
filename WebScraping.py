from selenium import webdriver 
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException

import argparse
import time
from datetime import datetime
import json
import os
import requests

class UnderstatScraper:
    def __init__(self):
        # Set up Chrome options
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")  # Run in headless mode
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")

        # Initialize the WebDriver
        self.driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options = chrome_options)

    def load_webpage(self, url):
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="league-chemp"]/table/tbody')))
            print('Page loaded successfully')
        except TimeoutException:
            print('Timed out waiting for page to load')


    # def scrape_league(self, league_name):
    #     url = f'https://understat.com/league/{league_name}'
    #     self.driver.get(url)
        
    #     # Allow time for the page to load
    #     time.sleep(5)
        
    #     # Locate the table
    #     table = self.driver.find_element(By.CSS_SELECTOR, 'table')
        
    #     # Locate all rows in the table
    #     rows = table.find_elements(By.TAG_NAME, 'tr')
        
    #     # Iterate through each row and extract data
    #     for row in rows:
    #         cells = row.find_elements(By.TAG_NAME, 'td')
    #         row_data = [cell.text for cell in cells]
    #         print(row_data)

    # def close(self):
    #     self.driver.quit()

if __name__ == '__main__': 
    scraper = UnderstatScraper()
    league_list = ['EPL', 'La_liga', 'Bundesliga', 'Serie_A', 'Ligue_1']
    for league in league_list:
        scraper.load_webpage(f'https://understat.com/league/{league}')

    scraper.close()
    