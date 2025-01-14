from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import time
import logging

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Login URL and credentials
login_url = 'https://artist.atvenu.com/users/sign_in'
email = 'chriswithmanhead@gmail.com'
password = 'cali2580'

# Setup WebDriver
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)

try:
    # Navigate to the login page
    driver.get(login_url)
    logging.info("Opened login page.")

    # Enter login credentials and submit form
    driver.find_element(By.NAME, 'user[email]').send_keys(email)
    driver.find_element(By.NAME, 'user[password]').send_keys(password)
    driver.find_element(By.NAME, 'commit').click()
    logging.info("Login submitted.")

    # Wait for navigation to complete
    time.sleep(5)  # Adjust sleep time as necessary

    # Navigate to the tour forecast shows page
    tour_forecast_url = 'https://artist.atvenu.com/as/talents/16364/tour_forecast_shows?tour_id=55644'
    driver.get(tour_forecast_url)
    logging.info("Navigated to tour forecast shows page.")

    # Wait for the page to load
    time.sleep(5)  # Adjust sleep time as necessary

    # Find the section with the ID 'tour_forecast_shows_container' and take a screenshot
    tour_forecast_section = driver.find_element(By.ID, 'tour_forecast_shows_container')
    tour_forecast_section.screenshot('tour_forecast_shows_section.png')
    logging.info("Screenshot of tour forecast shows section taken.")

except Exception as e:
    logging.exception("An error occurred: " + str(e))
finally:
    driver.quit()