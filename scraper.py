from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import pandas as pd

# Specify the correct path to the ChromeDriver binary
chrome_driver_path = "/Users/bikram/Downloads/chromedriver-mac-arm64-1/chromedriver"

# Initialize the Service
cService = Service(executable_path=chrome_driver_path)

# Start Chrome WebDriver
driver = webdriver.Chrome(service=cService)

# Open a website (e.g., Google)
driver.get("https://www.coingecko.com/")
tbody = driver.find_element(By.CSS_SELECTOR, "tbody.tw-divide-y")
rows = tbody.find_elements(By.XPATH, ".//tr")

data = []
for row in rows:
    number = row.find_element(By.XPATH, ".//td[2]").text
    coin_name = row.find_element(By.XPATH, ".//td[3]").text
    price = row.find_element(By.XPATH, ".//td[5]").text
    hourly_update = row.find_element(By.XPATH, ".//td[6]").text
    one_day_update = row.find_element(By.XPATH, ".//td[7]").text
    week_update = row.find_element(By.XPATH, ".//td[8]").text
    one_day_volume = row.find_element(By.XPATH, ".//td[10]").text
    market_cap = row.find_element(By.XPATH, ".//td[11]").text
    data.append({"rank": number,
                 "coin": coin_name,
                 "price": price,
                 "1h": hourly_update,
                 "24h": one_day_update,
                 "7d": week_update,
                 "24h Volume": one_day_volume,
                 "market cap": market_cap})
    
df = pd.DataFrame(data)

print(df)


# driver.quit()
