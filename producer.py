from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from confluent_kafka import Producer
from dotenv import load_dotenv
import os
import pandas as pd
import json

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "crypto_prices"

producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def delivery_report(err, msg):
    """ Callback for message delivery reports """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


# Specify the correct path to the ChromeDriver binary
chrome_driver_path = "/Users/bikram/Downloads/chromedriver-mac-arm64-1/chromedriver"

# Initialize the Service
cService = Service(executable_path=chrome_driver_path)

# Start Chrome WebDriver
driver = webdriver.Chrome(service=cService)

url = os.getenv("WEB_URL")
# Open a website (e.g., Google)
driver.get(url)
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
    crypto_data = {"rank": number,
                 "coin": coin_name,
                 "price": price,
                 "1h": hourly_update,
                 "24h": one_day_update,
                 "7d": week_update,
                 "24h Volume": one_day_volume,
                 "market cap": market_cap}
    
    producer.produce(TOPIC_NAME, key=coin_name, value=json.dumps(crypto_data), callback=delivery_report)
    producer.flush()
    
    # data.append({"rank": number,
    #              "coin": coin_name,
    #              "price": price,
    #              "1h": hourly_update,
    #              "24h": one_day_update,
    #              "7d": week_update,
    #              "24h Volume": one_day_volume,
    #              "market cap": market_cap})
    
df = pd.DataFrame(data)

print(df)


driver.quit()
