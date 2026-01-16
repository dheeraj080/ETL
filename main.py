import requests
from dotenv import load_dotenv
import os

load_dotenv()

API_URL = os.getenv("EXCHANGE_KEY")


def get_data(data):
    url = API_URL
    response = requests.get(url)

    if response.status_code == 200:
        code_rate = response.json()
        return code_rate
    else:
        print(f"failed to retrive data {response.status_code}")


parameter = "rates"
result_data = get_data(parameter)

if result_data:
    print(f"{result_data["conversion_rates"]}")
    print(f"{result_data["time_last_update_utc"]}")
