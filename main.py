import requests
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
import os

load_dotenv()

API_URL = os.getenv("EXCHANGE_KEY")
DATABASE_URL = os.getenv("SUPABASE_URL")

engine = sqlalchemy.create_engine(DATABASE_URL)


response = requests.get(API_URL)
print(response)

response_data = response.json()
# print(response_data)

rates_dict = response_data.get("conversion_rates", {})
df = pd.Series(rates_dict).reset_index()

# print(rates_dict)

df.to_sql(name="rates", con=engine, index=False, if_exists="fail")
