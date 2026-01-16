import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

API_URL = os.getenv("EXCHANGE_KEY")
DB_URL = os.getenv("SUPABASE_URL")


def capture_historical_rates():
    # 1. Fetch Data
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()

        # Extract the rates and the specific API timestamp
        rates_dict = data.get("conversion_rates", {})
        api_time = data.get(
            "time_last_update_utc"
        )  # e.g., "Mon, 19 Jan 2026 00:00:01 +0000"

        if not rates_dict or not api_time:
            print("Required data missing from API response.")
            return

    except Exception as e:
        print(f"API Error: {e}")
        return

    # 2. Transform JSON to DataFrame
    df = pd.Series(rates_dict).reset_index()
    df.columns = ["currency_code", "rate"]

    # 3. Add the API Timestamp to every row
    # pd.to_datetime handles the "UTC" string format automatically
    df["recorded_at"] = pd.to_datetime(api_time)

    # 4. Load into PostgreSQL
    try:
        engine = create_engine(DB_URL)

        # 'id' remains SERIAL (automatic), but we now provide the other 3 columns
        df.to_sql(
            name="exchange1",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )

        print(f"Saved {len(df)} rows with API timestamp: {api_time}")

    except Exception as e:
        print(f"Database Error: {e}")


if __name__ == "__main__":
    capture_historical_rates()
