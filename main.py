import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ---------------- CONFIG ---------------- #
load_dotenv()

# Always .strip() to avoid "Header Injection" or "URL not found" errors
API_URL = os.getenv("EXCHANGE_KEY", "").strip()
DB_URL = os.getenv("SUPABASE_URL", "").strip()

# Create engine globally for better connection management
# pool_pre_ping=True checks if the connection is alive before using it
engine = create_engine(DB_URL, pool_pre_ping=True)


def capture_historical_rates():
    # 1. Fetch Data with Timeout
    try:
        # Added a timeout so the script doesn't hang forever if the API is down
        response = requests.get(API_URL, timeout=15)
        response.raise_for_status()
        data = response.json()

        rates_dict = data.get("conversion_rates", {})
        api_time = data.get("time_last_update_utc")

        if not rates_dict or not api_time:
            print("❌ Data missing in API response.")
            return

    except Exception as e:
        print(f"❌ API Error: {e}")
        return

    # 2. Vectorized Transformation
    # Using pd.DataFrame.from_dict is slightly more memory-efficient for large dicts
    df = pd.DataFrame(list(rates_dict.items()), columns=["currency_code", "rate"])

    # 3. Add Timestamp (Vectorized)
    df["recorded_at"] = pd.to_datetime(api_time)

    # 4. Load into PostgreSQL (Optimized)
    try:
        with engine.begin() as connection:  # Use a transaction block
            df.to_sql(
                name="exchange_rates2",
                con=connection,
                if_exists="append",
                index=False,
                chunksize=500,  # Optimal for small-to-medium datasets
                method="multi",
            )
        print(f"✅ Successfully saved {len(df)} rates (Timestamp: {api_time})")

    except Exception as e:
        print(f"❌ Database Error: {e}")


if __name__ == "__main__":
    capture_historical_rates()
