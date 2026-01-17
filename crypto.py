import os
import requests
import pandas as pd
import time
from sqlalchemy import create_engine
from dotenv import load_dotenv

# 1. Load configuration
load_dotenv()
API_URL = "https://api.coingecko.com/api/v3/coins/markets"
API_KEY = os.getenv("GECKO_KEY")
DB_URL = os.getenv("SUPABASE_URL")  # Ensure this is a full postgres:// string


def get_coins(pages=12):
    all_data = []
    headers = {"x-cg-demo-api-key": API_KEY}

    for page in range(1, pages + 1):
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 250,
            "page": page,
            "sparkline": False,
        }

        response = requests.get(API_URL, headers=headers, params=params)
        if response.status_code == 200:
            all_data.extend(response.json())
            print(f"Fetched page {page}...")
        else:
            print(f"Error: {response.status_code}")
            break
        time.sleep(2.5)  # Crucial for free tier

    df = pd.DataFrame(all_data)

    # --- CLEANING ---
    df = df.dropna(subset=["market_cap_rank"])
    df = df[df["total_volume"] > 50000]

    keep_columns = [
        "id",  # Unique identifier (e.g., 'bitcoin')
        "symbol",  # Ticker (e.g., 'btc')
        "name",  # Display name
        "current_price",  # The 'Close' price for that timestamp
        "market_cap",  # Total valuation
        "total_volume",  # Trading activity
        "market_cap_rank",  # Position in the market
        "price_change_percentage_24h",  # Volatility metric
        "high_24h",
        "low_24h",
        "captured_at",  # Your record timestamp
        "last_updated",
    ]
    # Apply the filter to your DataFrame
    df = df[[col for col in keep_columns if col in df.columns]]

    df["captured_at"] = pd.Timestamp.now()

    return df


# --- EXECUTION FLOW ---

# A. Get the data
df_clean = get_coins(pages=12)
print(f"Total usable coins: {len(df_clean)}")

# B. Upload to Database (Outside the function)
if not df_clean.empty:
    try:
        # Create the engine here
        engine = create_engine(DB_URL)

        # Upload
        df_clean.to_sql("crypto_prices", engine, if_exists="append", index=False)
        print("✅ Data successfully uploaded to SUPABASE!")

    except Exception as e:
        print(f"❌ Database error: {e}")
