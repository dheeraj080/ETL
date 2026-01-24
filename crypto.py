import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from concurrent.futures import ThreadPoolExecutor

# 1. Load configuration
load_dotenv()
API_URL = "https://api.coingecko.com/api/v3/coins/markets"
API_KEY = os.getenv("GECKO_KEY")
DB_URL = os.getenv("SUPABASE_URL")


def get_robust_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session


session = get_robust_session()


def fetch_page(page):
    """Worker function for threading."""
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 250,
        "page": page,
        "sparkline": False,
    }
    headers = {"x-cg-demo-api-key": API_KEY}
    try:
        response = session.get(API_URL, headers=headers, params=params)
        if response.status_code == 200:
            print(f"Fetched page {page}...")
            return response.json()
    except Exception as e:
        print(f"Error fetching page {page}: {e}")
    return []


def get_crypto_data(pages=8):
    # --- A. Parallel Fetching ---
    # Using threads allows us to wait for multiple network responses at once
    with ThreadPoolExecutor(max_workers=pages) as executor:
        results = list(executor.map(fetch_page, range(1, pages + 1)))

    all_data = [item for sublist in results for item in sublist]
    df = pd.DataFrame(all_data)

    if df.empty:
        return df

    # --- B. Cleaning & Processing ---
    df = df.dropna(subset=["market_cap_rank"])
    df = df[df["total_volume"] > 50000]

    keep_columns = [
        "id",
        "symbol",
        "name",
        "current_price",
        "market_cap",
        "total_volume",
        "market_cap_rank",
        "price_change_percentage_24h",
        "high_24h",
        "low_24h",
        "last_updated",
    ]

    df = df[[col for col in keep_columns if col in df.columns]].copy()
    df["captured_at"] = pd.Timestamp.now()

    return df


# --- EXECUTION FLOW ---
if __name__ == "__main__":
    df_clean = get_crypto_data(pages=8)
    print(f"Total usable coins: {len(df_clean)}")

    if not df_clean.empty:
        try:
            # --- C. Fast Database Upload ---
            engine = create_engine(DB_URL)
            df_clean.to_sql(
                "crypto_prices",
                engine,
                if_exists="append",
                index=False,
                chunksize=1000,
                method="multi",  # Vital for speed
            )
            print("✅ Data successfully uploaded to SUPABASE!")
        except Exception as e:
            print(f"❌ Database error: {e}")
