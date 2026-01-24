import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# ---------------- CONFIG ---------------- #
load_dotenv()

API_URL = "https://api.coingecko.com/api/v3/coins/markets"
API_KEY = os.getenv("GECKO_KEY")
DB_URL = os.getenv("SUPABASE_URL")

PAGES = 8
MAX_WORKERS = 5          # safer for CoinGecko
TIMEOUT = 10             # seconds

KEEP_COLUMNS = [
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

# ---------------- HTTP SESSION ---------------- #
def create_session():
    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=MAX_WORKERS)
    session = requests.Session()
    session.mount("https://", adapter)
    return session


def fetch_page(page: int) -> list[dict]:
    session = create_session()  # one session per thread
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 250,
        "page": page,
        "sparkline": False,
    }
    headers = {"x-cg-demo-api-key": API_KEY}

    try:
        r = session.get(API_URL, headers=headers, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        print(f"✅ Page {page} fetched")
        return r.json()
    except Exception as e:
        print(f"❌ Page {page} failed: {e}")
        return []


# ---------------- MAIN LOGIC ---------------- #
def get_crypto_data(pages: int = PAGES) -> pd.DataFrame:
    all_rows = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_page, p) for p in range(1, pages + 1)]
        for future in as_completed(futures):
            all_rows.extend(future.result())

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    # Fast column selection
    df = df[[c for c in KEEP_COLUMNS if c in df.columns]]

    # Filters
    df = df.dropna(subset=["market_cap_rank"])
    df = df[df["total_volume"] > 50_000]

    df["captured_at"] = pd.Timestamp.utcnow()

    return df.reset_index(drop=True)


# ---------------- EXECUTION ---------------- #
if __name__ == "__main__":
    start_time = time.perf_counter()
    
    df = get_crypto_data()
    print(f"📊 Usable coins: {len(df)}")

    if not df.empty:
        try:
            engine = create_engine(
                DB_URL,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10,
            )

            df.to_sql(
                "crypto_prices",
                engine,
                if_exists="append",
                index=False,
                chunksize=2000,
                method="multi",
            )

            print("🚀 Uploaded to Supabase successfully")
        except Exception as e:
            print(f"🔥 Database error: {e}")
    
    end_time = time.perf_counter()
    print(f"⏱️ Total runtime: {end_time - start_time:.2f} seconds")
