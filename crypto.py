import os
import asyncio
import time
import aiohttp
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# ---------------- CONFIG ---------------- #
load_dotenv()

API_URL = "https://api.coingecko.com/api/v3/coins/markets"
API_KEY = os.getenv("GECKO_KEY", "").strip()
DB_URL = os.getenv("SUPABASE_URL", "").strip()

# Optional: Safety check to make sure the key isn't empty after stripping
if not API_KEY:
    print("⚠️ WARNING: GECKO_KEY is empty or missing!")

PAGES = 8
MAX_CONCURRENT = 5
VOLUME_CUTOFF = 50_000
TIMEOUT = aiohttp.ClientTimeout(total=10)

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

HEADERS = {"x-cg-demo-api-key": API_KEY}


# ---------------- ASYNC FETCH ---------------- #
async def fetch_page(session, page: int):
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 250,
        "page": page,
        "sparkline": "false",
    }

    try:
        async with session.get(API_URL, params=params, headers=HEADERS) as r:
            if r.status != 200:
                print(f"❌ Page {page} failed ({r.status})")
                return []

            data = await r.json()

            if data and data[-1]["total_volume"] < VOLUME_CUTOFF:
                print(f"🛑 Stopping at page {page} (low volume)")
                return None

            print(f"✅ Page {page} fetched")
            return data

    except asyncio.CancelledError:
        return []
    except Exception as e:
        print(f"❌ Page {page} error: {e}")
        return []


async def fetch_all_pages():
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT)
    async with aiohttp.ClientSession(
        headers=HEADERS,
        timeout=TIMEOUT,
        connector=connector,
    ) as session:
        # Fetch all 8 pages at once
        tasks = [fetch_page(session, p) for p in range(1, PAGES + 1)]

        # gather returns results in the exact order of the tasks list
        pages_data = await asyncio.gather(*tasks)

        results = []
        for data in pages_data:
            if data is None or not data:  # Handle the stop signal or empty responses
                break
            results.extend(data)

        return results


# ---------------- MAIN LOGIC ---------------- #
def get_crypto_data():
    # 1. Fetch data (Recommendation: Use asyncio.gather in fetch_all_pages
    # to ensure results come back in page order)
    rows = asyncio.run(fetch_all_pages())

    if not rows:
        return pd.DataFrame()

    # 2. Convert to DataFrame and select columns
    df = pd.DataFrame(rows)

    # Ensure all KEEP_COLUMNS exist before selecting
    existing_cols = [c for c in KEEP_COLUMNS if c in df.columns]
    df = df[existing_cols]

    # 3. Fast Filtering
    # Drop coins without a rank
    df = df.dropna(subset=["market_cap_rank"])

    # Filter by Volume
    df = df[df["total_volume"] > VOLUME_CUTOFF]

    # --- NEW: Filter by Volatility ---
    # Removes coins with practically zero price movement (< 0.05%)
    df = df[df["price_change_percentage_24h"].abs() > 0.05]

    # 4. Add metadata
    df["captured_at"] = pd.Timestamp.utcnow()

    # 5. Final Sort (Crucial for Async results)
    # Since async fetches pages out of order, we re-sort by market cap
    df = df.sort_values("market_cap_rank").reset_index(drop=True)

    # Convert API string to actual datetime objects
    df["last_updated"] = pd.to_datetime(df["last_updated"])

    return df


# ---------------- EXECUTION ---------------- #
if __name__ == "__main__":
    start = time.perf_counter()

    df = get_crypto_data()
    print(f"📊 Usable coins: {len(df)}")

    if not df.empty:
        engine = create_engine(
            DB_URL,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
        )

        t_db_start = time.perf_counter()

        df.to_sql(
            "crypto_prices",
            engine,
            if_exists="append",
            index=False,
            chunksize=5000,
            method="multi",
        )

        print(f"🗄️ DB write time: {time.perf_counter() - t_db_start:.2f}s")

    print(f"⏱️ Total runtime: {time.perf_counter() - start:.2f}s")
