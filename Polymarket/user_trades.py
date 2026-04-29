"""
Polymarket ROI Calculator
    - Reads wallet addresses + group labels from Dune/output_A.csv,
    - Fetches all trades per wallet in a specific market
    - Caches raw data to disk then calculates ROI per wallet
    - Note: the group labels are for data analysis later
"""

import csv
import json
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path

import requests

# CONFIG
MARKET_ID        = "0x87d67272f0ce1bb0d80ba12a1ab79287b2a235a5f361f5bcbc06ea0ce34e61c5"
RESOLUTION_PRICE = 1   # 1 = market resolved to YES; 0 = market resolved to NO 

DATA_DIR       = Path(r"C:\Users\nslep\Desktop\VS Code Projects\Polymarket-Wallet-ROI-Project\Polymarket-Wallet-ROI-Project\Dune")
INPUT_CSV      = DATA_DIR / "output_A.csv"

POLY_DIR       = Path(r"C:\Users\nslep\Desktop\VS Code Projects\Polymarket-Wallet-ROI-Project\Polymarket-Wallet-ROI-Project\Polymarket")
RAW_TRADES_DIR = POLY_DIR / "raw_trades"
RESULTS_FILE   = POLY_DIR / "roi_results.json"
OUTPUT_CSV     = POLY_DIR / "roi_output.csv"

API_BASE         = "https://data-api.polymarket.com"
PAGE_LIMIT       = 5000
MAX_RETRIES      = 4
REQUEST_TIMEOUT  = 30

#checks that the directories listed in the config exist, raises errors if they don't
def check_dirs():
    if not POLY_DIR.exists():
        raise FileNotFoundError(
            f"POLY_DIR does not exist: {POLY_DIR}\n"
            "Please update the POLY_DIR path in the CONFIG section."
        )
    if not DATA_DIR.exists():
        raise FileNotFoundError(
            f"DATA_DIR does not exist: {DATA_DIR}\n"
            "Please update the DATA_DIR path in the CONFIG section."
        )

#reads the csv being used as input to make a list of dictionaries containing the wallet address and group it is in
def read_wallets(csv_path: Path) -> list[dict]:
    wallets = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            address = row["identifier"].strip()
            group   = row["group_label"].strip()
            if address: #stops once the row is blank (last address)
                wallets.append({"address": address, "group": group})
    return wallets

#returns the path for the cached wallet's address, which is specified as a parameter (cached wallet = wallet whose trades were saved after fetching from API to avoid issues leading you to restart the entire process)
def trade_cache_path(address: str) -> Path:
    return RAW_TRADES_DIR / f"{address.lower().replace('0x', '')}.json"

#saves a wallet's trades as a .json file
def save_raw_trades(address: str, trades: list):
    with open(trade_cache_path(address), "w", encoding="utf-8") as f:
        json.dump(trades, f)

#reads if a wallet's trades exist --> if yes: reads and returns it as a list, if no: returns None
def load_raw_trades(address: str) -> list | None:
    path = trade_cache_path(address)
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None

#loads the results of which wallets have been fully cached AND ROI calculated
def load_results() -> dict:
    return json.loads(RESULTS_FILE.read_text(encoding="utf-8")) if RESULTS_FILE.exists() else {}

#saves the calculated roi (and additional information **BUT NOT THE FULL TRADE HISTORY THOUGH**) to roi_results.json
def save_results(results: dict):
    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

#makes an API request to polymarket and handles the different types of errors
def get_with_retry(url: str, params: dict) -> list:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                wait = 2 ** attempt
                print(f"  Rate limited. Waiting {wait}s... (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            raise ValueError(f"HTTP {resp.status_code}: {resp.text[:200]}")
        except requests.exceptions.RequestException as e:
            wait = 2 ** attempt
            print(f"  Request error: {e}. Waiting {wait}s... (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(wait)
    raise RuntimeError(f"Exceeded {MAX_RETRIES} retries for {url}")

#calls on get_with_retry() to make API requests for a specific wallet in a specific market
#Does this in batches and makes a list of all the information the API requests return (to be processed later)
def fetch_trades(address: str, market_id: str) -> list:
    all_trades = []
    offset = 0
    while True:
        batch = get_with_retry(f"{API_BASE}/trades", {
            "user":      address,
            "market":    market_id,
            "limit":     PAGE_LIMIT,
            "offset":    offset,
            "takerOnly": "false",
        })
        if not batch:
            break
        all_trades.extend(batch)
        if len(batch) < PAGE_LIMIT:
            break
        offset += PAGE_LIMIT
    return all_trades

#calculates ROI of the trader and returns a dictionary containing:
    #roi
    #total spent ($) 
    #total sold ($) 
    #left over value ($)
    #number of trades
    #shares bought
    #shares sold 
#Known issue: does not account for merges (especially a problem when calculating leftover shares value)
def calculate_roi(trades: list, resolution_price: int) -> dict | None:
    """
    ROI = (Total Sold + Leftover Shares Value - Total Spent ) / Total Spent

    Total Sold            = sum(sell_price * shares_sold)
    Total Spent           = sum(buy_price  * shares_bought)
    Leftover Shares Value = (shares_bought - shares_sold) * resolution_price 

    Returns None if Total Spent is zero (wallet never bought anything).
    """
    #Decimal to avoid rounding errors w/ floats
    total_spent   = Decimal("0")
    total_sold    = Decimal("0")
    shares_bought = Decimal("0")
    shares_sold   = Decimal("0")

    for trade in trades:
        try:
            price = Decimal(str(trade["price"]))
            size  = Decimal(str(trade["size"]))
            side  = trade["side"].upper()
        except (KeyError, TypeError, InvalidOperation):
            continue

        if side == "BUY":
            total_spent   += price * size
            shares_bought += size
        elif side == "SELL":
            total_sold  += price * size
            shares_sold += size

    if total_spent == Decimal("0"):
        return None

    leftover       = max(shares_bought - shares_sold, Decimal("0")) #max() to avoid negative numbers resulting from inaccurate data (negative leftover = sold more shares than bought)
    leftover_value = leftover * Decimal(str(resolution_price))
    roi            = (total_sold + leftover_value - total_spent) / total_spent

    return {
        "roi":            float(roi),
        "total_spent":    float(total_spent),
        "total_sold":     float(total_sold),
        "leftover_value": float(leftover_value),
        "trade_count":    len(trades),
        "shares_bought":  float(shares_bought),
        "shares_sold":    float(shares_sold),
    }


def main():
    check_dirs()

    print(f"Market     : {MARKET_ID}")
    print(f"Resolution : {'YES (1)' if RESOLUTION_PRICE == 1 else 'NO (0)'}\n")

    wallets = read_wallets(INPUT_CSV)
    print(f"Loaded {len(wallets)} wallets from CSV")

    results = load_results()
    print(f"{len(results)} wallets already done (will skip)\n")

    for i, w in enumerate(wallets): #loops through each wallet 
        address, group = w["address"], w["group"]
        print(f"[{i+1}/{len(wallets)}] {address}  group={group}") 

        if address in results: #if the wallet's roi and other information has already been stored, go to the next wallet
            print(f"  Already processed, skipping.\n") 
            continue
        #NEED TO LOOK AT BELOW****************
        trades = load_raw_trades(address)
        if trades is None:
            try:
                trades = fetch_trades(address, MARKET_ID)
            except Exception as e:
                print(f"  Fetch failed: {e} -- skipping.\n")
                continue
            save_raw_trades(address, trades)
            print(f"  Fetched and cached {len(trades)} trades.")
        else:
            print(f"  Loaded {len(trades)} trades from cache.")

        roi_data = calculate_roi(trades, RESOLUTION_PRICE)
        if roi_data is None:
            print(f"  ROI: N/A (no buy trades found)\n")
            results[address] = {"group": group, "roi": None, "trade_count": len(trades)}
        else:
            print(f"  ROI: {roi_data['roi']:+.4%}  "
                    f"(spent={roi_data['total_spent']:.4f}, "
                    f"sold={roi_data['total_sold']:.4f}, "
                    f"leftover={roi_data['leftover_value']:.4f}, "
                    f"shares_bought={roi_data['shares_bought']:.4f}, "
                    f"shares_sold={roi_data['shares_sold']:.4f})\n")
            results[address] = {"group": group, **roi_data}

        save_results(results)

    # Output CSV 
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "address", "group", "roi", "total_spent", "total_sold",
            "leftover_value", "trade_count", "shares_bought", "shares_sold",
        ])
        writer.writeheader()
        for addr, data in results.items():
            writer.writerow({"address": addr, **data})

    print(f"\nSaved: {RESULTS_FILE}  (checkpoint JSON)")
    print(f"Saved: {OUTPUT_CSV}  (final results)")


if __name__ == "__main__":
    main()