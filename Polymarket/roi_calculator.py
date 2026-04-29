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
RESOLUTION_OUTCOME = "Yes" #used to calculate leftover shares value assuming no merges happened

DATA_DIR       = Path(r"C:\Users\Nicholas\Desktop\VS Code Projects\Polymarket Wallet ROI Project\Polymarket-Wallet-ROI-Project\Dune")
INPUT_CSV      = DATA_DIR / "output_A.csv"

POLY_DIR       = Path(r"C:\Users\Nicholas\Desktop\VS Code Projects\Polymarket Wallet ROI Project\Polymarket-Wallet-ROI-Project\Polymarket")
RAW_TRADES_DIR = POLY_DIR / "raw_trades"
RESULTS_FILE   = POLY_DIR / "roi_results.json"
OUTPUT_CSV     = POLY_DIR / "roi_output.csv"

API_BASE         = "https://data-api.polymarket.com"
PAGE_LIMIT       = 5000
MAX_RETRIES      = 4
REQUEST_TIMEOUT  = 30

#checks that the directories listed in the config exist, raises errors if they don't, but makes it so just rerun the program
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
    RAW_TRADES_DIR.mkdir(parents=True, exist_ok=True) #creates raw_trades directory if it doesn't already exist (that's what exist_ok=True parameter does)

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

#calculates the ROI for a wallet based on the trades it made and the resolution outcome of the market
#Known issue: does not account for merges (especially a problem when calculating leftover shares value)
def calculate_roi(trades: list, resolution_outcome: str) -> dict | None:
    """
    ROI = (Total Sold + Leftover Shares Value - Total Spent ) / Total Spent

    YES and NO shares are tracked completely separately.
    At resolution, shares matching resolution_outcome are worth $1; the other side is worth $0.
    Leftover value is only counted for the winning outcome's shares.
 
    Returns None if Total Spent is zero (wallet never bought anything).
    """
    #Decimal to avoid rounding errors w/ floats
    yes_shares_bought = Decimal("0")  
    yes_shares_sold   = Decimal("0")  
    yes_spent         = Decimal("0") 
    yes_sold          = Decimal("0")  
 
    no_shares_bought  = Decimal("0")  
    no_shares_sold    = Decimal("0")  
    no_spent          = Decimal("0")  
    no_sold           = Decimal("0")  

    for trade in trades:
        try:
            price = Decimal(str(trade["price"]))
            size  = Decimal(str(trade["size"]))
            side  = trade["side"].upper()
            outcome = trade["outcome"].strip().capitalize() #just capitalizes the first letter - e.g. "YES" or "yes" to "Yes"
        except (KeyError, TypeError, InvalidOperation):
            continue

        if outcome == "Yes": 
            if side == "BUY":
                yes_spent         += price * size
                yes_shares_bought += size
            elif side == "SELL":
                yes_sold          += price * size
                yes_shares_sold   += size
        elif outcome == "No":  
            if side == "BUY":
                no_spent          += price * size
                no_shares_bought  += size
            elif side == "SELL":
                no_sold           += price * size
                no_shares_sold    += size
 
    total_spent = yes_spent + no_spent  

    if total_spent == Decimal("0"):
        return None

    yes_leftover = max(yes_shares_bought - yes_shares_sold, Decimal("0")) 
    no_leftover  = max(no_shares_bought  - no_shares_sold,  Decimal("0"))  
 
    yes_leftover_value = yes_leftover * (Decimal("1") if resolution_outcome == "Yes" else Decimal("0"))  
    no_leftover_value  = no_leftover  * (Decimal("1") if resolution_outcome == "No"  else Decimal("0"))  
 
    total_sold          = yes_sold + no_sold                            
    total_leftover_value = yes_leftover_value + no_leftover_value       
 
    roi = (total_sold + total_leftover_value - total_spent) / total_spent  
 
    return {
        "roi":               float(roi),
        "total_spent":       float(total_spent),          
        "total_sold":        float(total_sold),           
        "leftover_value":    float(total_leftover_value), 
        "trade_count":       len(trades),
        "yes_shares_bought": float(yes_shares_bought),    
        "yes_shares_sold":   float(yes_shares_sold),      
        "yes_spent":         float(yes_spent),            
        "yes_sold":          float(yes_sold),             
        "yes_leftover_value":float(yes_leftover_value),   
        "no_shares_bought":  float(no_shares_bought),     
        "no_shares_sold":    float(no_shares_sold),       
        "no_spent":          float(no_spent),             
        "no_sold":           float(no_sold),              
        "no_leftover_value": float(no_leftover_value),    
    }


def main():
    check_dirs()

    print(f"Market     : {MARKET_ID}")
    print(f"Resolution : {RESOLUTION_OUTCOME}\n")  

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
        
        trades = load_raw_trades(address) #looks for if ALL the trades the wallet made in a specific market was saved as .json file (aka you called the api enough times for the specific wallet and got all their trades in the specific market)
        if trades is None: #the trades for the wallet aren't saved/cached
            try:
                trades = fetch_trades(address, MARKET_ID)
            except Exception as e:
                print(f"  Fetch failed: {e} -- skipping.\n")
                continue #stops the results from being saved if there are any API-related issues (e.g. if you only get half the trades for a wallet, the wallet WONT be saved to results)
            save_raw_trades(address, trades)
            print(f"  Fetched and cached {len(trades)} trades.")
        else: #the trades for the wallet were saved already
            print(f"  Loaded {len(trades)} trades from cache.")

        roi_data = calculate_roi(trades, RESOLUTION_OUTCOME) #calculates roi (and some additional information) for the wallet
        if roi_data is None:
            print(f"  ROI: N/A (no buy trades found)\n")
            results[address] = {"group": group, "roi": None, "trade_count": len(trades)}
        else: #prints out data obtained through calculate_roi() for the wallet and then 
            print(f"  ROI: {roi_data['roi']:+.4%}  "
                    f"(spent={roi_data['total_spent']:.4f}, "
                    f"sold={roi_data['total_sold']:.4f}, "
                    f"leftover={roi_data['leftover_value']:.4f}, "
                    f"yes_bought={roi_data['yes_shares_bought']:.4f}, "   
                    f"yes_sold={roi_data['yes_shares_sold']:.4f}, "       
                    f"no_bought={roi_data['no_shares_bought']:.4f}, "    
                    f"no_sold={roi_data['no_shares_sold']:.4f})\n")       
            results[address] = {"group": group, **roi_data} #adds the wallet address as a key in the results dict and its value is a dictionary containing the data from calulate_roi()

        save_results(results) #saves the current "results" dictionary as a .json file (for future use)

    # Output CSV (AFTER ALL THE WALLETS HAVE BEEN PROCESSED)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "address", "group", "roi",
            "total_spent", "total_sold", "leftover_value", "trade_count",
            "yes_shares_bought", "yes_shares_sold", "yes_spent", "yes_sold", "yes_leftover_value",
            "no_shares_bought",  "no_shares_sold",  "no_spent",  "no_sold",  "no_leftover_value",
        ], restval="") #restval="" stops crashes from occuring for wallets with roi=None
        writer.writeheader()
        for addr, data in results.items():
            writer.writerow({"address": addr, **data})

    print(f"\nSaved: {RESULTS_FILE}  (checkpoint JSON)")
    print(f"Saved: {OUTPUT_CSV}  (final results)")


if __name__ == "__main__":
    main()
