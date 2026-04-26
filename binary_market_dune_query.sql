WITH params AS (
  SELECT
    '0xYOUR_CONDITION_ID_HERE' AS condition_id,
    1 AS resolution_price  -- set to 1 if YES won, 0 if NO won
),

trades AS (
  SELECT
    trader_address,
    side,
    price,
    shares,
    token_outcome,
    (price * shares) AS usdc_value,
    block_time,
    ROW_NUMBER() OVER (ORDER BY block_time) AS trade_number
  FROM polymarket_polygon.market_trades
  WHERE condition_id = (SELECT condition_id FROM params)
),


-- Counts number of trades per wallet to filter out bots 
valid_wallets AS (
  SELECT trader_address
  FROM trades
  GROUP BY trader_address
  HAVING COUNT(*) BETWEEN 3 AND 100  -- market-specific trade count threshold for each individual wallet (in threshold = not considered a bot)
),

-- Only trades for valid_wallets are kept
filtered_trades AS (
  SELECT t.*
  FROM trades t
  INNER JOIN valid_wallets v ON t.trader_address = v.trader_address
),

total_trade_count AS (
  SELECT COUNT(*) AS total FROM filtered_trades  -- gets total trade count for NON BOT WALLETS
),

-- Column with number for each wallet's first trade
wallet_first_trade AS (
  SELECT
    trader_address,
    MIN(trade_number) AS first_trade_number
  FROM filtered_trades
  GROUP BY trader_address
),

/*
Column with early or late depending on whether the wallet's
first trade was in the first half of all FILTERED trades, or after
*/
wallet_timing AS (
  SELECT
    w.trader_address,
    w.first_trade_number,
    t.total,
    CASE
      WHEN w.first_trade_number <= (t.total / 2) THEN 'early'
      ELSE 'late'
    END AS entry_timing
  FROM wallet_first_trade w
  CROSS JOIN total_trade_count t
),

per_wallet AS (
  SELECT
    trader_address,
    SUM(CASE WHEN side = 'BUY'  THEN usdc_value ELSE 0 END) AS total_spent,
    SUM(CASE WHEN side = 'SELL' THEN usdc_value ELSE 0 END) AS total_sold,

    -- YES shares
    SUM(CASE WHEN side = 'BUY'  AND token_outcome = 'YES' THEN shares ELSE 0 END) AS yes_shares_bought,
    SUM(CASE WHEN side = 'SELL' AND token_outcome = 'YES' THEN shares ELSE 0 END) AS yes_shares_sold,

    -- NO shares
    SUM(CASE WHEN side = 'BUY'  AND token_outcome = 'NO' THEN shares ELSE 0 END) AS no_shares_bought,
    SUM(CASE WHEN side = 'SELL' AND token_outcome = 'NO' THEN shares ELSE 0 END) AS no_shares_sold,

    COUNT(*) AS total_trades
  FROM filtered_trades
  GROUP BY trader_address
),

with_leftover AS (
  SELECT
    p.trader_address,
    p.total_spent,
    p.total_sold,
    p.yes_shares_bought,
    p.yes_shares_sold,
    p.no_shares_bought,
    p.no_shares_sold,
    p.total_trades,
    (p.yes_shares_bought - p.yes_shares_sold) AS leftover_yes_shares,
    (p.no_shares_bought  - p.no_shares_sold)  AS leftover_no_shares
  FROM per_wallet p
),

with_roi AS (
  SELECT
    l.trader_address,
    l.total_spent,
    l.total_sold,
    l.leftover_yes_shares,
    l.leftover_no_shares,
    l.total_trades,
    /*
      leftover value depends on which side the wallet's leftover shares are on and how the market resolved
      if market resolved YES: yes shares worth 1 each, no shares worth 0
      if market resolved NO:  no shares worth 1 each, yes shares worth 0

      Uses this logic to assign value to the leftover shares, which is used in the ROI formula
    */
    CASE
      WHEN l.total_spent = 0 THEN NULL
      ELSE (
        (
          l.total_sold +
          CASE
            WHEN params.resolution_price = 1 THEN l.leftover_yes_shares -- if YES wins, leftover YES shares have value, so use them to calculate ROI
            ELSE l.leftover_no_shares -- if NO wins, leftover NO shares have value, so use them to calculate ROI
          END
        ) - l.total_spent
      ) / l.total_spent
    END AS roi
  FROM with_leftover l, params
)

SELECT
  r.trader_address,
  r.total_trades,
  wt.entry_timing,
  wt.first_trade_number,
  r.roi
FROM with_roi r
JOIN wallet_timing wt ON r.trader_address = wt.trader_address
ORDER BY wt.entry_timing, wt.first_trade_number ASC