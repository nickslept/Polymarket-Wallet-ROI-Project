/*
QUERY PURPOSE:
- For a given market condition, identify traders who made their first trade after a specified start date
- Split traders into two groups based on whether their first trade was before or after a specified timestamp (IN PARAMS)
- If both groups have at least X traders, randomly sample Y traders from each group and return their wallet addresses and timestamp of first trade
- If either group has fewer than X traders, return a daily count of each day’s amount of NEW traders (no wallet addresses)

**IMPORTANT:** EDIT PARAMS ONLY
*/

WITH params AS (
    SELECT
        0x<YOUR_CONDITION_ID_HERE>          AS condition_id,       -- market condition ID
        DATE '2024-01-01'                   AS market_start,       -- market open date
        TIMESTAMP '2024-06-01 00:00:00'     AS split_timestamp,    -- group A/B split point
        10                                  AS min_group_size,     -- minimum wallets per group (X)
        5                                   AS sample_size         -- wallets to sample per group (Y)... must be <= min_group_size
),

all_traders AS (
    SELECT
        wallet_address,
        MIN(block_time) AS first_trade_time
    FROM (
        SELECT maker AS wallet_address, block_time
        FROM polymarket_polygon.market_trades, params
        WHERE block_time >= params.market_start
          AND condition_id = params.condition_id

        UNION ALL

        SELECT taker AS wallet_address, block_time
        FROM polymarket_polygon.market_trades, params
        WHERE block_time >= params.market_start
          AND condition_id = params.condition_id
    ) AS trades
    GROUP BY wallet_address
),

grouped AS (
    SELECT
        wallet_address,
        first_trade_time,
        CASE
            WHEN first_trade_time <= (SELECT split_timestamp FROM params) THEN 'A'
            ELSE 'B'
        END AS group_label
    FROM all_traders
),

-- Check the size of the smaller group
group_sizes AS (
    SELECT group_label, COUNT(*) AS cnt
    FROM grouped
    GROUP BY group_label
),

smallest_group AS (
    SELECT MIN(cnt) AS min_cnt FROM group_sizes
),

-- Randomly rank wallets within each group for sampling
sampled AS (
    SELECT
        wallet_address,
        first_trade_time,
        group_label,
        ROW_NUMBER() OVER (PARTITION BY group_label ORDER BY RANDOM()) AS rn
    FROM grouped
),

/*
OUTPUT A: Groups are large enough; return sampled wallets
*/
wallet_result AS (
    SELECT
        'sampled_wallet'                        AS result_type,
        CAST(wallet_address AS VARCHAR)         AS identifier,
        CAST(first_trade_time AS VARCHAR)       AS value,
        group_label
    FROM sampled
    WHERE rn <= (SELECT sample_size FROM params)
      AND (SELECT min_cnt FROM smallest_group) >= (SELECT min_group_size FROM params)
),

/*
OUTPUT B: A group is too small; return count of new traders per day instead
*/
daily_result AS (
    SELECT
        'daily_new_wallets'                                         AS result_type,
        CAST(DATE_TRUNC('day', first_trade_time) AS VARCHAR)        AS identifier,
        CAST(COUNT(*) AS VARCHAR)                                   AS value,
        NULL                                                        AS group_label
    FROM all_traders
    WHERE (SELECT min_cnt FROM smallest_group) < (SELECT min_group_size FROM params)
    GROUP BY DATE_TRUNC('day', first_trade_time)
)

SELECT * FROM wallet_result
UNION ALL
SELECT * FROM daily_result
ORDER BY result_type, identifier ASC