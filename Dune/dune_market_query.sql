/*
QUERY PURPOSE:
- For a given *RESOLVED* market, identify traders who made their first trade after a specified start date
- Split traders into two groups based on whether their first trade was before or after a specified timestamp (IN PARAMS)
- If both groups have at least X traders, randomly sample Y traders from each group and return their wallet addresses and timestamp of first trade
- If either group has fewer than X traders, return a daily count of each day’s amount of NEW traders (no wallet addresses)

**IMPORTANT:** EDIT PARAMS ONLY
*/

WITH params AS (
    SELECT
        0x87d67272f0ce1bb0d80ba12a1ab79287b2a235a5f361f5bcbc06ea0ce34e61c5          AS condition_id,
        TIMESTAMP '2024-09-03 16:16:55.822'                   AS market_start,
        TIMESTAMP '2024-12-17 19:00:00'     AS split_timestamp, --should be before market_end
        TIMESTAMP '2024-12-31 12:00:00'     AS market_end,        
        501                                  AS min_group_size, --MUST BE >= SAMPLE_SIZE
        50                                   AS sample_size
),

--Determine each wallet's first transaction in the market (whether it was as a taker or maker)
all_traders AS (
    SELECT
        wallet_address,
        MIN(block_time) AS first_trade_time
    FROM polymarket_polygon.market_trades AS mt
    CROSS JOIN UNNEST(ARRAY[maker, taker]) AS t(wallet_address)
    CROSS JOIN params
    WHERE block_time >= params.market_start
      AND block_time <= params.market_end
      AND mt.condition_id = params.condition_id
    GROUP BY wallet_address
),

--Split into 2 groups based on early/late relative to split_timestamp
grouped AS (
    SELECT
        wallet_address,
        first_trade_time,
        CASE
            WHEN first_trade_time <= (SELECT split_timestamp FROM params) THEN 'A' --A = early; B = late
            ELSE 'B'
        END AS group_label
    FROM all_traders
),

--Checks how many wallets in group A & how many wallets in group B
group_sizes AS (
    SELECT group_label, COUNT(*) AS cnt
    FROM grouped
    GROUP BY group_label
),

--Figures out how many wallets are in the SMALLEST group (between A and B) and ensures that there is at least 1 wallet present in both groups
smallest_group AS (
    SELECT
        MIN(cnt)                        AS min_cnt,
        COUNT(DISTINCT group_label) = 2 AS both_groups_present
    FROM group_sizes
),

/*
Randomizes the row order within each group (meaning each wallet is now in a random spot but still with its group A/B)
and counts up from 1 to the amount of wallets in the group, making it column "rn"

**THIS IS TO ASSIGN A RANDOM NUMBER TO EACH WALLET WITHIN EACH GROUP**
*/
sampled AS (
    SELECT
        wallet_address,
        first_trade_time,
        group_label,
        ROW_NUMBER() OVER (PARTITION BY group_label ORDER BY RANDOM()) AS rn
    FROM grouped
),

/*
POTENTIAL OUTPUT [A]:
Given that: 
    - the smallest group meets the minimum group size 
    - the smallest group is NOT blank (to avoid potential bugs w/ null)

--> Group A already has randomly ordered wallets with a corresponding label from 1 to the size of Group A. The same applies to Group B.
--> Essentially checks which rows have rn <= sample_size and only keeps those rows

The output is 5 columns: 
result_type (this will be the same string for all rows: sampled_wallet)
identifier (string: wallet ID)
value (string: time of first trade)
group_label (string: either A or B)
pre_sample_group_size (string: the size of the group that the wallet belongs to BEFORE sampling)
*/
wallet_result AS (
    SELECT
        'sampled_wallet'                        AS result_type,
        CAST(wallet_address AS VARCHAR)         AS identifier,
        CAST(first_trade_time AS VARCHAR)       AS value,
        s.group_label,
        CAST(gs.cnt AS VARCHAR)                 AS pre_sample_group_size  
    FROM sampled s
    JOIN group_sizes gs ON s.group_label = gs.group_label
    WHERE rn <= (SELECT sample_size FROM params)
      AND (SELECT min_cnt FROM smallest_group) >= (SELECT min_group_size FROM params)
      AND (SELECT both_groups_present FROM smallest_group)
),

/*
POTENTIAL OUTPUT [B]:
Given that:
    - the smallest group does not meet the minimum group size OR one of the groups is missing (null)

--> for each wallet, takes their first trade time stamp and gets rid of exact time of day (just date is kept)
--> counts how many unique wallets had their first trade on the same day
--> null is put in group_label just for consistency (4 columns for both outputs)

The output is 5 columns:
result_type (this will be the same string for all rows: daily_new_wallets)
identifier (string: date as a time stamp at 00:00:00)
value (string: number of new wallets)
group_label (always null - placeholder to make UNION ALL work)
pre_sample_group_size (always null - placeholder to make UNION ALL work)
*/
daily_result AS (
    SELECT
        'daily_new_wallets'                                         AS result_type,
        CAST(DATE_TRUNC('day', first_trade_time) AS VARCHAR)        AS identifier,
        CAST(COUNT(*) AS VARCHAR)                                   AS value,
        NULL                                                        AS group_label,
        NULL                                                        AS pre_sample_group_size  -- placeholder to make union all work (# of cols needs to be consistent between the two tables)
    FROM all_traders
    WHERE (
    (SELECT min_cnt FROM smallest_group) < (SELECT min_group_size FROM params)
    OR NOT (SELECT both_groups_present FROM smallest_group)
    )
    GROUP BY DATE_TRUNC('day', first_trade_time)
)

/*
combines both Ouput A and Output B 
    Note: ONE OF THEM WILL ALWAYS BE BLANK AND THE OTHER WILL BE FILLED.
    THIS IS BASED ON THE PRECONDITIONS SPECIFIED IN THE COMMENTS ABOVE EACH CTE.
*/
SELECT * FROM wallet_result
UNION ALL
SELECT * FROM daily_result
ORDER BY result_type, identifier ASC
