DROP VIEW IF EXISTS client_activity;

CREATE VIEW client_activity AS
WITH deposit_agg AS (
    SELECT
        client_id,
        MIN(deposit_date) AS first_deposit_date,
        MAX(deposit_date) AS last_deposit_date,
        COUNT(*) AS total_deposit_count,
        ROUND(SUM(amount_usd), 2) AS total_deposit_amount_usd
    FROM fact_deposit
    WHERE LOWER(deposit_status) = 'completed'
    GROUP BY client_id
),
trade_agg AS (
    SELECT
        client_id,
        MIN(trade_date) AS first_trade_date,
        MAX(trade_date) AS last_trade_date,
        COUNT(*) AS total_trade_count,
        ROUND(SUM(realized_pnl_usd), 2) AS total_realized_pnl_usd
    FROM fact_trade
    GROUP BY client_id
)
SELECT
    c.client_id,
    c.country,
    s.signup_date,
    c.account_type,
    c.kyc_status,
    d.first_deposit_date,
    t.first_trade_date,
    COALESCE(d.total_deposit_count, 0) AS total_deposit_count,
    COALESCE(d.total_deposit_amount_usd, 0.0) AS total_deposit_amount_usd,
    COALESCE(t.total_trade_count, 0) AS total_trade_count,
    COALESCE(t.total_realized_pnl_usd, 0.0) AS total_realized_pnl_usd,
    t.last_trade_date,
    d.last_deposit_date,
    CASE WHEN COALESCE(d.total_deposit_count, 0) > 0 THEN 1 ELSE 0 END AS is_funded_client,
    CASE WHEN COALESCE(t.total_trade_count, 0) > 0 THEN 1 ELSE 0 END AS is_trading_client
FROM dim_client_current AS c
LEFT JOIN fact_signup AS s
    ON s.client_id = c.client_id
LEFT JOIN deposit_agg AS d
    ON d.client_id = c.client_id
LEFT JOIN trade_agg AS t
    ON t.client_id = c.client_id;
