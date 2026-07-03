DROP TABLE IF EXISTS dim_client_current;
DROP TABLE IF EXISTS fact_signup;
DROP TABLE IF EXISTS fact_deposit;
DROP TABLE IF EXISTS fact_trade;

CREATE TABLE dim_client_current (
    client_id TEXT PRIMARY KEY,
    country TEXT NOT NULL,
    account_type TEXT NOT NULL,
    kyc_status TEXT NOT NULL
);

CREATE TABLE fact_signup (
    client_id TEXT PRIMARY KEY,
    signup_date TEXT NOT NULL,
    signup_channel TEXT,
    country_at_signup TEXT
);

CREATE TABLE fact_deposit (
    deposit_id TEXT PRIMARY KEY,
    client_id TEXT NOT NULL,
    deposit_date TEXT NOT NULL,
    amount_usd REAL NOT NULL,
    fee_usd REAL NOT NULL,
    payment_method TEXT,
    deposit_status TEXT NOT NULL
);

CREATE TABLE fact_trade (
    trade_id TEXT PRIMARY KEY,
    client_id TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    instrument TEXT,
    realized_pnl_usd REAL NOT NULL
);

INSERT INTO dim_client_current (client_id, country, account_type, kyc_status) VALUES
    ('CL001', 'US', 'standard', 'verified'),
    ('CL002', 'GB', 'demo', 'pending'),
    ('CL003', 'FR', 'standard', 'verified'),
    ('CL004', 'ZA', 'standard', 'verified');

INSERT INTO fact_signup (client_id, signup_date, signup_channel, country_at_signup) VALUES
    ('CL001', '2024-03-01', 'organic', 'US'),
    ('CL002', '2024-03-02', 'affiliate', 'GB'),
    ('CL003', '2024-03-03', 'organic', 'FR'),
    ('CL004', '2024-03-04', 'paid_search', 'ZA');

INSERT INTO fact_deposit (deposit_id, client_id, deposit_date, amount_usd, fee_usd, payment_method, deposit_status) VALUES
    ('DEP001', 'CL001', '2024-03-05', 100.00, 2.50, 'bank_transfer', 'completed'),
    ('DEP002', 'CL001', '2024-03-10', 50.00, 1.00, 'credit_card', 'completed'),
    ('DEP003', 'CL002', '2024-03-08', 200.00, 3.00, 'card', 'completed');

INSERT INTO fact_trade (trade_id, client_id, trade_date, instrument, realized_pnl_usd) VALUES
    ('TR001', 'CL001', '2024-03-06', 'EURUSD', 12.50),
    ('TR002', 'CL001', '2024-03-11', 'XAUUSD', -5.25),
    ('TR003', 'CL002', '2024-03-09', 'BTCUSD', 25.00);
