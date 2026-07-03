#!/bin/sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
repo_dir=$(CDPATH= cd -- "$script_dir/.." && pwd)
db_path=$(mktemp "${TMPDIR:-/tmp}/deriv-interview-sqlite.XXXXXX.db")
trap 'rm -f "$db_path"' EXIT INT TERM

sqlite3 "$db_path" < "$repo_dir/sql/query_a_fixture.sql"
sqlite3 "$db_path" < "$repo_dir/sql/client_activity.sql"

actual_client_activity=$(
    sqlite3 -csv "$db_path" "
        SELECT
            client_id,
            country,
            signup_date,
            account_type,
            kyc_status,
            first_deposit_date,
            first_trade_date,
            total_deposit_count,
            total_deposit_amount_usd,
            total_trade_count,
            total_realized_pnl_usd,
            last_trade_date,
            last_deposit_date,
            is_funded_client,
            is_trading_client
        FROM client_activity
        ORDER BY client_id;
    "
)

expected_client_activity=$(printf '%s\n' \
    'CL001,US,2024-03-01,standard,verified,2024-03-05,2024-03-06,2,150.0,2,7.25,2024-03-11,2024-03-10,1,1' \
    'CL002,GB,2024-03-02,demo,pending,2024-03-08,2024-03-09,1,200.0,1,25.0,2024-03-09,2024-03-08,1,1' \
    'CL003,FR,2024-03-03,standard,verified,,,0,0.0,0,0.0,,,0,0' \
    'CL004,ZA,2024-03-04,standard,verified,,,0,0.0,0,0.0,,,0,0'
)

if [ "$actual_client_activity" != "$expected_client_activity" ]; then
    echo "client_activity validation failed"
    echo "Expected:"
    printf '%s\n' "$expected_client_activity"
    echo "Actual:"
    printf '%s\n' "$actual_client_activity"
    exit 1
fi

actual_query_a=$(sqlite3 -csv "$db_path" < "$repo_dir/sql/query_a_deposit_count_by_country.sql")
expected_query_a=$(printf '%s\n' \
    'FR,0' \
    'ZA,0' \
    'US,2' \
    'GB,1'
)

if [ "$actual_query_a" != "$expected_query_a" ]; then
    echo "Query A validation failed"
    echo "Expected:"
    printf '%s\n' "$expected_query_a"
    echo "Actual:"
    printf '%s\n' "$actual_query_a"
    exit 1
fi

echo "SQLite validation passed"
