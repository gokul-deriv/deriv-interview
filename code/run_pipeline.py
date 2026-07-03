#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
import sys
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
WAREHOUSE_DIR = PROJECT_ROOT / "warehouse"
DB_PATH = WAREHOUSE_DIR / "warehouse.db"

LAYER_EXPORTS = {
    "bronze": [],
    "silver": [
        "silver_client_signup",
        "silver_client_profile",
        "silver_client_deposit_snapshot",
        "silver_client_trade",
        "silver_vendor_deposit",
        "silver_client_cdc",
        "silver_quarantine",
    ],
    "gold": [
        "dim_client_current",
        "dim_client_status_risk_scd",
        "fact_client_balance_history",
        "fact_signup",
        "fact_deposit",
        "fact_trade",
        "fact_vendor_deposit_reconciliation",
    ],
    "curated": [
        "client_activity",
    ],
    "ops": [
        "pipeline_runs",
        "pipeline_stage_runs",
        "dq_failures",
        "source_file_manifest",
        "sla_breaches",
    ],
}

SOURCE_CONFIG = {
    "client_signup.json": {
        "source_system": "baseline_signup",
        "format": "json_array",
        "bronze_table": "bronze_client_signup",
    },
    "client_profile.json": {
        "source_system": "baseline_profile",
        "format": "json_array",
        "bronze_table": "bronze_client_profile",
    },
    "client_deposit.json": {
        "source_system": "baseline_deposit",
        "format": "json_array",
        "bronze_table": "bronze_client_deposit",
    },
    "client_trade.json": {
        "source_system": "baseline_trade",
        "format": "json_array",
        "bronze_table": "bronze_client_trade",
    },
    "client_profile_cdc.jsonl": {
        "source_system": "client_cdc",
        "format": "jsonl",
        "bronze_table": "bronze_client_profile_cdc",
    },
}

VENDOR_SOURCE_CONFIG = {
    "source_system": "vendor_deposit",
    "format": "csv",
    "bronze_table": "bronze_vendor_deposit",
}

BRONZE_RAW_COLUMNS = {
    "bronze_client_signup": [
        "client_id",
        "signup_date",
        "country",
        "email",
        "referral_source",
        "account_type",
        "kyc_status",
        "signup_platform",
        "promo_code",
        "assigned_manager",
    ],
    "bronze_client_profile": [
        "client_id",
        "full_name",
        "date_of_birth",
        "phone_number",
        "risk_category",
        "account_status",
        "account_balance_usd",
        "last_login_at",
    ],
    "bronze_client_deposit": [
        "deposit_id",
        "client_id",
        "deposit_date",
        "amount_usd",
        "fee_usd",
        "payment_method",
        "credit_card",
        "processing_days",
        "status",
    ],
    "bronze_client_trade": [
        "trade_id",
        "client_id",
        "trade_date",
        "instrument",
        "volume_lots",
        "realized_pnl_usd",
        "status",
    ],
    "bronze_client_profile_cdc": [
        "lsn",
        "op",
        "client_id",
        "effective_at",
        "risk_category",
        "account_status",
        "account_balance_usd",
    ],
    "bronze_vendor_deposit": [
        "deposit_id",
        "client_id",
        "deposit_date",
        "amount_usd",
        "fee_usd",
        "payment_method",
        "method",
        "processing_days",
        "status",
    ],
}

BRONZE_METADATA_COLUMNS = [
    "source_system",
    "source_file_name",
    "source_file_sha256",
    "load_timestamp",
    "row_number",
]

VENDOR_REQUIRED_HEADERS = {
    "deposit_id",
    "client_id",
    "deposit_date",
    "amount_usd",
    "fee_usd",
    "processing_days",
    "status",
}


@dataclass
class StageMetrics:
    rows_read: int = 0
    rows_loaded: int = 0
    rows_quarantined: int = 0
    error_message: str | None = None
    max_lsn_applied: int | None = None


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def mask_email(email: str | None) -> str | None:
    if not email or "@" not in email:
        return None
    local_part, domain = email.split("@", 1)
    if not local_part:
        return f"***@{domain}"
    return f"{local_part[0]}***@{domain}"


def mask_name(full_name: str | None) -> str | None:
    if not full_name:
        return None
    parts = full_name.split()
    masked_parts = []
    for part in parts:
        if len(part) == 1:
            masked_parts.append(part)
        else:
            masked_parts.append(f"{part[0]}{'*' * (len(part) - 1)}")
    return " ".join(masked_parts)


def mask_phone(phone: str | None) -> str | None:
    if not phone:
        return None
    suffix = phone[-4:] if len(phone) >= 4 else phone
    return f"{'*' * max(len(phone) - 4, 0)}{suffix}"


def parse_float(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    return float(value)


def parse_int(value: Any, default: int | None = None) -> int | None:
    if value is None or value == "":
        return default
    return int(value)


def normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def raw_text(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def derive_status_from_kyc(kyc_status: str | None) -> str:
    mapping = {
        "approved": "active",
        "pending": "pending_review",
        "rejected": "restricted",
    }
    return mapping.get((kyc_status or "").lower(), "unknown")


def bronze_export_name(source_file_name: str) -> str:
    return f"{Path(source_file_name).stem}.csv"


def age_band_from_dob(date_of_birth: str | None) -> str | None:
    if not date_of_birth or len(date_of_birth) < 4 or not date_of_birth[:4].isdigit():
        return None
    birth_year = int(date_of_birth[:4])
    current_year = datetime.now(timezone.utc).year
    age = current_year - birth_year
    if age < 18:
        return "under_18"
    if age < 25:
        return "18_24"
    if age < 35:
        return "25_34"
    if age < 45:
        return "35_44"
    if age < 55:
        return "45_54"
    if age < 65:
        return "55_64"
    return "65_plus"


def ensure_dirs() -> None:
    for directory in ["bronze", "silver", "gold", "curated", "ops", "pipelines"]:
        (WAREHOUSE_DIR / directory).mkdir(parents=True, exist_ok=True)


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id TEXT PRIMARY KEY,
            job_name TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            status TEXT NOT NULL,
            error_message TEXT
        );

        CREATE TABLE IF NOT EXISTS pipeline_stage_runs (
            stage_run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            stage_name TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT NOT NULL,
            status TEXT NOT NULL,
            rows_read INTEGER NOT NULL DEFAULT 0,
            rows_loaded INTEGER NOT NULL DEFAULT 0,
            rows_quarantined INTEGER NOT NULL DEFAULT 0,
            error_message TEXT,
            max_lsn_applied INTEGER
        );

        CREATE TABLE IF NOT EXISTS dq_failures (
            dq_failure_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            stage_name TEXT NOT NULL,
            dq_check TEXT NOT NULL,
            severity TEXT NOT NULL,
            source_file_name TEXT,
            business_key TEXT,
            details TEXT NOT NULL,
            detected_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS source_file_manifest (
            source_file_name TEXT NOT NULL,
            source_file_sha256 TEXT NOT NULL,
            source_system TEXT NOT NULL,
            source_format TEXT NOT NULL,
            header_columns TEXT,
            row_count INTEGER NOT NULL DEFAULT 0,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            ingest_status TEXT NOT NULL,
            PRIMARY KEY (source_file_name, source_file_sha256)
        );

        CREATE TABLE IF NOT EXISTS sla_breaches (
            sla_breach_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            source_file_name TEXT NOT NULL,
            expected_delivery_date TEXT,
            arrived_at TEXT,
            severity TEXT NOT NULL,
            status TEXT NOT NULL,
            details TEXT,
            detected_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS bronze_json_rows (
            bronze_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_system TEXT NOT NULL,
            source_file_name TEXT NOT NULL,
            source_file_sha256 TEXT NOT NULL,
            load_timestamp TEXT NOT NULL,
            row_number INTEGER NOT NULL,
            raw_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS bronze_csv_rows (
            bronze_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_system TEXT NOT NULL,
            source_file_name TEXT NOT NULL,
            source_file_sha256 TEXT NOT NULL,
            load_timestamp TEXT NOT NULL,
            row_number INTEGER NOT NULL,
            raw_csv_line TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS bronze_cdc_rows (
            bronze_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_system TEXT NOT NULL,
            source_file_name TEXT NOT NULL,
            source_file_sha256 TEXT NOT NULL,
            load_timestamp TEXT NOT NULL,
            row_number INTEGER NOT NULL,
            raw_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS bronze_client_signup (
            client_id TEXT,
            signup_date TEXT,
            country TEXT,
            email TEXT,
            referral_source TEXT,
            account_type TEXT,
            kyc_status TEXT,
            signup_platform TEXT,
            promo_code TEXT,
            assigned_manager TEXT,
            source_system TEXT NOT NULL,
            source_file_name TEXT NOT NULL,
            source_file_sha256 TEXT NOT NULL,
            load_timestamp TEXT NOT NULL,
            row_number INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS bronze_client_profile (
            client_id TEXT,
            full_name TEXT,
            date_of_birth TEXT,
            phone_number TEXT,
            risk_category TEXT,
            account_status TEXT,
            account_balance_usd TEXT,
            last_login_at TEXT,
            source_system TEXT NOT NULL,
            source_file_name TEXT NOT NULL,
            source_file_sha256 TEXT NOT NULL,
            load_timestamp TEXT NOT NULL,
            row_number INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS bronze_client_deposit (
            deposit_id TEXT,
            client_id TEXT,
            deposit_date TEXT,
            amount_usd TEXT,
            fee_usd TEXT,
            payment_method TEXT,
            credit_card TEXT,
            processing_days TEXT,
            status TEXT,
            source_system TEXT NOT NULL,
            source_file_name TEXT NOT NULL,
            source_file_sha256 TEXT NOT NULL,
            load_timestamp TEXT NOT NULL,
            row_number INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS bronze_client_trade (
            trade_id TEXT,
            client_id TEXT,
            trade_date TEXT,
            instrument TEXT,
            volume_lots TEXT,
            realized_pnl_usd TEXT,
            status TEXT,
            source_system TEXT NOT NULL,
            source_file_name TEXT NOT NULL,
            source_file_sha256 TEXT NOT NULL,
            load_timestamp TEXT NOT NULL,
            row_number INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS bronze_client_profile_cdc (
            lsn TEXT,
            op TEXT,
            client_id TEXT,
            effective_at TEXT,
            risk_category TEXT,
            account_status TEXT,
            account_balance_usd TEXT,
            source_system TEXT NOT NULL,
            source_file_name TEXT NOT NULL,
            source_file_sha256 TEXT NOT NULL,
            load_timestamp TEXT NOT NULL,
            row_number INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS bronze_vendor_deposit (
            deposit_id TEXT,
            client_id TEXT,
            deposit_date TEXT,
            amount_usd TEXT,
            fee_usd TEXT,
            payment_method TEXT,
            method TEXT,
            processing_days TEXT,
            status TEXT,
            source_system TEXT NOT NULL,
            source_file_name TEXT NOT NULL,
            source_file_sha256 TEXT NOT NULL,
            load_timestamp TEXT NOT NULL,
            row_number INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS silver_client_signup (
            client_id TEXT PRIMARY KEY,
            signup_date TEXT NOT NULL,
            country TEXT NOT NULL,
            email_masked TEXT,
            email_token TEXT,
            referral_source TEXT,
            account_type TEXT,
            kyc_status TEXT,
            signup_platform TEXT,
            promo_code TEXT,
            assigned_manager TEXT,
            source_file_name TEXT NOT NULL,
            source_row_number INTEGER NOT NULL,
            row_hash TEXT NOT NULL
        );

        DROP TABLE IF EXISTS silver_client_profile;

        CREATE TABLE silver_client_profile (
            client_id TEXT PRIMARY KEY,
            full_name_masked TEXT,
            full_name_token TEXT,
            age_band TEXT,
            phone_masked TEXT,
            risk_category TEXT,
            account_status TEXT,
            account_balance_usd REAL,
            last_login_at TEXT,
            is_dob_anomaly INTEGER NOT NULL DEFAULT 0,
            source_file_name TEXT NOT NULL,
            source_row_number INTEGER NOT NULL,
            row_hash TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS silver_client_deposit_snapshot (
            deposit_id TEXT PRIMARY KEY,
            client_id TEXT NOT NULL,
            deposit_date TEXT NOT NULL,
            amount_usd REAL NOT NULL,
            fee_usd REAL,
            payment_method TEXT,
            processing_days INTEGER,
            status TEXT NOT NULL,
            source_file_name TEXT NOT NULL,
            source_row_number INTEGER NOT NULL,
            row_hash TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS silver_client_trade (
            trade_id TEXT PRIMARY KEY,
            client_id TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            instrument TEXT,
            volume_lots REAL,
            realized_pnl_usd REAL NOT NULL,
            status TEXT NOT NULL,
            source_file_name TEXT NOT NULL,
            source_row_number INTEGER NOT NULL,
            row_hash TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS silver_vendor_deposit (
            deposit_id TEXT PRIMARY KEY,
            client_id TEXT NOT NULL,
            deposit_date TEXT NOT NULL,
            amount_usd REAL NOT NULL,
            fee_usd REAL,
            payment_method TEXT,
            processing_days INTEGER,
            status TEXT NOT NULL,
            source_file_name TEXT NOT NULL,
            source_row_number INTEGER NOT NULL,
            row_hash TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS silver_client_cdc (
            lsn INTEGER PRIMARY KEY,
            op TEXT NOT NULL,
            client_id TEXT NOT NULL,
            effective_at TEXT NOT NULL,
            risk_category TEXT,
            account_status TEXT,
            account_balance_usd REAL,
            source_file_name TEXT NOT NULL,
            source_row_number INTEGER NOT NULL,
            row_hash TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS silver_quarantine (
            quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_name TEXT NOT NULL,
            source_file_name TEXT NOT NULL,
            source_row_number INTEGER,
            severity TEXT NOT NULL,
            dq_check TEXT NOT NULL,
            business_key TEXT,
            raw_record TEXT NOT NULL,
            error_message TEXT NOT NULL,
            quarantined_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS dim_client_current (
            client_id TEXT PRIMARY KEY,
            signup_date TEXT,
            country TEXT,
            account_type TEXT,
            kyc_status TEXT,
            risk_category TEXT,
            account_status TEXT,
            account_balance_usd REAL,
            is_deleted INTEGER NOT NULL DEFAULT 0,
            current_effective_from TEXT,
            latest_cdc_lsn INTEGER
        );

        CREATE TABLE IF NOT EXISTS dim_client_status_risk_scd (
            scd_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id TEXT NOT NULL,
            risk_category TEXT,
            account_status TEXT,
            effective_from TEXT NOT NULL,
            effective_to TEXT,
            is_current INTEGER NOT NULL,
            is_deleted INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS fact_client_balance_history (
            balance_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id TEXT NOT NULL,
            account_balance_usd REAL,
            effective_from TEXT NOT NULL,
            effective_to TEXT,
            is_current INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS fact_signup (
            client_id TEXT PRIMARY KEY,
            signup_date TEXT NOT NULL,
            country TEXT NOT NULL,
            referral_source TEXT,
            account_type TEXT,
            kyc_status TEXT,
            signup_platform TEXT,
            promo_code TEXT,
            assigned_manager TEXT
        );

        CREATE TABLE IF NOT EXISTS fact_deposit (
            deposit_id TEXT PRIMARY KEY,
            client_id TEXT NOT NULL,
            deposit_date TEXT NOT NULL,
            amount_usd REAL NOT NULL,
            fee_usd REAL,
            payment_method TEXT,
            processing_days INTEGER,
            deposit_status TEXT NOT NULL,
            source_system TEXT NOT NULL,
            source_file_name TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS fact_trade (
            trade_id TEXT PRIMARY KEY,
            client_id TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            instrument TEXT,
            volume_lots REAL,
            realized_pnl_usd REAL NOT NULL,
            trade_status TEXT NOT NULL,
            source_file_name TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS fact_vendor_deposit_reconciliation (
            reconciliation_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            reconciliation_run_id TEXT NOT NULL,
            deposit_id TEXT NOT NULL,
            client_id TEXT NOT NULL,
            reconciliation_status TEXT NOT NULL,
            variance_amount_usd REAL NOT NULL,
            source_file_name TEXT NOT NULL,
            reconciled_at TEXT NOT NULL
        );
        """
    )
    conn.commit()


def begin_run(conn: sqlite3.Connection, job_name: str) -> str:
    run_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO pipeline_runs (run_id, job_name, started_at, status)
        VALUES (?, ?, ?, ?)
        """,
        (run_id, job_name, now_utc(), "running"),
    )
    conn.commit()
    return run_id


def finish_run(conn: sqlite3.Connection, run_id: str, status: str, error_message: str | None = None) -> None:
    conn.execute(
        """
        UPDATE pipeline_runs
        SET ended_at = ?, status = ?, error_message = ?
        WHERE run_id = ?
        """,
        (now_utc(), status, error_message, run_id),
    )
    conn.commit()


def log_stage(
    conn: sqlite3.Connection,
    run_id: str,
    stage_name: str,
    started_at: str,
    status: str,
    metrics: StageMetrics,
) -> None:
    conn.execute(
        """
        INSERT INTO pipeline_stage_runs (
            run_id,
            stage_name,
            started_at,
            ended_at,
            status,
            rows_read,
            rows_loaded,
            rows_quarantined,
            error_message,
            max_lsn_applied
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            stage_name,
            started_at,
            now_utc(),
            status,
            metrics.rows_read,
            metrics.rows_loaded,
            metrics.rows_quarantined,
            metrics.error_message,
            metrics.max_lsn_applied,
        ),
    )
    conn.commit()


def log_dq_failure(
    conn: sqlite3.Connection,
    run_id: str,
    stage_name: str,
    dq_check: str,
    severity: str,
    source_file_name: str | None,
    business_key: str | None,
    details: str,
) -> None:
    conn.execute(
        """
        INSERT INTO dq_failures (
            run_id,
            stage_name,
            dq_check,
            severity,
            source_file_name,
            business_key,
            details,
            detected_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (run_id, stage_name, dq_check, severity, source_file_name, business_key, details, now_utc()),
    )
    conn.commit()


def quarantine_record(
    conn: sqlite3.Connection,
    entity_name: str,
    source_file_name: str,
    source_row_number: int | None,
    severity: str,
    dq_check: str,
    business_key: str | None,
    raw_record: dict[str, Any],
    error_message: str,
) -> None:
    conn.execute(
        """
        INSERT INTO silver_quarantine (
            entity_name,
            source_file_name,
            source_row_number,
            severity,
            dq_check,
            business_key,
            raw_record,
            error_message,
            quarantined_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            entity_name,
            source_file_name,
            source_row_number,
            severity,
            dq_check,
            business_key,
            canonical_json(raw_record),
            error_message,
            now_utc(),
        ),
    )
    conn.commit()


def stage_wrapper(conn: sqlite3.Connection, run_id: str, stage_name: str, func) -> StageMetrics:
    started_at = now_utc()
    metrics = StageMetrics()
    try:
        metrics = func(conn, run_id)
        log_stage(conn, run_id, stage_name, started_at, "success", metrics)
        return metrics
    except Exception as exc:
        metrics.error_message = str(exc)
        log_stage(conn, run_id, stage_name, started_at, "failed", metrics)
        raise


def detect_source(path: Path) -> dict[str, str] | None:
    if path.name in SOURCE_CONFIG:
        return SOURCE_CONFIG[path.name]
    if path.name.startswith("deposits_vendor_") and path.suffix == ".csv":
        return VENDOR_SOURCE_CONFIG
    return None


def bronze_rows_exist(conn: sqlite3.Connection, table_name: str, source_file_name: str, source_file_sha256: str) -> bool:
    existing = conn.execute(
        f"""
        SELECT 1
        FROM {table_name}
        WHERE source_file_name = ? AND source_file_sha256 = ?
        LIMIT 1
        """,
        (source_file_name, source_file_sha256),
    ).fetchone()
    return existing is not None


def insert_bronze_record(
    conn: sqlite3.Connection,
    table_name: str,
    raw_record: dict[str, Any],
    source_system: str,
    source_file_name: str,
    source_file_sha256: str,
    load_timestamp: str,
    row_number: int,
) -> None:
    raw_columns = BRONZE_RAW_COLUMNS[table_name]
    insert_columns = raw_columns + BRONZE_METADATA_COLUMNS
    placeholders = ", ".join("?" for _ in insert_columns)
    values = [raw_text(raw_record.get(column)) for column in raw_columns] + [
        source_system,
        source_file_name,
        source_file_sha256,
        load_timestamp,
        row_number,
    ]
    conn.execute(
        f"""
        INSERT INTO {table_name} ({", ".join(insert_columns)})
        VALUES ({placeholders})
        """,
        values,
    )


def ingest_bronze(conn: sqlite3.Connection, run_id: str) -> StageMetrics:
    del run_id
    metrics = StageMetrics()

    for path in sorted(DATA_DIR.iterdir()):
        if not path.is_file():
            continue
        config = detect_source(path)
        if config is None:
            continue

        source_sha = file_sha256(path)
        manifest_exists = conn.execute(
            """
            SELECT 1
            FROM source_file_manifest
            WHERE source_file_name = ? AND source_file_sha256 = ?
            """,
            (path.name, source_sha),
        ).fetchone()
        bronze_exists = bronze_rows_exist(conn, config["bronze_table"], path.name, source_sha)
        if manifest_exists and bronze_exists:
            conn.execute(
                """
                UPDATE source_file_manifest
                SET last_seen_at = ?, ingest_status = ?
                WHERE source_file_name = ? AND source_file_sha256 = ?
                """,
                (now_utc(), "duplicate_skip", path.name, source_sha),
            )
            conn.commit()
            continue

        ingested_at = now_utc()
        row_count = 0
        header_columns = None
        bronze_table = config["bronze_table"]

        if config["format"] == "json_array":
            records = json.loads(path.read_text())
            row_count = len(records)
            header_columns = ",".join(BRONZE_RAW_COLUMNS[bronze_table])
            for row_number, record in enumerate(records, start=1):
                insert_bronze_record(
                    conn=conn,
                    table_name=bronze_table,
                    raw_record=record,
                    source_system=config["source_system"],
                    source_file_name=path.name,
                    source_file_sha256=source_sha,
                    load_timestamp=ingested_at,
                    row_number=row_number,
                )

        elif config["format"] == "jsonl":
            lines = [line for line in path.read_text().splitlines() if line.strip()]
            row_count = len(lines)
            header_columns = ",".join(BRONZE_RAW_COLUMNS[bronze_table])
            for row_number, line in enumerate(lines, start=1):
                insert_bronze_record(
                    conn=conn,
                    table_name=bronze_table,
                    raw_record=json.loads(line),
                    source_system=config["source_system"],
                    source_file_name=path.name,
                    source_file_sha256=source_sha,
                    load_timestamp=ingested_at,
                    row_number=row_number,
                )

        elif config["format"] == "csv":
            with path.open(newline="") as handle:
                reader = csv.DictReader(handle)
                headers = reader.fieldnames or []
                header_columns = ",".join(headers)
                for row_number, row in enumerate(reader, start=1):
                    row_count += 1
                    insert_bronze_record(
                        conn=conn,
                        table_name=bronze_table,
                        raw_record=row,
                        source_system=config["source_system"],
                        source_file_name=path.name,
                        source_file_sha256=source_sha,
                        load_timestamp=ingested_at,
                        row_number=row_number,
                    )

        if manifest_exists:
            conn.execute(
                """
                UPDATE source_file_manifest
                SET header_columns = ?,
                    row_count = ?,
                    last_seen_at = ?,
                    ingest_status = ?
                WHERE source_file_name = ? AND source_file_sha256 = ?
                """,
                (header_columns, row_count, ingested_at, "ingested", path.name, source_sha),
            )
        else:
            conn.execute(
                """
                INSERT INTO source_file_manifest (
                    source_file_name,
                    source_file_sha256,
                    source_system,
                    source_format,
                    header_columns,
                    row_count,
                    first_seen_at,
                    last_seen_at,
                    ingest_status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    path.name,
                    source_sha,
                    config["source_system"],
                    config["format"],
                    header_columns,
                    row_count,
                    ingested_at,
                    ingested_at,
                    "ingested",
                ),
            )
        conn.commit()
        metrics.rows_read += row_count
        metrics.rows_loaded += row_count

    export_layer(conn, "bronze")
    export_layer(conn, "ops")
    return metrics


def rebuild_silver(conn: sqlite3.Connection, run_id: str) -> StageMetrics:
    metrics = StageMetrics()
    conn.executescript(
        """
        DELETE FROM silver_client_signup;
        DELETE FROM silver_client_profile;
        DELETE FROM silver_client_deposit_snapshot;
        DELETE FROM silver_client_trade;
        DELETE FROM silver_vendor_deposit;
        DELETE FROM silver_client_cdc;
        DELETE FROM silver_quarantine;
        """
    )
    conn.commit()

    known_clients: set[str] = set()

    signup_rows = conn.execute(
        """
        SELECT *
        FROM bronze_client_signup
        ORDER BY source_file_name, row_number
        """
    ).fetchall()
    for row in signup_rows:
        record = dict(row)
        metrics.rows_read += 1
        client_id = normalize_text(record.get("client_id"))
        if not client_id:
            metrics.rows_quarantined += 1
            quarantine_record(
                conn,
                "client_signup",
                row["source_file_name"],
                row["row_number"],
                "critical",
                "Critical key null check",
                None,
                record,
                "Missing client_id in signup record.",
            )
            log_dq_failure(
                conn,
                run_id,
                "silver",
                "Critical key null check",
                "critical",
                row["source_file_name"],
                None,
                "Signup record missing client_id.",
            )
            continue

        known_clients.add(client_id)
        payload = {
            "client_id": client_id,
            "signup_date": normalize_text(record.get("signup_date")),
            "country": normalize_text(record.get("country")),
            "email_masked": mask_email(normalize_text(record.get("email"))),
            "email_token": stable_hash((normalize_text(record.get("email")) or "").lower()),
            "referral_source": normalize_text(record.get("referral_source")),
            "account_type": normalize_text(record.get("account_type")),
            "kyc_status": normalize_text(record.get("kyc_status")),
            "signup_platform": normalize_text(record.get("signup_platform")),
            "promo_code": normalize_text(record.get("promo_code")),
            "assigned_manager": normalize_text(record.get("assigned_manager")),
            "source_file_name": row["source_file_name"],
            "source_row_number": row["row_number"],
        }
        conn.execute(
            """
            INSERT OR REPLACE INTO silver_client_signup (
                client_id,
                signup_date,
                country,
                email_masked,
                email_token,
                referral_source,
                account_type,
                kyc_status,
                signup_platform,
                promo_code,
                assigned_manager,
                source_file_name,
                source_row_number,
                row_hash
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["client_id"],
                payload["signup_date"],
                payload["country"],
                payload["email_masked"],
                payload["email_token"],
                payload["referral_source"],
                payload["account_type"],
                payload["kyc_status"],
                payload["signup_platform"],
                payload["promo_code"],
                payload["assigned_manager"],
                payload["source_file_name"],
                payload["source_row_number"],
                stable_hash(canonical_json(payload)),
            ),
        )
        metrics.rows_loaded += 1

    profile_rows = conn.execute(
        """
        SELECT *
        FROM bronze_client_profile
        ORDER BY source_file_name, row_number
        """
    ).fetchall()
    for row in profile_rows:
        record = dict(row)
        metrics.rows_read += 1
        client_id = normalize_text(record.get("client_id"))
        if not client_id or client_id not in known_clients:
            metrics.rows_quarantined += 1
            quarantine_record(
                conn,
                "client_profile",
                row["source_file_name"],
                row["row_number"],
                "warning",
                "Referential integrity to client",
                client_id,
                record,
                "Profile references an unknown client_id.",
            )
            log_dq_failure(
                conn,
                run_id,
                "silver",
                "Referential integrity to client",
                "warning",
                row["source_file_name"],
                client_id,
                "Profile references an unknown client_id.",
            )
            continue

        dob = normalize_text(record.get("date_of_birth"))
        dob_year = int(dob[:4]) if dob and len(dob) >= 4 and dob[:4].isdigit() else None
        is_dob_anomaly = 1 if dob_year is not None and dob_year < 1900 else 0
        if is_dob_anomaly:
            log_dq_failure(
                conn,
                run_id,
                "silver",
                "PII plausibility",
                "warning",
                row["source_file_name"],
                client_id,
                f"Implausible date_of_birth detected for {client_id}: {dob}",
            )

        payload = {
            "client_id": client_id,
            "full_name_masked": mask_name(normalize_text(record.get("full_name"))),
            "full_name_token": stable_hash((normalize_text(record.get("full_name")) or "").lower()),
            "age_band": age_band_from_dob(dob),
            "phone_masked": mask_phone(normalize_text(record.get("phone_number"))),
            "risk_category": normalize_text(record.get("risk_category")) or "unknown",
            "account_status": normalize_text(record.get("account_status")) or "unknown",
            "account_balance_usd": parse_float(record.get("account_balance_usd"), 0.0),
            "last_login_at": normalize_text(record.get("last_login_at")),
            "is_dob_anomaly": is_dob_anomaly,
            "source_file_name": row["source_file_name"],
            "source_row_number": row["row_number"],
        }
        conn.execute(
            """
            INSERT OR REPLACE INTO silver_client_profile (
                client_id,
                full_name_masked,
                full_name_token,
                age_band,
                phone_masked,
                risk_category,
                account_status,
                account_balance_usd,
                last_login_at,
                is_dob_anomaly,
                source_file_name,
                source_row_number,
                row_hash
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["client_id"],
                payload["full_name_masked"],
                payload["full_name_token"],
                payload["age_band"],
                payload["phone_masked"],
                payload["risk_category"],
                payload["account_status"],
                payload["account_balance_usd"],
                payload["last_login_at"],
                payload["is_dob_anomaly"],
                payload["source_file_name"],
                payload["source_row_number"],
                stable_hash(canonical_json(payload)),
            ),
        )
        metrics.rows_loaded += 1

    baseline_deposit_seen: set[str] = set()
    deposit_rows = conn.execute(
        """
        SELECT *
        FROM bronze_client_deposit
        ORDER BY source_file_name, row_number
        """
    ).fetchall()
    for row in deposit_rows:
        record = dict(row)
        metrics.rows_read += 1
        standardized = standardize_deposit_record(
            conn=conn,
            run_id=run_id,
            stage_name="silver",
            entity_name="client_deposit",
            record=record,
            source_file_name=row["source_file_name"],
            source_row_number=row["row_number"],
            known_clients=known_clients,
            seen_deposit_ids=baseline_deposit_seen,
            allow_method_alias=False,
            allow_credit_card_alias=True,
        )
        if standardized is None:
            metrics.rows_quarantined += 1
            continue

        conn.execute(
            """
            INSERT INTO silver_client_deposit_snapshot (
                deposit_id,
                client_id,
                deposit_date,
                amount_usd,
                fee_usd,
                payment_method,
                processing_days,
                status,
                source_file_name,
                source_row_number,
                row_hash
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                standardized["deposit_id"],
                standardized["client_id"],
                standardized["deposit_date"],
                standardized["amount_usd"],
                standardized["fee_usd"],
                standardized["payment_method"],
                standardized["processing_days"],
                standardized["status"],
                standardized["source_file_name"],
                standardized["source_row_number"],
                standardized["row_hash"],
            ),
        )
        metrics.rows_loaded += 1

    trade_rows = conn.execute(
        """
        SELECT *
        FROM bronze_client_trade
        ORDER BY source_file_name, row_number
        """
    ).fetchall()
    seen_trade_ids: set[str] = set()
    for row in trade_rows:
        record = dict(row)
        metrics.rows_read += 1
        trade_id = normalize_text(record.get("trade_id"))
        client_id = normalize_text(record.get("client_id"))
        if not trade_id or not client_id:
            metrics.rows_quarantined += 1
            quarantine_record(
                conn,
                "client_trade",
                row["source_file_name"],
                row["row_number"],
                "critical",
                "Critical key null check",
                trade_id,
                record,
                "Trade record missing required trade_id or client_id.",
            )
            log_dq_failure(
                conn,
                run_id,
                "silver",
                "Critical key null check",
                "critical",
                row["source_file_name"],
                trade_id,
                "Trade record missing required trade_id or client_id.",
            )
            continue
        if client_id not in known_clients:
            metrics.rows_quarantined += 1
            quarantine_record(
                conn,
                "client_trade",
                row["source_file_name"],
                row["row_number"],
                "warning",
                "Referential integrity to client",
                trade_id,
                record,
                f"Trade references unknown client_id {client_id}.",
            )
            log_dq_failure(
                conn,
                run_id,
                "silver",
                "Referential integrity to client",
                "warning",
                row["source_file_name"],
                trade_id,
                f"Trade references unknown client_id {client_id}.",
            )
            continue
        if trade_id in seen_trade_ids:
            metrics.rows_quarantined += 1
            quarantine_record(
                conn,
                "client_trade",
                row["source_file_name"],
                row["row_number"],
                "critical",
                "Trade uniqueness",
                trade_id,
                record,
                f"Duplicate trade_id {trade_id} encountered.",
            )
            log_dq_failure(
                conn,
                run_id,
                "silver",
                "Trade uniqueness",
                "critical",
                row["source_file_name"],
                trade_id,
                f"Duplicate trade_id {trade_id} encountered.",
            )
            continue

        seen_trade_ids.add(trade_id)
        payload = {
            "trade_id": trade_id,
            "client_id": client_id,
            "trade_date": normalize_text(record.get("trade_date")),
            "instrument": normalize_text(record.get("instrument")),
            "volume_lots": parse_float(record.get("volume_lots"), 0.0),
            "realized_pnl_usd": parse_float(record.get("realized_pnl_usd"), 0.0),
            "status": normalize_text(record.get("status")) or "unknown",
            "source_file_name": row["source_file_name"],
            "source_row_number": row["row_number"],
        }
        conn.execute(
            """
            INSERT INTO silver_client_trade (
                trade_id,
                client_id,
                trade_date,
                instrument,
                volume_lots,
                realized_pnl_usd,
                status,
                source_file_name,
                source_row_number,
                row_hash
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["trade_id"],
                payload["client_id"],
                payload["trade_date"],
                payload["instrument"],
                payload["volume_lots"],
                payload["realized_pnl_usd"],
                payload["status"],
                payload["source_file_name"],
                payload["source_row_number"],
                stable_hash(canonical_json(payload)),
            ),
        )
        metrics.rows_loaded += 1

    vendor_seen_deposit_ids: set[str] = set()
    vendor_manifests = conn.execute(
        """
        SELECT source_file_name, header_columns
        FROM source_file_manifest
        WHERE source_system = 'vendor_deposit'
        ORDER BY source_file_name
        """
    ).fetchall()
    for manifest in vendor_manifests:
        headers = (manifest["header_columns"] or "").split(",")
        header_set = set(headers)
        missing_headers = sorted(VENDOR_REQUIRED_HEADERS - header_set)
        has_payment_method = "payment_method" in header_set
        has_method_alias = "method" in header_set

        if missing_headers or (not has_payment_method and not has_method_alias):
            metrics.rows_quarantined += 1
            details = (
                f"Vendor file {manifest['source_file_name']} missing required headers: "
                f"{', '.join(missing_headers + (['payment_method_or_method'] if not has_payment_method and not has_method_alias else []))}"
            )
            log_dq_failure(
                conn,
                run_id,
                "silver",
                "Vendor schema conformance",
                "critical",
                manifest["source_file_name"],
                None,
                details,
            )
            continue

        if has_method_alias and not has_payment_method:
            log_dq_failure(
                conn,
                run_id,
                "silver",
                "Vendor schema conformance",
                "warning",
                manifest["source_file_name"],
                None,
                "Mapped method to payment_method during standardization.",
            )

        vendor_rows = conn.execute(
            """
            SELECT *
            FROM bronze_vendor_deposit
            WHERE source_file_name = ?
            ORDER BY row_number
            """,
            (manifest["source_file_name"],),
        ).fetchall()
        for row in vendor_rows:
            metrics.rows_read += 1
            record = dict(row)
            standardized = standardize_deposit_record(
                conn=conn,
                run_id=run_id,
                stage_name="silver",
                entity_name="vendor_deposit",
                record=record,
                source_file_name=manifest["source_file_name"],
                source_row_number=row["row_number"],
                known_clients=known_clients,
                seen_deposit_ids=vendor_seen_deposit_ids,
                allow_method_alias=True,
                allow_credit_card_alias=False,
            )
            if standardized is None:
                metrics.rows_quarantined += 1
                continue

            conn.execute(
                """
                INSERT INTO silver_vendor_deposit (
                    deposit_id,
                    client_id,
                    deposit_date,
                    amount_usd,
                    fee_usd,
                    payment_method,
                    processing_days,
                    status,
                    source_file_name,
                    source_row_number,
                    row_hash
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    standardized["deposit_id"],
                    standardized["client_id"],
                    standardized["deposit_date"],
                    standardized["amount_usd"],
                    standardized["fee_usd"],
                    standardized["payment_method"],
                    standardized["processing_days"],
                    standardized["status"],
                    standardized["source_file_name"],
                    standardized["source_row_number"],
                    standardized["row_hash"],
                ),
            )
            metrics.rows_loaded += 1

    cdc_rows = conn.execute(
        """
        SELECT *
        FROM bronze_client_profile_cdc
        ORDER BY source_file_name, row_number
        """
    ).fetchall()
    parsed_cdc = []
    seen_lsn: set[int] = set()
    for row in cdc_rows:
        record = dict(row)
        metrics.rows_read += 1
        lsn = parse_int(record.get("lsn"))
        if lsn is None:
            metrics.rows_quarantined += 1
            quarantine_record(
                conn,
                "client_cdc",
                row["source_file_name"],
                row["row_number"],
                "critical",
                "Critical key null check",
                None,
                record,
                "CDC record missing lsn.",
            )
            log_dq_failure(
                conn,
                run_id,
                "silver",
                "Critical key null check",
                "critical",
                row["source_file_name"],
                None,
                "CDC record missing lsn.",
            )
            continue
        if lsn in seen_lsn:
            raise ValueError(f"Duplicate LSN detected in CDC stream: {lsn}")
        seen_lsn.add(lsn)
        parsed_cdc.append(
            {
                "lsn": lsn,
                "op": normalize_text(record.get("op")),
                "client_id": normalize_text(record.get("client_id")),
                "effective_at": normalize_text(record.get("effective_at")),
                "risk_category": normalize_text(record.get("risk_category")),
                "account_status": normalize_text(record.get("account_status")),
                "account_balance_usd": parse_float(record.get("account_balance_usd"), 0.0),
                "source_file_name": row["source_file_name"],
                "source_row_number": row["row_number"],
            }
        )

    for record in sorted(parsed_cdc, key=lambda item: item["lsn"]):
        conn.execute(
            """
            INSERT INTO silver_client_cdc (
                lsn,
                op,
                client_id,
                effective_at,
                risk_category,
                account_status,
                account_balance_usd,
                source_file_name,
                source_row_number,
                row_hash
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["lsn"],
                record["op"],
                record["client_id"],
                record["effective_at"],
                record["risk_category"],
                record["account_status"],
                record["account_balance_usd"],
                record["source_file_name"],
                record["source_row_number"],
                stable_hash(canonical_json(record)),
            ),
        )
        metrics.rows_loaded += 1
        metrics.max_lsn_applied = record["lsn"]

    conn.commit()
    export_layer(conn, "silver")
    export_layer(conn, "ops")
    return metrics


def standardize_deposit_record(
    conn: sqlite3.Connection,
    run_id: str,
    stage_name: str,
    entity_name: str,
    record: dict[str, Any],
    source_file_name: str,
    source_row_number: int,
    known_clients: set[str],
    seen_deposit_ids: set[str],
    allow_method_alias: bool,
    allow_credit_card_alias: bool,
) -> dict[str, Any] | None:
    deposit_id = normalize_text(record.get("deposit_id"))
    client_id = normalize_text(record.get("client_id"))

    if not deposit_id or not client_id:
        quarantine_record(
            conn,
            entity_name,
            source_file_name,
            source_row_number,
            "critical",
            "Critical key null check",
            deposit_id,
            record,
            "Deposit record missing deposit_id or client_id.",
        )
        log_dq_failure(
            conn,
            run_id,
            stage_name,
            "Critical key null check",
            "critical",
            source_file_name,
            deposit_id,
            "Deposit record missing deposit_id or client_id.",
        )
        return None

    payment_method = normalize_text(record.get("payment_method"))
    if allow_method_alias and not payment_method and normalize_text(record.get("method")):
        payment_method = normalize_text(record.get("method"))
        log_dq_failure(
            conn,
            run_id,
            stage_name,
            "Vendor schema conformance",
            "warning",
            source_file_name,
            deposit_id,
            f"Mapped method to payment_method for deposit {deposit_id}.",
        )
    if allow_credit_card_alias and not payment_method and normalize_text(record.get("credit_card")):
        payment_method = normalize_text(record.get("credit_card"))
        log_dq_failure(
            conn,
            run_id,
            stage_name,
            "Baseline schema anomaly",
            "warning",
            source_file_name,
            deposit_id,
            f"Mapped credit_card to payment_method for deposit {deposit_id}.",
        )

    amount_usd = parse_float(record.get("amount_usd"), 0.0)
    fee_usd = parse_float(record.get("fee_usd"), 0.0)
    processing_days = parse_int(record.get("processing_days"), 0)
    status = normalize_text(record.get("status")) or "unknown"
    deposit_date = normalize_text(record.get("deposit_date"))

    if deposit_id in seen_deposit_ids:
        quarantine_record(
            conn,
            entity_name,
            source_file_name,
            source_row_number,
            "critical",
            "Deposit uniqueness",
            deposit_id,
            record,
            f"Duplicate deposit_id {deposit_id} encountered after standardization.",
        )
        log_dq_failure(
            conn,
            run_id,
            stage_name,
            "Deposit uniqueness",
            "critical",
            source_file_name,
            deposit_id,
            f"Duplicate deposit_id {deposit_id} encountered after standardization.",
        )
        return None

    if client_id not in known_clients:
        quarantine_record(
            conn,
            entity_name,
            source_file_name,
            source_row_number,
            "warning",
            "Referential integrity to client",
            deposit_id,
            record,
            f"Deposit references unknown client_id {client_id}.",
        )
        log_dq_failure(
            conn,
            run_id,
            stage_name,
            "Referential integrity to client",
            "warning",
            source_file_name,
            deposit_id,
            f"Deposit references unknown client_id {client_id}.",
        )
        return None

    if status.lower() == "completed" and amount_usd is not None and amount_usd < 0:
        quarantine_record(
            conn,
            entity_name,
            source_file_name,
            source_row_number,
            "critical",
            "Financial validity",
            deposit_id,
            record,
            f"Completed deposit {deposit_id} has a negative amount_usd.",
        )
        log_dq_failure(
            conn,
            run_id,
            stage_name,
            "Financial validity",
            "critical",
            source_file_name,
            deposit_id,
            f"Completed deposit {deposit_id} has a negative amount_usd.",
        )
        return None

    seen_deposit_ids.add(deposit_id)
    payload = {
        "deposit_id": deposit_id,
        "client_id": client_id,
        "deposit_date": deposit_date,
        "amount_usd": amount_usd,
        "fee_usd": fee_usd,
        "payment_method": payment_method or "unknown",
        "processing_days": processing_days,
        "status": status,
        "source_file_name": source_file_name,
        "source_row_number": source_row_number,
    }
    payload["row_hash"] = stable_hash(canonical_json(payload))
    return payload


def rebuild_gold(conn: sqlite3.Connection, run_id: str) -> StageMetrics:
    metrics = StageMetrics()
    conn.executescript(
        """
        DELETE FROM dim_client_current;
        DELETE FROM dim_client_status_risk_scd;
        DELETE FROM fact_client_balance_history;
        DELETE FROM fact_signup;
        DELETE FROM fact_deposit;
        DELETE FROM fact_trade;
        DELETE FROM fact_vendor_deposit_reconciliation;
        """
    )
    conn.commit()

    signup_rows = conn.execute("SELECT * FROM silver_client_signup ORDER BY client_id").fetchall()
    profile_map = {
        row["client_id"]: row
        for row in conn.execute("SELECT * FROM silver_client_profile ORDER BY client_id").fetchall()
    }
    cdc_by_client: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in conn.execute("SELECT * FROM silver_client_cdc ORDER BY lsn").fetchall():
        cdc_by_client[row["client_id"]].append(row)

    for row in signup_rows:
        metrics.rows_read += 1
        conn.execute(
            """
            INSERT INTO fact_signup (
                client_id,
                signup_date,
                country,
                referral_source,
                account_type,
                kyc_status,
                signup_platform,
                promo_code,
                assigned_manager
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["client_id"],
                row["signup_date"],
                row["country"],
                row["referral_source"],
                row["account_type"],
                row["kyc_status"],
                row["signup_platform"],
                row["promo_code"],
                row["assigned_manager"],
            ),
        )
        metrics.rows_loaded += 1

    deposit_sources = [
        ("baseline_deposit", "silver_client_deposit_snapshot"),
        ("vendor_deposit", "silver_vendor_deposit"),
    ]
    for source_system, table_name in deposit_sources:
        rows = conn.execute(f"SELECT * FROM {table_name} ORDER BY deposit_id").fetchall()
        for row in rows:
            metrics.rows_read += 1
            conn.execute(
                """
                INSERT INTO fact_deposit (
                    deposit_id,
                    client_id,
                    deposit_date,
                    amount_usd,
                    fee_usd,
                    payment_method,
                    processing_days,
                    deposit_status,
                    source_system,
                    source_file_name
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["deposit_id"],
                    row["client_id"],
                    row["deposit_date"],
                    row["amount_usd"],
                    row["fee_usd"],
                    row["payment_method"],
                    row["processing_days"],
                    row["status"],
                    source_system,
                    row["source_file_name"],
                ),
            )
            metrics.rows_loaded += 1

    trade_rows = conn.execute("SELECT * FROM silver_client_trade ORDER BY trade_id").fetchall()
    for row in trade_rows:
        metrics.rows_read += 1
        conn.execute(
            """
            INSERT INTO fact_trade (
                trade_id,
                client_id,
                trade_date,
                instrument,
                volume_lots,
                realized_pnl_usd,
                trade_status,
                source_file_name
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["trade_id"],
                row["client_id"],
                row["trade_date"],
                row["instrument"],
                row["volume_lots"],
                row["realized_pnl_usd"],
                row["status"],
                row["source_file_name"],
            ),
        )
        metrics.rows_loaded += 1

    signup_map = {row["client_id"]: row for row in signup_rows}
    all_client_ids = set(signup_map) | set(profile_map) | set(cdc_by_client)

    for client_id in sorted(all_client_ids):
        signup = signup_map.get(client_id)
        profile = profile_map.get(client_id)
        signup_date = signup["signup_date"] if signup else None
        country = signup["country"] if signup else "unknown"
        account_type = signup["account_type"] if signup else "inferred"
        kyc_status = signup["kyc_status"] if signup else "unknown"
        risk_category = profile["risk_category"] if profile else "unknown"
        account_status = profile["account_status"] if profile else derive_status_from_kyc(kyc_status)
        account_balance_usd = profile["account_balance_usd"] if profile else 0.0
        current_effective_from = f"{signup_date}T00:00:00Z" if signup_date else now_utc()

        status_segments = []
        balance_segments = []
        latest_lsn = None
        deleted = 0

        base_state = {
            "risk_category": risk_category,
            "account_status": account_status,
            "account_balance_usd": account_balance_usd,
            "is_deleted": deleted,
        }
        status_segments.append(
            {
                "client_id": client_id,
                "risk_category": base_state["risk_category"],
                "account_status": base_state["account_status"],
                "effective_from": current_effective_from,
                "effective_to": None,
                "is_current": 1,
                "is_deleted": deleted,
            }
        )
        balance_segments.append(
            {
                "client_id": client_id,
                "account_balance_usd": base_state["account_balance_usd"],
                "effective_from": current_effective_from,
                "effective_to": None,
                "is_current": 1,
            }
        )

        current_state = dict(base_state)
        for cdc_row in cdc_by_client.get(client_id, []):
            latest_lsn = cdc_row["lsn"]
            event_time = cdc_row["effective_at"]
            op = cdc_row["op"]

            if op == "insert" and signup is not None:
                log_dq_failure(
                    conn,
                    run_id,
                    "gold",
                    "CDC baseline overlap",
                    "warning",
                    cdc_row["source_file_name"],
                    client_id,
                    f"CDC insert overlaps baseline snapshot for {client_id}; reconciled as upsert.",
                )
            if op == "insert" and signup is None:
                log_dq_failure(
                    conn,
                    run_id,
                    "gold",
                    "Late-arriving dimensions",
                    "warning",
                    cdc_row["source_file_name"],
                    client_id,
                    f"Inferred client member created from CDC for unknown client {client_id}.",
                )

            next_risk = cdc_row["risk_category"] or current_state["risk_category"]
            next_status = cdc_row["account_status"] or current_state["account_status"]
            next_balance = (
                cdc_row["account_balance_usd"]
                if cdc_row["account_balance_usd"] is not None
                else current_state["account_balance_usd"]
            )
            next_deleted = 1 if op == "delete" else 0

            if (
                next_risk != current_state["risk_category"]
                or next_status != current_state["account_status"]
                or next_deleted != current_state["is_deleted"]
            ):
                status_segments[-1]["effective_to"] = event_time
                status_segments[-1]["is_current"] = 0
                status_segments.append(
                    {
                        "client_id": client_id,
                        "risk_category": next_risk,
                        "account_status": next_status,
                        "effective_from": event_time,
                        "effective_to": None,
                        "is_current": 1,
                        "is_deleted": next_deleted,
                    }
                )

            if next_balance != current_state["account_balance_usd"]:
                balance_segments[-1]["effective_to"] = event_time
                balance_segments[-1]["is_current"] = 0
                balance_segments.append(
                    {
                        "client_id": client_id,
                        "account_balance_usd": next_balance,
                        "effective_from": event_time,
                        "effective_to": None,
                        "is_current": 1,
                    }
                )

            current_state = {
                "risk_category": next_risk,
                "account_status": next_status,
                "account_balance_usd": next_balance,
                "is_deleted": next_deleted,
            }
            current_effective_from = event_time

        for segment in status_segments:
            conn.execute(
                """
                INSERT INTO dim_client_status_risk_scd (
                    client_id,
                    risk_category,
                    account_status,
                    effective_from,
                    effective_to,
                    is_current,
                    is_deleted
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    segment["client_id"],
                    segment["risk_category"],
                    segment["account_status"],
                    segment["effective_from"],
                    segment["effective_to"],
                    segment["is_current"],
                    segment["is_deleted"],
                ),
            )
            metrics.rows_loaded += 1

        for segment in balance_segments:
            conn.execute(
                """
                INSERT INTO fact_client_balance_history (
                    client_id,
                    account_balance_usd,
                    effective_from,
                    effective_to,
                    is_current
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    segment["client_id"],
                    segment["account_balance_usd"],
                    segment["effective_from"],
                    segment["effective_to"],
                    segment["is_current"],
                ),
            )
            metrics.rows_loaded += 1

        conn.execute(
            """
            INSERT INTO dim_client_current (
                client_id,
                signup_date,
                country,
                account_type,
                kyc_status,
                risk_category,
                account_status,
                account_balance_usd,
                is_deleted,
                current_effective_from,
                latest_cdc_lsn
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                client_id,
                signup_date,
                country,
                account_type,
                kyc_status,
                current_state["risk_category"],
                current_state["account_status"],
                current_state["account_balance_usd"],
                current_state["is_deleted"],
                current_effective_from,
                latest_lsn,
            ),
        )
        metrics.rows_loaded += 1

    vendor_rows = conn.execute("SELECT * FROM silver_vendor_deposit ORDER BY deposit_id").fetchall()
    fact_deposit_ids = {
        row["deposit_id"]
        for row in conn.execute("SELECT deposit_id FROM fact_deposit").fetchall()
    }
    for row in vendor_rows:
        metrics.rows_read += 1
        status = "matched_to_fact" if row["deposit_id"] in fact_deposit_ids else "missing_from_fact"
        conn.execute(
            """
            INSERT INTO fact_vendor_deposit_reconciliation (
                reconciliation_run_id,
                deposit_id,
                client_id,
                reconciliation_status,
                variance_amount_usd,
                source_file_name,
                reconciled_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                row["deposit_id"],
                row["client_id"],
                status,
                0.0,
                row["source_file_name"],
                now_utc(),
            ),
        )
        metrics.rows_loaded += 1

    conn.commit()
    export_layer(conn, "gold")
    export_layer(conn, "ops")
    return metrics


def rebuild_curated(conn: sqlite3.Connection, run_id: str) -> StageMetrics:
    del run_id
    metrics = StageMetrics()
    conn.execute("DROP TABLE IF EXISTS client_activity")
    conn.execute(
        """
        CREATE TABLE client_activity AS
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
            WHERE LOWER(trade_status) = 'closed'
            GROUP BY client_id
        )
        SELECT
            dc.client_id,
            dc.country,
            fs.signup_date,
            dc.account_type,
            fs.kyc_status,
            da.first_deposit_date,
            ta.first_trade_date,
            COALESCE(da.total_deposit_count, 0) AS total_deposit_count,
            COALESCE(da.total_deposit_amount_usd, 0.0) AS total_deposit_amount_usd,
            COALESCE(ta.total_trade_count, 0) AS total_trade_count,
            COALESCE(ta.total_realized_pnl_usd, 0.0) AS total_realized_pnl_usd,
            ta.last_trade_date,
            da.last_deposit_date,
            CASE WHEN COALESCE(da.total_deposit_count, 0) > 0 THEN 1 ELSE 0 END AS is_funded_client,
            CASE WHEN COALESCE(ta.total_trade_count, 0) > 0 THEN 1 ELSE 0 END AS is_trading_client
        FROM dim_client_current AS dc
        LEFT JOIN fact_signup AS fs
            ON fs.client_id = dc.client_id
        LEFT JOIN deposit_agg AS da
            ON da.client_id = dc.client_id
        LEFT JOIN trade_agg AS ta
            ON ta.client_id = dc.client_id
        WHERE dc.is_deleted = 0
        ORDER BY dc.client_id
        """
    )
    conn.commit()
    metrics.rows_loaded = conn.execute("SELECT COUNT(*) AS count_rows FROM client_activity").fetchone()["count_rows"]
    export_layer(conn, "curated")
    return metrics


def export_layer(conn: sqlite3.Connection, layer_name: str) -> None:
    layer_dir = WAREHOUSE_DIR / layer_name
    layer_dir.mkdir(parents=True, exist_ok=True)
    if layer_name == "bronze":
        export_bronze_sources(conn, layer_dir)
        return
    for table_name in LAYER_EXPORTS[layer_name]:
        export_table(conn, table_name, layer_dir / f"{table_name}.csv")


def export_bronze_sources(conn: sqlite3.Connection, layer_dir: Path) -> None:
    for existing_csv in layer_dir.glob("*.csv"):
        existing_csv.unlink()

    for source_file_name, config in SOURCE_CONFIG.items():
        export_table(
            conn,
            config["bronze_table"],
            layer_dir / bronze_export_name(source_file_name),
            where_clause="source_file_name = ?",
            where_params=(source_file_name,),
            order_by="row_number",
        )

    vendor_files = conn.execute(
        """
        SELECT DISTINCT source_file_name
        FROM bronze_vendor_deposit
        ORDER BY source_file_name
        """
    ).fetchall()
    for row in vendor_files:
        export_table(
            conn,
            "bronze_vendor_deposit",
            layer_dir / bronze_export_name(row["source_file_name"]),
            where_clause="source_file_name = ?",
            where_params=(row["source_file_name"],),
            order_by="row_number",
        )


def export_table(
    conn: sqlite3.Connection,
    table_name: str,
    output_path: Path,
    where_clause: str | None = None,
    where_params: tuple[Any, ...] = (),
    order_by: str | None = None,
) -> None:
    query = f"SELECT * FROM {table_name}"
    if where_clause:
        query += f" WHERE {where_clause}"
    if order_by:
        query += f" ORDER BY {order_by}"
    cursor = conn.execute(query, where_params)
    rows = cursor.fetchall()
    headers = [column[0] for column in cursor.description]
    with output_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        for row in rows:
            writer.writerow([row[column] for column in headers])


def print_summary(conn: sqlite3.Connection, run_id: str) -> None:
    stage_rows = conn.execute(
        """
        SELECT stage_name, status, rows_read, rows_loaded, rows_quarantined, max_lsn_applied
        FROM pipeline_stage_runs
        WHERE run_id = ?
        ORDER BY stage_run_id
        """,
        (run_id,),
    ).fetchall()
    print(f"Pipeline run complete: {run_id}")
    for row in stage_rows:
        print(
            f" - {row['stage_name']}: status={row['status']}, "
            f"rows_read={row['rows_read']}, rows_loaded={row['rows_loaded']}, "
            f"rows_quarantined={row['rows_quarantined']}, max_lsn_applied={row['max_lsn_applied']}"
        )


def run_pipeline(job_name: str) -> int:
    ensure_dirs()
    conn = connect_db()
    ensure_schema(conn)
    run_id = begin_run(conn, job_name)
    try:
        stage_wrapper(conn, run_id, "bronze", ingest_bronze)
        stage_wrapper(conn, run_id, "silver", rebuild_silver)
        stage_wrapper(conn, run_id, "gold", rebuild_gold)
        stage_wrapper(conn, run_id, "curated", rebuild_curated)
        finish_run(conn, run_id, "success")
        print_summary(conn, run_id)
        return 0
    except Exception as exc:
        finish_run(conn, run_id, "failed", str(exc))
        print(f"Pipeline failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the local deriv interview warehouse pipeline.")
    parser.add_argument("--job", default="all", help="Logical job name recorded in ops metadata.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return run_pipeline(args.job)


if __name__ == "__main__":
    raise SystemExit(main())
