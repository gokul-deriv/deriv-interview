# Warehouse

This folder contains the runnable local warehouse implementation.

## Layout

- `../code/run_pipeline.py`: end-to-end local runner
- `warehouse.db`: persistent SQLite warehouse database created on first run
- `bronze/`: exported bronze raw replicas named after the source files
- `silver/`: exported silver tables and quarantine output
- `gold/`: exported gold dimensions and facts
- `curated/`: exported curated analytics tables
- `ops/`: exported pipeline runs, stage runs, DQ failures, and manifest tables
- `.gitignore` at the project root excludes `warehouse.db`, cache artifacts, and common secret file types from git

## Run

From the project root:

```bash
python3 code/run_pipeline.py
```

This will:

1. ingest the raw files in `data/` into bronze
2. standardize and validate records into silver
3. rebuild gold dimensions and facts
4. refresh curated outputs
5. export all layers to CSV for inspection

Bronze output examples:

- `warehouse/bronze/client_profile.csv`
- `warehouse/bronze/client_deposit.csv`
- `warehouse/bronze/client_trade.csv`
- `warehouse/bronze/deposits_vendor_20240302.csv`

Each bronze file preserves the raw source columns for that source and appends:

- `source_system`
- `source_file_name`
- `source_file_sha256`
- `load_timestamp`
- `row_number`

PII handling in the runnable pipeline:

- bronze keeps raw landed values for auditability
- silver stores masked email or phone data and derived DOB `age_band` only
- gold and curated exclude raw direct identifiers entirely

## Notes

- The runner is idempotent at the file-ingest level through `source_file_name + source_file_sha256`.
- Re-running with the same raw files will skip duplicate bronze ingestion and rebuild downstream layers deterministically from persisted bronze state.
- The pipeline uses only Python standard library modules plus SQLite.
