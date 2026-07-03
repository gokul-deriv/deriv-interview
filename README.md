# deriv-interview

Local-only interview submission for a compact analytics warehouse design built around four layers:

`bronze -> silver -> gold -> curated`

This submission stays intentionally lightweight and self-contained. It uses Markdown for the design, SQLite-compatible SQL for the example models, and local-tool assumptions only:

- `python3`
- `sqlite3`
- `shasum -a 256`
- `cron`
- Markdown + Mermaid

It now also includes a runnable local warehouse implementation backed by SQLite and raw source files under `data/`.
The project is initialized as a local git repository for GitHub repo `gokul-deriv/deriv-interview`, and [.gitignore](/Users/gokulsubramanian/nrs/git/regentmarkets/deriv-interview-gokul/.gitignore) keeps the SQLite database, cache artifacts, and common secret file types out of version control.
The sample source files and exported layer CSVs in this repo are intentionally versioned for interview reproducibility. `warehouse.db`, cache artifacts, and secret-like files remain ignored.

## Deliverables

- `part1_pipeline.md`: pipeline architecture, orchestration, monitoring, data quality, and edge cases
- `part2_data_model.md`: Kimball-style gold model, historization strategy, and curated model design
- `part3_pii.md`: PII masking and local access-control approach
- `code/`: runnable prototype scripts
- `sql/client_activity.sql`: SQLite-compatible curated model
- `sql/query_a_deposit_count_by_country.sql`: SQLite-compatible answer for Query A
- `sql/query_a_fixture.sql`: local fixture used to validate the SQL
- `data/`: raw baseline JSON, vendor CSV, and CDC JSONL inputs
- `warehouse/`: runnable pipeline, persistent SQLite warehouse, and exported layer outputs
- `.gitignore`: keeps the SQLite database, caches, and common secret file types out of version control
- `PROMPTS.md`: prompt history and decision log

## Design commitments

- The warehouse uses four explicit layers: `bronze`, `silver`, `gold`, `curated`.
- Incremental logic lives in `silver`.
- `gold` uses Kimball-style conformed dimensions and facts.
- `curated.client_activity` is the primary analytic output in this version.
- `total_withdrawals` is intentionally excluded because no withdrawal source is provided.

## Directory layout

```text
deriv-interview/
|-- README.md
|-- code/
|   |-- README.md
|   |-- run_pipeline.py
|   `-- validate_sqlite.sh
|-- data/
|   |-- client_signup.json
|   |-- client_profile.json
|   |-- client_deposit.json
|   |-- client_trade.json
|   |-- client_profile_cdc.jsonl
|   |-- deposits_vendor_20240301.csv
|   |-- deposits_vendor_20240302.csv
|   `-- deposits_vendor_20240303.csv
|-- part1_pipeline.md
|-- part2_data_model.md
|-- part3_pii.md
|-- PROMPTS.md
|-- warehouse/
|   |-- README.md
|   |-- bronze/
|   |-- silver/
|   |-- gold/
|   |-- curated/
|   `-- ops/
`-- sql/
    |-- client_activity.sql
    |-- query_a_deposit_count_by_country.sql
    `-- query_a_fixture.sql
```

The deliverable-required structure is the top-level Markdown files plus `sql/`, `code/`, and `PROMPTS.md`. The `data/` and `warehouse/` folders are supporting assets added so the submission is reproducible and runnable locally.

## Run The Pipeline

From the project root:

```bash
python3 code/run_pipeline.py
```

This materializes:

- bronze source-specific raw replica files plus source manifest
- silver standardized tables plus quarantine output
- gold dimensions and facts
- curated `client_activity`
- ops metadata and DQ logs

CSV exports are written under `warehouse/bronze`, `warehouse/silver`, `warehouse/gold`, `warehouse/curated`, and `warehouse/ops`.
The bronze folder now follows the source names directly, for example `client_profile.csv`, `client_deposit.csv`, `client_trade.csv`, and one CSV per vendor delivery file.
Silver and all downstream layers keep masked or derived PII only. Raw PII stays confined to `data/` and bronze.
The `data/` and bronze files in this repo are sample interview fixtures and are versioned intentionally; the SQLite database file remains untracked.

## Quick SQL Validation

From the project root:

```bash
chmod +x code/validate_sqlite.sh
./code/validate_sqlite.sh
```

The validation script:

- builds a temporary SQLite database from `sql/query_a_fixture.sql`
- materializes `client_activity` from `sql/client_activity.sql`
- checks the curated output against expected rows
- checks that Query A returns countries with zero deposits first

## Local operations assumptions

- Bronze ingestion fingerprints each delivered file with `shasum -a 256`.
- `cron` triggers a small `python3` runner, and the runner writes run metadata into a local operations store.
- Alerting defaults to local SMTP/email. A Slack webhook is an optional enhancement if outbound networking exists in the runtime environment.

## Bronze audit fields

- raw source columns exactly as landed for that source
- `source_system`
- `source_file_name`
- `source_file_sha256`
- `load_timestamp`
- `row_number`

## Notes

- The documentation references concrete anomalies from the provided scenario, including `CL031`, `CL099`, `CL030`, `VDEP001`, `VDEP002`, and `VDEP005`.
- The implemented pipeline quarantines the invalid or unresolved rows and records those events in `warehouse/ops/dq_failures.csv` and `warehouse/silver/silver_quarantine.csv`.
- CDC deletes are modeled as soft deletes in history so auditability is preserved.
- The current local setup can transform and mask PII, but strong access control should be handled by a restricted local database layer rather than SQLite alone.
