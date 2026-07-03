# deriv-interview-gokul

Local-only interview submission for a compact analytics warehouse design built around four layers:

`bronze -> silver -> gold -> curated`

This submission stays intentionally lightweight and self-contained. It uses Markdown for the design, SQLite-compatible SQL for the example models, and local-tool assumptions only:

- `python3`
- `sqlite3`
- `shasum -a 256`
- `cron`
- Markdown + Mermaid

It now also includes a runnable local warehouse implementation backed by SQLite and raw source files under `data/`.
The project is initialized as a local git repository, and [.gitignore](/Users/gokulsubramanian/nrs/git/regentmarkets/deriv-interview-gokul/.gitignore) keeps raw source data, generated warehouse artifacts, and common secret file types out of version control.

## Deliverables

- `part1_pipeline.md`: pipeline architecture, orchestration, monitoring, data quality, and edge cases
- `part2_data_model.md`: Kimball-style gold model, historization strategy, and curated model design
- `part3_pii.md`: PII masking and local access-control approach
- `sql/client_activity.sql`: SQLite-compatible curated model
- `sql/query_a_deposit_count_by_country.sql`: SQLite-compatible answer for Query A
- `sql/query_a_fixture.sql`: local fixture used to validate the SQL
- `scripts/validate_sqlite.sh`: local validation script
- `data/`: raw baseline JSON, vendor CSV, and CDC JSONL inputs
- `warehouse/`: runnable pipeline, persistent SQLite warehouse, and exported layer outputs
- `.gitignore`: prevents raw source files, generated outputs, and common secrets from being committed
- `PROMPTS.md`: prompt history and decision log

## Design commitments

- The warehouse uses four explicit layers: `bronze`, `silver`, `gold`, `curated`.
- Incremental logic lives in `silver`.
- `gold` uses Kimball-style conformed dimensions and facts.
- `curated.client_activity` is the primary analytic output in this version.
- `total_withdrawals` is intentionally excluded because no withdrawal source is provided.

## Directory layout

```text
deriv-interview-gokul/
|-- README.md
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
|-- scripts/
|   `-- validate_sqlite.sh
|-- warehouse/
|   |-- README.md
|   `-- pipelines/
|       `-- run_pipeline.py
`-- sql/
    |-- client_activity.sql
    |-- query_a_deposit_count_by_country.sql
    `-- query_a_fixture.sql
```

## Run The Pipeline

From `deriv-interview-gokul`:

```bash
python3 warehouse/pipelines/run_pipeline.py
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

## Quick SQL Validation

From `deriv-interview-gokul`:

```bash
chmod +x scripts/validate_sqlite.sh
./scripts/validate_sqlite.sh
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
