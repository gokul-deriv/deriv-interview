# Part 3: PII Handling

## Position

Mask or tokenize sensitive client data in `silver` before it reaches `gold` or `curated`.

Raw unmasked PII should exist only in:

- `bronze`
- a restricted `silver` area used for controlled transformations

This keeps analytics-facing layers useful while sharply reducing the exposure footprint.

## Where masking happens

Recommended masking layer:

- `silver`

Reason:

- bronze remains an immutable audit landing zone
- silver is the first place where schema standardization already occurs
- applying masking there prevents raw PII from leaking into conformed facts, dimensions, or downstream extracts

Examples:

- email -> hashed token plus masked display form
- phone number -> masked last digits only
- address -> removed from analytic layers unless explicitly required
- date of birth -> do not carry the raw value beyond bronze; keep only the least sensitive analytic derivative, such as age band
- payment card fields -> never expose raw values downstream

## Current local-only controls

The local flat-file and SQLite-style setup can transform PII, but it does not provide strong native access control on its own.

Minimum controls for the current setup:

- OS-level file permissions on bronze and restricted silver folders
- separate runtime or service account for pipeline execution
- for real production data, no raw PII committed to Git
- masked fields only in analytic outputs

These controls are enough for a demo-style local design, but they are not a strong long-term security boundary.

For this interview repo specifically, the sample source files are synthetic fixtures and may be versioned for reproducibility. That exception should not be carried over to real source data.

## Recommended access-control method

Recommended stronger control:

- store raw PII in a restricted local PostgreSQL schema, or a similar access-controlled local database
- expose only masked views or tokenized columns to downstream consumers

Why this is better than SQLite alone:

- database grants give a real access boundary
- raw and masked zones can be separated clearly
- auditability and least-privilege access are easier to enforce

## Practical operating model

1. Land source rows with raw PII in bronze.
2. Standardize and validate records in silver.
3. Apply masking or tokenization in silver before publishing conformed outputs.
4. Publish only masked attributes to gold and curated tables.
5. Restrict any exception path for raw PII to named operators and a controlled runtime account.

## Field-level guidance

| Field type | Bronze | Silver restricted area | Gold | Curated |
|---|---|---|---|---|
| Full name | raw allowed | masked or tokenized | masked only if needed | usually excluded |
| Email | raw allowed | hash plus masked display | token or masked value only | excluded unless business need is explicit |
| Phone | raw allowed | masked | masked only | excluded |
| Date of birth | raw allowed | validated and transformed to age band plus anomaly flag | age band or anomaly flag | age band only if needed |
| Payment details | raw allowed if source contains them | tokenize or drop | never raw | never raw |

## Final recommendation

For this local-only submission:

- perform masking in `silver`
- keep raw PII in `bronze` or restricted silver only
- use OS permissions as the immediate control
- recommend a restricted local PostgreSQL schema as the durable access-control method

That combination answers both parts of the question directly:

- masking layer: `silver`
- access-control method: restricted local database schema with downstream masked views
