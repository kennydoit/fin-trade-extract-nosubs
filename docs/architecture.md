# Architecture Reference

## Overview

This project extracts financial and government data from external APIs, stages it in S3, and loads it into Snowflake for analytics.

```
External APIs → Python ETL → S3 (staging) → Snowflake (raw → curated)
```

---

## Snowflake

### Account
| Setting | Value |
|---|---|
| Account | `GIDLNKY-MBC80835` |
| Database | `FIN_TRADE_EXTRACT_NOSUBS` |
| Warehouse | `FIN_TRADE_WH` |
| Default ETL Role | `ETL_ROLE` |

### Schemas
| Schema | Purpose |
|---|---|
| `RAW` | FRED economic data (raw landing zone) |
| `RAW_FBI` | FBI CDE data (raw landing zone) |
| `ANALYTICS` | Curated/transformed data for consumption |

### Connection Pattern
All ETL scripts connect using **RSA private key authentication** (no password). The key file is `snowflake_rsa_key.der` (git-ignored). In CI/CD it is stored base64-encoded as the `SNOWFLAKE_PRIVATE_KEY_B64` secret and decoded at runtime.

### Runbook SQL Templating
SQL runbooks use `{{TOKEN_TYPE:VAR_NAME}}` placeholders resolved by `scripts/github_actions/snowflake_run_sql_file.py`:

| Token | Behavior |
|---|---|
| `{{RAW:VAR}}` | Substitutes env var value as-is |
| `{{SQLSTR:VAR}}` | Substitutes as a single-quoted SQL string (escapes `'`) |
| `{{OPT:VAR}}` | If env var is empty, the entire line is dropped; otherwise treated as `SQLSTR` |

**Example:**
```sql
USE DATABASE {{RAW:SNOWFLAKE_DATABASE}};
CREATE OR REPLACE STAGE MY_STAGE
    URL = {{SQLSTR:S3_URI}}
    CREDENTIALS = (
        AWS_KEY_ID     = {{SQLSTR:AWS_ACCESS_KEY_ID}}
        AWS_SECRET_KEY = {{SQLSTR:AWS_SECRET_ACCESS_KEY}}
        AWS_TOKEN      = {{OPT:AWS_SESSION_TOKEN}}   -- line dropped if no session token
    );
```

### Role Privileges Required
`ETL_ROLE` must have:
- `USAGE` on database `FIN_TRADE_EXTRACT`
- `CREATE SCHEMA` on database `FIN_TRADE_EXTRACT` (for new data domains)
- `ALL` on each schema it owns
- `USAGE` on warehouse `FIN_TRADE_WH`

---

## S3

### Bucket
`fin-trade-extract-nosubs-bucket` (us-east-1)

### Folder Structure
```
fin-trade-extract-nosubs-bucket/
├── fred/                              # FRED economic series
│   └── {SERIES_ID}/
│       └── {SERIES_ID}_{date}.csv
└── fbi/                               # FBI CDE data
    └── agencies/
        └── state={STATE_ABBR}/
            └── {STATE}_{YYYYMMDD_HHMMSS}.csv
```

### Naming Conventions
- Partition folders use Hive-style keys: `state=NY/`, `series=GDP/`
- File names include the date/timestamp of extraction: `NY_20260319_175931.csv`
- Snowflake `COPY INTO` uses `METADATA$FILENAME` to populate `SOURCE_FILE`

### IAM Permissions Required
The AWS IAM user/role used for S3 access needs:
```json
{
  "Effect": "Allow",
  "Action": [
    "s3:PutObject",
    "s3:GetObject",
    "s3:ListBucket",
    "s3:DeleteObject"
  ],
  "Resource": [
    "arn:aws:s3:::fin-trade-extract-nosubs-bucket",
    "arn:aws:s3:::fin-trade-extract-nosubs-bucket/*"
  ]
}
```

---

## ETL Pattern

Each data source follows the same three-step pattern:

### 1. Python ETL Script (`scripts/etl/`)
- Loads credentials from `.env` (local) or environment variables (CI/CD)
- Fetches data from the source API
- Normalizes to a flat CSV with a consistent schema
- Uploads to the appropriate S3 prefix via `boto3`

### 2. Snowflake Runbook (`snowflake/runbooks/`)
- Creates the external stage pointing to the S3 prefix
- Creates the raw landing table (`IF NOT EXISTS`)
- Runs `COPY INTO` with `ON_ERROR = 'CONTINUE'` and `FORCE = TRUE`
- Ends with a verification `SELECT COUNT(*)` or grouped summary

### 3. GitHub Actions Workflow (`.github/workflows/`)
- Triggered on schedule, push, or `workflow_dispatch`
- Configures AWS credentials via `aws-actions/configure-aws-credentials`
- Decodes Snowflake private key from `SNOWFLAKE_PRIVATE_KEY_B64` secret
- Runs ETL script, then runbook via `snowflake_run_sql_file.py`

---

## Adding a New Data Source

1. Create `scripts/etl/fetch_{source}.py` — follow `fetch_fbi_agencies.py` as a template
2. Create `snowflake/runbooks/load_{source}_from_s3.sql` — follow `load_fbi_agencies_from_s3.sql`
3. Create `.github/workflows/{source}_etl.yml` — follow `fred_watermark_etl.yml`
4. Add any new env vars to `.env` (local template) and push via `bash scripts/setup_github_secrets.sh`

---

## Environment Variables Reference

| Variable | Used By | Description |
|---|---|---|
| `SNOWFLAKE_ACCOUNT` | All ETL | Snowflake account identifier |
| `SNOWFLAKE_USER` | All ETL | Snowflake username |
| `SNOWFLAKE_PASSWORD` | Local only | Password (not used in CI — key auth preferred) |
| `SNOWFLAKE_DATABASE` | All ETL | Target database |
| `SNOWFLAKE_SCHEMA` | All ETL | Default schema |
| `SNOWFLAKE_WAREHOUSE` | All ETL | Compute warehouse |
| `SNOWFLAKE_ROLE` | All ETL | Role to assume |
| `SNOWFLAKE_PRIVATE_KEY_PATH` | All ETL | Path to `.der` key file (local) |
| `SNOWFLAKE_PRIVATE_KEY_B64` | CI only | Base64-encoded `.der` key (GitHub secret) |
| `AWS_ACCESS_KEY_ID` | S3 ETL | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | S3 ETL | AWS secret key |
| `AWS_REGION` | S3 ETL | AWS region (default: `us-east-1`) |
| `AWS_SESSION_TOKEN` | S3 ETL | Optional session token (STS/assumed role) |
| `S3_BUCKET` | S3 ETL | Target bucket name |
| `S3_FBI_PREFIX` | FBI ETL | S3 prefix for FBI data (default: `fbi/`) |
| `S3_FBI_AGENCIES_URI` | FBI runbook | Full S3 URI for FBI agencies stage |
| `DATA_GOV_API_KEY` | FBI ETL | API key for `api.usa.gov` |
| `FRED_API_KEY` | FRED ETL | FRED API key |
