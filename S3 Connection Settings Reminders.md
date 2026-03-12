# S3 Connection Settings Reminders

## IAM User Permissions

When adding a new S3 bucket to this workflow, the IAM user must have **all four** of the following permissions on the bucket. Missing `s3:GetObject` or `s3:ListBucket` will cause Snowflake to fail with `access denied` even though Python uploads succeed.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::YOUR-BUCKET-NAME/*",
        "arn:aws:s3:::YOUR-BUCKET-NAME"
      ]
    }
  ]
}
```

| Permission | Used by | Purpose |
|---|---|---|
| `s3:PutObject` | Python ETL script | Upload CSV files to S3 |
| `s3:DeleteObject` | Workflow `aws s3 rm` step | Clear prefix before each run |
| `s3:GetObject` | Snowflake `COPY INTO` | Read file contents from stage |
| `s3:ListBucket` | Snowflake stage | Discover files matching `PATTERN` |

## Snowflake Stage Credentials

The stage in `snowflake/runbooks/load_fred_from_s3.sql` uses inline `CREDENTIALS`. The `AWS_TOKEN` line uses an `{{OPT:...}}` token — it is included only when a session token exists (temporary/STS credentials) and omitted automatically for long-term static IAM keys.

```sql
CREATE OR REPLACE STAGE FRED_DATA_RAW_STAGE
    URL = 's3://your-bucket/prefix/'
    CREDENTIALS = (
        AWS_KEY_ID     = '<AWS_ACCESS_KEY_ID>'
        AWS_SECRET_KEY = '<AWS_SECRET_ACCESS_KEY>'
        AWS_TOKEN      = '<AWS_SESSION_TOKEN>'   -- omitted if using static keys
    );
```

The GitHub Actions step must explicitly declare `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in its `env:` block — they are **not** automatically inherited from the `aws-actions/configure-aws-credentials` step when a step has its own `env:` block.

```yaml
env:
  AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
  AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
```

## GitHub Secrets Checklist

| Secret | Required for |
|---|---|
| `AWS_ACCESS_KEY_ID` | S3 upload + Snowflake stage |
| `AWS_SECRET_ACCESS_KEY` | S3 upload + Snowflake stage |
| `AWS_SESSION_TOKEN` | Only if using temporary/STS credentials |
| `AWS_REGION` | `aws-actions/configure-aws-credentials` |
| `SNOWFLAKE_ACCOUNT` | Snowflake connection |
| `SNOWFLAKE_USER` | Snowflake connection |
| `SNOWFLAKE_PRIVATE_KEY_B64` | Snowflake key-pair auth (base64-encoded DER) |
| `SNOWFLAKE_DATABASE` | Snowflake connection |
| `SNOWFLAKE_SCHEMA` | Snowflake connection |
| `SNOWFLAKE_WAREHOUSE` | Snowflake connection |
| `SNOWFLAKE_ROLE` | Snowflake connection (optional) |
| `FRED_API_KEY` | FRED API (32-char lowercase alphanumeric) |
