#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# setup_github_secrets.sh
#
# Pushes all required secrets to the GitHub repository using values from
# the local .env file.
#
# Prerequisites:
#   1. A GitHub Personal Access Token (PAT) with the `secrets` scope.
#      Generate at: https://github.com/settings/tokens
#      (Classic token → scope: repo → secrets:write)
#
#   2. Authenticate gh CLI with your PAT before running this script:
#        gh auth login
#      ...or export the token directly:
#        export GITHUB_TOKEN=<your-pat>
#
#   3. A populated .env file in the repo root.
#
# Usage:
#   chmod +x scripts/setup_github_secrets.sh
#   source .env && bash scripts/setup_github_secrets.sh
# ---------------------------------------------------------------------------
set -euo pipefail

# The codespace injects GITHUB_TOKEN which overrides any stored gh credentials.
# Unset it so the PAT stored via `gh auth login` is used instead.
unset GITHUB_TOKEN || true

REPO="kennydoit/fin-trade-extract-nosubs"

# Source .env from the repo root (if present) with export so all variables
# are visible to this script even when called directly without `source .env`.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env"
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

require_var() {
  local name="$1"
  local val="${!name:-}"
  if [[ -z "$val" ]]; then
    echo "WARNING: $name is unset or empty — skipping."
    return 1
  fi
  return 0
}

set_secret() {
  local name="$1"
  local val="$2"
  echo "  → Setting $name"
  printf '%s' "$val" | gh secret set "$name" --repo "$REPO" --body -
}

echo "=== Setting GitHub repository secrets for $REPO ==="
echo ""

# ── Snowflake (text) ────────────────────────────────────────────────────────
for var in SNOWFLAKE_ACCOUNT SNOWFLAKE_USER SNOWFLAKE_PASSWORD \
           SNOWFLAKE_DATABASE SNOWFLAKE_SCHEMA SNOWFLAKE_WAREHOUSE \
           SNOWFLAKE_ROLE; do
  val="${!var:-}"
  if [[ -n "$val" ]]; then
    set_secret "$var" "$val"
  else
    echo "  (skipping $var — empty)"
  fi
done

# ── Snowflake private key (binary → base64) ─────────────────────────────────
KEY_PATH="${SNOWFLAKE_PRIVATE_KEY_PATH:-snowflake_rsa_key.der}"
if [[ -f "$KEY_PATH" ]]; then
  echo "  → Setting SNOWFLAKE_PRIVATE_KEY_B64 (base64 of $KEY_PATH)"
  base64 < "$KEY_PATH" | gh secret set "SNOWFLAKE_PRIVATE_KEY_B64" --repo "$REPO" --body -
else
  echo "  WARNING: $KEY_PATH not found — SNOWFLAKE_PRIVATE_KEY_B64 not set."
fi

# ── AWS ─────────────────────────────────────────────────────────────────────
for var in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION; do
  val="${!var:-}"
  if [[ -n "$val" ]]; then
    set_secret "$var" "$val"
  else
    echo "  (skipping $var — empty)"
  fi
done

# ── API keys ─────────────────────────────────────────────────────────────────
for var in DATA_GOV_API_KEY FRED_API_KEY; do
  val="${!var:-}"
  if [[ -n "$val" ]]; then
    set_secret "$var" "$val"
  else
    echo "  (skipping $var — empty)"
  fi
done

echo ""
echo "=== Done. Verify at: https://github.com/$REPO/settings/secrets/actions ==="
