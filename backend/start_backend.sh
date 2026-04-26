#!/usr/bin/env bash
set -euo pipefail

export OPENAI_API_KEY="${OPENAI_API_KEY:-CHANGE_ME}"
export OPENAI_MODEL="${OPENAI_MODEL:-gpt-4.1-mini}"
export APP_TOKEN="${APP_TOKEN:-CHANGE_ME_APP_TOKEN}"
export ALLOWED_ORIGIN="${ALLOWED_ORIGIN:-http://localhost}"

python backend.py
