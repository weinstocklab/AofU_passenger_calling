#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="${SCRIPT_DIR}/output.log"

{
  bash "${SCRIPT_DIR}/example_singletons_checkpoint_env.sh"
  bash "${SCRIPT_DIR}/example_singletons_parquet_env.sh"
} 2>&1 | tee "${LOG_FILE}"
