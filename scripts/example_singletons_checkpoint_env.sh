#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/example_singletons_common_env.sh"

export JOB_MODE="checkpoint"
export NUM_WORKERS="4"
export NUM_SECONDARY_WORKERS="10"

bash "${SCRIPT_DIR}/run_singletons_ab_wb.sh"
