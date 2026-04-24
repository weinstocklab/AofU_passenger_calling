#!/usr/bin/env bash
set -euo pipefail

# Example environment for running singleton extraction in the somatic workspace.
# This defaults to a single-chromosome pilot on an autosome before scaling out.

export WORKSPACE_ID="somatic"

# Confirmed referenced bucket in this workspace.
export DATASETS_BUCKET="gs://vwb-aou-datasets-controlled"

# Confirmed AoU short-read VDS path.
export VDS_URI="${DATASETS_BUCKET}/v8/wgs/short_read/snpindel/vds/hail.vds"

# Writable workspace bucket.
export WORKSPACE_BUCKET="gs://working-wb-quick-beet-1004"
export TMP_DIR_URI="${WORKSPACE_BUCKET}/hail-tmp/singletons_ab"
export SCRIPT_GS_URI="${WORKSPACE_BUCKET}/jobs/singletons_ab_from_vds.py"
export OUTPUT_PARQUET_URI="${WORKSPACE_BUCKET}/results/singletons_ab_0p1_0p3.parquet"
export REQUESTER_PAYS_PROJECT="wb-quick-beet-1004"
export REQUESTER_PAYS_BUCKETS="vwb-aou-datasets-controlled"

# Conservative starter cluster settings.
export CLUSTER_RESOURCE_ID="somatic_hail_cluster"
export CLUSTER_ID="somatic-hail-cluster"
export REGION="us-central1"
export MANAGER_MACHINE_TYPE="n2-standard-4"
export WORKER_MACHINE_TYPE="n2-standard-8"
export NUM_WORKERS="4"
export NUM_SECONDARY_WORKERS="20"
export SECONDARY_WORKER_TYPE="spot"
export IDLE_DELETE_TTL="600s"

# Pilot on a single autosome first. If your contigs are numeric, replace chr21 with 21.
export CONTIGS="chr21"

# Requested allele balance filter.
export AB_MIN="0.1"
export AB_MAX="0.3"

# Safer default: do not overwrite unless you set this explicitly.
# export OVERWRITE="1"

bash scripts/run_singletons_ab_wb.sh
