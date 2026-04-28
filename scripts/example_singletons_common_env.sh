#!/usr/bin/env bash
set -euo pipefail

export WORKSPACE_ID="somatic"

export DATASETS_BUCKET="gs://vwb-aou-datasets-controlled"
export VDS_URI="${DATASETS_BUCKET}/v8/wgs/short_read/snpindel/vds/hail.vds"

export WORKSPACE_BUCKET="gs://working-wb-quick-beet-1004"
export TMP_DIR_URI="${WORKSPACE_BUCKET}/hail-tmp/singletons_ab"
export SCRIPT_GS_URI="${WORKSPACE_BUCKET}/jobs/singletons_ab_from_vds.py"
export CHECKPOINT_HT_URI="${WORKSPACE_BUCKET}/checkpoints/singletons_ab_0p1_0p3_chr21.ht"
export OUTPUT_PARQUET_URI="${WORKSPACE_BUCKET}/results/singletons_ab_0p1_0p3.parquet"
export REQUESTER_PAYS_PROJECT="wb-quick-beet-1004"
export REQUESTER_PAYS_BUCKETS="vwb-aou-datasets-controlled"

export CLUSTER_RESOURCE_ID="somatic_hail_cluster"
export CLUSTER_ID="somatic-hail-cluster"
export REGION="us-central1"
export MANAGER_MACHINE_TYPE="n2-standard-4"
export MANAGER_BOOT_DISK_SIZE="100"
export WORKER_MACHINE_TYPE="n2-standard-8"
export WORKER_BOOT_DISK_SIZE="100"
export NUM_WORKERS="2"
export SECONDARY_WORKER_MACHINE_TYPE="n2-standard-4"
export SECONDARY_WORKER_BOOT_DISK_SIZE="100"
export NUM_SECONDARY_WORKERS="5"
export SECONDARY_WORKER_TYPE="spot"
export IDLE_DELETE_TTL="1800s"

export CONTIGS="chr21"
export AB_MIN="0.1"
export AB_MAX="0.3"

# export OVERWRITE="1"
