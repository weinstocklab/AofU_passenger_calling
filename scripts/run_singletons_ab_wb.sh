#!/usr/bin/env bash
set -euo pipefail

# Template runner for Workbench + Dataproc/Hail.
# Assumes:
# - wb is installed/authenticated
# - You have permission to create/start cluster resources in the workspace
#
# Required environment variables:
#   WORKSPACE_ID          Workbench workspace ID (somatic workspace)
#   VDS_URI               gs:// path to short-read VDS
#   OUTPUT_PARQUET_URI    gs:// output parquet dataset path
#   TMP_DIR_URI           gs:// temp path for Hail/Spark
#   SCRIPT_GS_URI         gs:// path to upload the pyspark script
#
# Optional environment variables:
#   WB_BIN                wb binary path (default: wb)
#   CLUSTER_RESOURCE_ID   resource ID in workspace (default: somatic_hail_cluster)
#   CLUSTER_ID            Dataproc cluster name (default: somatic-hail-cluster)
#   REGION                GCP region (default: us-central1)
#   MANAGER_MACHINE_TYPE  default: n2-standard-4
#   WORKER_MACHINE_TYPE   default: n2-standard-8
#   NUM_WORKERS           default: 4
#   NUM_SECONDARY_WORKERS default: 20
#   SECONDARY_WORKER_TYPE default: spot
#   AB_MIN                default: 0.1
#   AB_MAX                default: 0.3
#   IDLE_DELETE_TTL       Dataproc idle auto-delete TTL (default: 600s)
#   REQUESTER_PAYS_PROJECT billing project for requester-pays GCS buckets
#   REQUESTER_PAYS_BUCKETS optional comma-separated requester-pays bucket list
#   CONTIGS               optional comma-separated contig list for pilot runs
#   OVERWRITE             set to 1 to pass --overwrite to the Hail job
#   STARTUP_RETRIES       submission retries while the cluster becomes ready (default: 20)
#   STARTUP_SLEEP_SECONDS sleep between retries (default: 30)

WB_BIN="${WB_BIN:-wb}"
WORKSPACE_ID="${WORKSPACE_ID:?Set WORKSPACE_ID}"
VDS_URI="${VDS_URI:?Set VDS_URI}"
OUTPUT_PARQUET_URI="${OUTPUT_PARQUET_URI:?Set OUTPUT_PARQUET_URI}"
TMP_DIR_URI="${TMP_DIR_URI:?Set TMP_DIR_URI}"
SCRIPT_GS_URI="${SCRIPT_GS_URI:?Set SCRIPT_GS_URI}"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
TMP_JOB_DIR="$(mktemp -d)"

CLUSTER_RESOURCE_ID="${CLUSTER_RESOURCE_ID:-somatic_hail_cluster}"
CLUSTER_ID="${CLUSTER_ID:-somatic-hail-cluster}"
REGION="${REGION:-us-central1}"
MANAGER_MACHINE_TYPE="${MANAGER_MACHINE_TYPE:-n2-standard-4}"
WORKER_MACHINE_TYPE="${WORKER_MACHINE_TYPE:-n2-standard-8}"
NUM_WORKERS="${NUM_WORKERS:-4}"
NUM_SECONDARY_WORKERS="${NUM_SECONDARY_WORKERS:-20}"
SECONDARY_WORKER_TYPE="${SECONDARY_WORKER_TYPE:-spot}"
AB_MIN="${AB_MIN:-0.1}"
AB_MAX="${AB_MAX:-0.3}"
IDLE_DELETE_TTL="${IDLE_DELETE_TTL:-600s}"
REQUESTER_PAYS_PROJECT="${REQUESTER_PAYS_PROJECT:-}"
REQUESTER_PAYS_BUCKETS="${REQUESTER_PAYS_BUCKETS:-}"
CONTIGS="${CONTIGS:-}"
OVERWRITE="${OVERWRITE:-}"
STARTUP_RETRIES="${STARTUP_RETRIES:-20}"
STARTUP_SLEEP_SECONDS="${STARTUP_SLEEP_SECONDS:-30}"

LOCAL_SCRIPT="${SCRIPT_DIR}/singletons_ab_from_vds.py"
LOCAL_WRAPPER_SCRIPT="${TMP_JOB_DIR}/singletons_ab_from_vds_job.py"
SUBMIT_LOG="${TMP_JOB_DIR}/submit.log"

cleanup() {
  rm -rf "${TMP_JOB_DIR}"
}

trap cleanup EXIT

log() {
  printf '[%(%Y-%m-%dT%H:%M:%SZ)T] %s\n' -1 "$*" >&2
}

extract_cluster_details() {
  local resource_json="$1"
  python3 -c '
import json
import sys

data = json.loads(sys.argv[1])
cluster = None
region = None

def walk(value):
    global cluster, region
    if isinstance(value, dict):
        for key, item in value.items():
            lower = key.lower()
            if cluster is None and lower in {"clusterid", "clustername"} and isinstance(item, str):
                cluster = item
            if region is None and lower == "region" and isinstance(item, str):
                region = item
            walk(item)
    elif isinstance(value, list):
        for item in value:
            walk(item)

walk(data)
print(cluster or "")
print(region or "")
' "${resource_json}"
}

extract_cluster_status() {
  local resource_json="$1"
  python3 -c '
import json
import sys

data = json.loads(sys.argv[1])
print(data.get("status", ""))
' "${resource_json}"
}

extract_hail_wheel() {
  local resource_json="$1"
  python3 -c '
import json
import sys

data = json.loads(sys.argv[1])
metadata = data.get("metadata", {})
print(metadata.get("WHEEL", ""))
' "${resource_json}"
}

submit_job() {
  log "Submitting Dataproc job to cluster=${ACTUAL_CLUSTER_ID} region=${ACTUAL_REGION}"
  local submit_cmd=(
    "${WB_BIN}" gcloud dataproc jobs submit pyspark "${SCRIPT_GS_URI}"
    --cluster="${ACTUAL_CLUSTER_ID}"
    --region="${ACTUAL_REGION}"
  )

  if [[ -n "${REQUESTER_PAYS_PROJECT}" ]]; then
    local rp_buckets="${REQUESTER_PAYS_BUCKETS}"
    if [[ -z "${rp_buckets}" ]]; then
      rp_buckets="$(python3 -c 'from urllib.parse import urlparse; import sys; print(urlparse(sys.argv[1]).netloc)' "${VDS_URI}")"
    fi
    local submit_properties="^#^spark.hadoop.fs.gs.requester.pays.mode=CUSTOM"
    submit_properties="${submit_properties}#spark.hadoop.fs.gs.requester.pays.project.id=${REQUESTER_PAYS_PROJECT}"
    submit_properties="${submit_properties}#spark.hadoop.fs.gs.requester.pays.buckets=${rp_buckets}"
    log "Submitting with requester-pays Spark properties for buckets=${rp_buckets} billed_to=${REQUESTER_PAYS_PROJECT}"
    submit_cmd+=(--properties="${submit_properties}")
  fi

  if [[ -n "${HAIL_WHEEL_URI}" ]]; then
    log "Submitting with Hail wheel ${HAIL_WHEEL_URI}"
    submit_cmd+=(--py-files="${HAIL_WHEEL_URI}")
  fi

  "${submit_cmd[@]}" 2>&1 | tee "${SUBMIT_LOG}"
  return "${PIPESTATUS[0]}"
}

log "Configuring workspace context for ${WORKSPACE_ID}"
"${WB_BIN}" workspace set --id="${WORKSPACE_ID}"

if ! "${WB_BIN}" resource describe --id="${CLUSTER_RESOURCE_ID}" >/dev/null 2>&1; then
  log "Creating Dataproc cluster resource ${CLUSTER_RESOURCE_ID}"
  "${WB_BIN}" resource create dataproc-cluster \
    --id="${CLUSTER_RESOURCE_ID}" \
    --cluster-id="${CLUSTER_ID}" \
    --software-framework=HAIL \
    --region="${REGION}" \
    --idle-delete-ttl="${IDLE_DELETE_TTL}" \
    --manager-machine-type="${MANAGER_MACHINE_TYPE}" \
    --worker-machine-type="${WORKER_MACHINE_TYPE}" \
    --num-workers="${NUM_WORKERS}" \
    --num-secondary-workers="${NUM_SECONDARY_WORKERS}" \
    --secondary-worker-type="${SECONDARY_WORKER_TYPE}" \
    --quiet
fi

log "Resolving cluster metadata from Workbench resource ${CLUSTER_RESOURCE_ID}"
RESOURCE_JSON="$("${WB_BIN}" resource describe --id="${CLUSTER_RESOURCE_ID}" --format=JSON)"
RESOURCE_STATUS="$(extract_cluster_status "${RESOURCE_JSON}")"

if [[ "${RESOURCE_STATUS}" == "RUNNING" ]]; then
  log "Cluster resource ${CLUSTER_RESOURCE_ID} is already RUNNING"
else
  log "Starting cluster resource ${CLUSTER_RESOURCE_ID}"
  if ! "${WB_BIN}" cluster start --id="${CLUSTER_RESOURCE_ID}"; then
    log "wb cluster start returned an error; re-checking cluster status"
    RESOURCE_JSON="$("${WB_BIN}" resource describe --id="${CLUSTER_RESOURCE_ID}" --format=JSON)"
    RESOURCE_STATUS="$(extract_cluster_status "${RESOURCE_JSON}")"
    if [[ "${RESOURCE_STATUS}" != "RUNNING" ]]; then
      log "Cluster resource status after failed start is ${RESOURCE_STATUS:-UNKNOWN}"
      exit 1
    fi
    log "Cluster resource became RUNNING despite start error; continuing"
  else
    RESOURCE_JSON="$("${WB_BIN}" resource describe --id="${CLUSTER_RESOURCE_ID}" --format=JSON)"
    RESOURCE_STATUS="$(extract_cluster_status "${RESOURCE_JSON}")"
    log "Cluster resource status after start: ${RESOURCE_STATUS:-UNKNOWN}"
  fi
fi

readarray -t CLUSTER_DETAILS < <(extract_cluster_details "${RESOURCE_JSON}")
HAIL_WHEEL_URI="$(extract_hail_wheel "${RESOURCE_JSON}")"

ACTUAL_CLUSTER_ID="${CLUSTER_DETAILS[0]:-${CLUSTER_ID}}"
ACTUAL_REGION="${CLUSTER_DETAILS[1]:-${REGION}}"

if [[ -z "${ACTUAL_CLUSTER_ID}" || -z "${ACTUAL_REGION}" ]]; then
  echo "Unable to resolve cluster ID and region from Workbench resource metadata." >&2
  exit 1
fi

if [[ "${ACTUAL_CLUSTER_ID}" != "${CLUSTER_ID}" || "${ACTUAL_REGION}" != "${REGION}" ]]; then
  log "Using cluster settings from existing resource: cluster_id=${ACTUAL_CLUSTER_ID} region=${ACTUAL_REGION}"
fi

cat > "${LOCAL_WRAPPER_SCRIPT}" <<EOF
#!/usr/bin/env python3
import hashlib
import os
import pathlib
import sys
import zipfile


def _materialize_hail_wheel() -> None:
    for entry in list(sys.path):
        if not entry.endswith(".whl"):
            continue
        wheel_name = os.path.basename(entry)
        if not wheel_name.startswith("hail-"):
            continue
        target_root = pathlib.Path("/tmp") / (
            "hail-wheel-" + hashlib.md5(entry.encode("utf-8"), usedforsecurity=False).hexdigest()
        )
        if not target_root.exists():
            with zipfile.ZipFile(entry) as zf:
                zf.extractall(target_root)
        sys.path.insert(0, str(target_root))
        break


_materialize_hail_wheel()

sys.argv = [
    "singletons_ab_from_vds.py",
    "--vds-uri", ${VDS_URI@Q},
    "--output-parquet-uri", ${OUTPUT_PARQUET_URI@Q},
    "--tmp-dir", ${TMP_DIR_URI@Q},
    "--ab-min", ${AB_MIN@Q},
    "--ab-max", ${AB_MAX@Q},
]
EOF

if [[ -n "${REQUESTER_PAYS_PROJECT}" ]]; then
  printf 'sys.argv.extend(["--requester-pays-project", %s])\n' "$(python3 -c 'import json, sys; print(json.dumps(sys.argv[1]))' "${REQUESTER_PAYS_PROJECT}")" >> "${LOCAL_WRAPPER_SCRIPT}"
fi

if [[ -n "${REQUESTER_PAYS_BUCKETS}" ]]; then
  printf 'sys.argv.extend(["--requester-pays-buckets", %s])\n' "$(python3 -c 'import json, sys; print(json.dumps(sys.argv[1]))' "${REQUESTER_PAYS_BUCKETS}")" >> "${LOCAL_WRAPPER_SCRIPT}"
fi

if [[ -n "${CONTIGS}" ]]; then
  printf 'sys.argv.extend(["--contigs", %s])\n' "$(python3 -c 'import json, sys; print(json.dumps(sys.argv[1]))' "${CONTIGS}")" >> "${LOCAL_WRAPPER_SCRIPT}"
fi

if [[ -n "${OVERWRITE}" ]]; then
  printf 'sys.argv.append("--overwrite")\n' >> "${LOCAL_WRAPPER_SCRIPT}"
fi

cat "${LOCAL_SCRIPT}" >> "${LOCAL_WRAPPER_SCRIPT}"

log "Uploading job wrapper ${LOCAL_WRAPPER_SCRIPT} to ${SCRIPT_GS_URI}"
"${WB_BIN}" gsutil cp "${LOCAL_WRAPPER_SCRIPT}" "${SCRIPT_GS_URI}"

attempt=1
until submit_job; do
  if (( attempt >= STARTUP_RETRIES )); then
    log "Dataproc job submission failed after ${attempt} attempts"
    exit 1
  fi
  if grep -Eq 'NameError:|Traceback \(most recent call last\)|error: --|Job \[[^]]+\] failed with error:' "${SUBMIT_LOG}"; then
    log "Dataproc job failed after submission; not retrying a deterministic job error"
    exit 1
  fi
  log "Cluster may still be starting; retrying in ${STARTUP_SLEEP_SECONDS}s (attempt ${attempt}/${STARTUP_RETRIES})"
  sleep "${STARTUP_SLEEP_SECONDS}"
  attempt=$((attempt + 1))
done

log "Dataproc job submission completed"
