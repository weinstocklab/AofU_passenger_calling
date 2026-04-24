# AoU Singleton Extraction Pipeline

This pipeline finds **singleton variants** — genetic variants carried by exactly one person — in the NIH All of Us (AoU) v8 whole-genome sequencing dataset. It runs as a large distributed compute job on Google Cloud and writes the results to a file you can analyze locally.

---

## Background: What Is Actually Happening

When you run this pipeline, here is what happens under the hood:

1. **A compute cluster is spun up on Google Cloud.** The AoU dataset is too large to process on a single machine — it spans hundreds of thousands of genomes. The pipeline uses **Apache Spark** (via a service called **Dataproc**) to split the work across many machines in parallel. Think of it like a temporary supercomputer that gets created on demand, does the work, and then shuts itself down.

2. **Hail reads the VDS.** The genomic data is stored in a format called a **VDS (Variant Dataset)**, which is Hail's compressed, cloud-native format for very large variant call sets. [Hail](https://hail.is) is a Python library built on top of Spark specifically for genomic data.

3. **The pipeline filters down to singletons.** It finds variants with allele count (AC) = 1, meaning only one person in the entire cohort carries that alternate allele. It then applies an **allele balance filter** to exclude likely genotyping errors (explained below).

4. **Results are written to Parquet.** [Parquet](https://parquet.apache.org/) is a compressed columnar file format that can be read quickly with Python (pandas, polars) or R.

---

## Prerequisites

### 1. All of Us Researcher Account

You need an approved AoU Researcher Workbench account. If you do not have one, apply at:
https://www.researchallofus.org/register/

Once approved, you will be given access to a **workspace** — a project environment inside the Verily Workbench that holds your compute resources, files, and billing. The workspace used by this pipeline is called `somatic`. Confirm with the PI that you have been added to it.

### 2. Log In to the Verily Workbench

Go to https://workbench.verily.com and log in with your AoU-linked Google account. You should see the `somatic` workspace listed.

### 3. The Workbench CLI (`wb`)

The `wb` binary in this directory is the **Verily Workbench command-line tool**. It is what the pipeline uses to create and control the Dataproc cluster and to copy files to Google Cloud Storage. It is already included — you do not need to download it.

It requires Java to be installed. Inside the Workbench Jupyter environment, Java is already available.

---

## Where to Run This

> **Important:** You cannot run this pipeline from your laptop. The scripts must be run from a terminal inside the Verily Workbench Jupyter environment. This is because the `wb` tool, the GCS bucket permissions, and the controlled AoU data are all locked to the Workbench environment.

### How to open a terminal on the Workbench

1. Log in at https://workbench.verily.com
2. Open the `somatic` workspace
3. Start a Jupyter notebook environment (there will be a "Launch" or "Jupyter" button in the workspace)
4. Once Jupyter loads in your browser, click **File → New → Terminal**
5. In the terminal, navigate to this repository:
   ```bash
   cd ~/AofU
   ```
6. Authenticate the `wb` CLI (first time only):
   ```bash
   ./wb auth login
   ```
   This opens a browser tab — log in with your AoU Google account. Verify it worked:
   ```bash
   ./wb auth status
   ```

---

## Running the Pipeline

### Step 1: Copy and edit the environment file

Environment variables are how you tell the pipeline where your data lives, where to write output, and how to configure the cluster. They are just named settings you set in the terminal before running the script.

Copy the example file and open it in a text editor:

```bash
cp scripts/example_singletons_ab_env.sh scripts/my_env.sh
nano scripts/my_env.sh   # or use any text editor
```

#### Variables you need to confirm or change

**Workspace and billing:**

| Variable | What it is | Example value |
|---|---|---|
| `WORKSPACE_ID` | The name/ID of your Workbench workspace | `somatic` |
| `WORKSPACE_BUCKET` | Your writable Google Cloud Storage bucket. This is where output and temporary files are stored. Find it in the Workbench workspace page under "Cloud storage". | `gs://working-wb-quick-beet-1004` |
| `REQUESTER_PAYS_PROJECT` | Your GCP **billing project ID**. The AoU dataset bucket charges the requester for data access, so you must specify which project to bill. Find this in the workspace page — it looks like `wb-quick-beet-1004`. | `wb-quick-beet-1004` |

**Input/output paths** (these are derived from `WORKSPACE_BUCKET` and should not need editing):

| Variable | What it is |
|---|---|
| `VDS_URI` | Path to the AoU v8 VDS on GCS. Pre-filled with the confirmed location — do not change. |
| `OUTPUT_PARQUET_URI` | Where the results Parquet file will be written inside your workspace bucket. |
| `TMP_DIR_URI` | Temporary scratch space for Hail/Spark intermediate files inside your workspace bucket. |
| `SCRIPT_GS_URI` | Where the pipeline script is uploaded before being submitted to Dataproc. |

**Cluster size** (these control cost and speed):

| Variable | What it is | Default |
|---|---|---|
| `NUM_WORKERS` | Number of primary worker machines in the cluster | `4` |
| `NUM_SECONDARY_WORKERS` | Number of additional **spot** (cheap, interruptible) workers | `20` |
| `WORKER_MACHINE_TYPE` | Machine type for workers | `n2-standard-8` (8 CPU, 32 GB RAM each) |
| `IDLE_DELETE_TTL` | How long the cluster stays alive when idle before auto-deleting | `600s` (10 min) |

**Filtering:**

| Variable | What it is | Default |
|---|---|---|
| `AB_MIN` | Minimum allele balance (see below) | `0.1` |
| `AB_MAX` | Maximum allele balance (see below) | `0.3` |
| `CONTIGS` | Restrict to specific chromosomes for a test run | `chr21` in the example |

### Step 2: Run a pilot on one chromosome first

Before launching the full genome-wide job (which costs real money and takes hours), always test on a single chromosome first. The example file already sets `CONTIGS=chr21` for this reason.

```bash
source scripts/my_env.sh
```

That one command sets all the variables and submits the job. The script will:

1. Authenticate with the Workbench and set the active workspace
2. Create a Dataproc cluster in your workspace (or reuse one if it already exists)
3. Upload the pipeline script to your GCS bucket
4. Submit the job to the cluster and stream logs to your terminal
5. Retry automatically if the cluster is still starting up

Expect the pilot (chr21 only) to take roughly **10–20 minutes**. Watch the log output for errors.

### Step 3: Scale to the full genome

Once the pilot finishes and the output looks correct, clear the contig restriction and rerun:

```bash
export CONTIGS=""
bash scripts/run_singletons_ab_wb.sh
```

A full genome-wide run will take several hours depending on cluster size.

---

## The Allele Balance Filter — Why It Exists

A singleton is a variant where exactly one person carries the alternate allele (AC = 1). For heterozygous calls, we expect roughly **half** of the reads at that position to show the reference allele and half to show the alternate allele (allele balance ≈ 0.5). In practice, sequencing is noisy, so we accept a range.

However, if the allele balance is very low (e.g., only 5% of reads are alternate) or very high (e.g., 95%), this is a red flag that the variant call may be a sequencing or mapping artifact rather than a real heterozygous variant. The filter removes these likely errors.

The default range of `AB_MIN=0.1` to `AB_MAX=0.3` is intentionally conservative for singletons — it captures het calls with moderate alt read support while excluding likely artifacts at the extremes.

---

## Output

Results are written as a Parquet file to `OUTPUT_PARQUET_URI`. Each row is one singleton variant passing the allele balance filter.

| Column | Type | Description |
|---|---|---|
| `sample_id` | string | AoU participant ID of the singleton carrier |
| `chrom` | string | Chromosome (e.g., `chr1`) |
| `pos` | int | Position on GRCh38 |
| `ref` | string | Reference allele |
| `alt` | string | Alternate allele |
| `ad_alt` | int | Number of reads supporting the alternate allele |
| `ad_ref` | int | Number of reads supporting the reference allele |
| `dp` | int | Total read depth at this position |
| `is_snp` | bool | `true` if this is a SNP; `false` if it is an indel |

To read the results in Python:
```python
import pandas as pd
df = pd.read_parquet("gs://your-bucket/results/singletons_ab_0p1_0p3.parquet")
```

---

## Cost and Safety Notes

- **Always pilot on one chromosome first.** A full genome-wide run uses ~24 machines and costs meaningful money.
- **The cluster auto-deletes after 10 minutes of idle time.** If something goes wrong, you do not need to manually shut it down — it will clean itself up.
- **Spot workers can be interrupted.** If Google reclaims spot machines mid-job, Spark will retry affected tasks automatically. If too many are reclaimed at once the job may fail; rerun it.
- **Do not copy participant-level data outside the Workbench.** AoU controlled-access data must stay within approved environments. The output Parquet file contains participant IDs and must remain in the workspace GCS bucket.

---

## File Overview

```
scripts/
  singletons_ab_from_vds.py      # Core Hail pipeline — submitted to Dataproc, not run locally
  run_singletons_ab_wb.sh        # Orchestrator: creates cluster, uploads script, submits job
  example_singletons_ab_env.sh   # Template for environment variables — copy this and edit it
wb                               # Verily Workbench CLI binary (pre-installed, requires Java)
pixi.toml                        # Local Python environment for code linting only (not needed to run the pipeline)
```
