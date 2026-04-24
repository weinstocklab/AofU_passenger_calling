#!/usr/bin/env python3
"""Extract singleton variants from an AoU short-read VDS and write Parquet.

Expected runtime environment:
- Dataproc/Spark cluster with Hail installed (e.g., Workbench Dataproc with HAIL framework)
- Input/output paths in cloud storage (gs://...)

Output schema (column order):
1) sample_id
2) chrom
3) pos
4) ref
5) alt
6) ad_alt
7) ad_ref
8) dp
9) is_snp
"""

import argparse
import os
import sys
from datetime import datetime, timezone
from urllib.parse import urlparse

import hail as hl


def log(message: str) -> None:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{timestamp}] {message}", file=sys.stderr, flush=True)


def _pick_entry_field(mt: hl.MatrixTable, candidates: list[str], what: str) -> str:
    fields = set(mt.entry.dtype.fields)
    for name in candidates:
        if name in fields:
            return name
    raise ValueError(
        f"Could not find {what} entry field. Looked for {candidates}; "
        f"available entry fields: {sorted(fields)}"
    )


def _entry_field_names(mt: hl.MatrixTable) -> set[str]:
    return set(mt.entry.dtype.fields)


def env_or_default(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return value


def env_flag(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return value.lower() in {"1", "true", "t", "yes", "y"}


def bucket_from_gs_uri(uri: str) -> str | None:
    if not uri.startswith("gs://"):
        return None
    parsed = urlparse(uri)
    return parsed.netloc or None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compute AC=1 singleton variants from a short-read VDS, filter by "
            "allele balance range, and write result rows to Parquet."
        )
    )
    parser.add_argument(
        "--vds-uri",
        default=env_or_default("VDS_URI"),
        help="Input VDS path (gs://...)",
    )
    parser.add_argument(
        "--output-parquet-uri",
        default=env_or_default("OUTPUT_PARQUET_URI"),
        help="Output parquet dataset path (gs://...)",
    )
    parser.add_argument(
        "--tmp-dir",
        default=env_or_default("TMP_DIR_URI"),
        help="Temporary directory for Hail/Spark (gs://...)",
    )
    parser.add_argument(
        "--ab-min",
        type=float,
        default=float(env_or_default("AB_MIN", "0.1")),
        help="Minimum allele balance inclusive (default: 0.1)",
    )
    parser.add_argument(
        "--ab-max",
        type=float,
        default=float(env_or_default("AB_MAX", "0.3")),
        help="Maximum allele balance inclusive (default: 0.3)",
    )
    parser.add_argument(
        "--log-uri",
        default=env_or_default("LOG_URI"),
        help="Optional Hail log path (local path or gs://...)",
    )
    parser.add_argument(
        "--reference-genome",
        default=env_or_default("REFERENCE_GENOME", "GRCh38"),
        help="Reference genome (default: GRCh38)",
    )
    parser.add_argument(
        "--requester-pays-project",
        default=env_or_default("REQUESTER_PAYS_PROJECT"),
        help="Billing project for requester-pays GCS buckets",
    )
    parser.add_argument(
        "--requester-pays-buckets",
        default=env_or_default("REQUESTER_PAYS_BUCKETS"),
        help="Optional comma-separated requester-pays bucket allowlist",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=env_flag("OVERWRITE", False),
        help="Overwrite output parquet path if it exists",
    )
    parser.add_argument(
        "--contigs",
        default=env_or_default("CONTIGS"),
        help=(
            "Optional comma-separated contig list for a pilot run "
            '(example: "chr20" or "20"). Default keeps autosomes only.'
        ),
    )
    parser.add_argument(
        "--split-multi",
        dest="split_multi",
        action="store_true",
        default=env_flag("SPLIT_MULTI", True),
        help="Split multi-allelic sites before singleton calculation (default)",
    )
    parser.add_argument(
        "--no-split-multi",
        dest="split_multi",
        action="store_false",
        help="Do not split multi-allelic sites",
    )
    args = parser.parse_args()
    if not args.vds_uri:
        parser.error("--vds-uri is required or set VDS_URI")
    if not args.output_parquet_uri:
        parser.error("--output-parquet-uri is required or set OUTPUT_PARQUET_URI")
    if not args.tmp_dir:
        parser.error("--tmp-dir is required or set TMP_DIR_URI")
    if not (0.0 <= args.ab_min <= 1.0 and 0.0 <= args.ab_max <= 1.0):
        parser.error("--ab-min and --ab-max must be between 0 and 1")
    if args.ab_min > args.ab_max:
        parser.error("--ab-min cannot be greater than --ab-max")
    return args


def main() -> None:
    args = parse_args()
    requester_pays_config: str | tuple[str, list[str]] | None = None
    if args.requester_pays_project:
        if args.requester_pays_buckets:
            rp_buckets = [b.strip() for b in args.requester_pays_buckets.split(",") if b.strip()]
        else:
            inferred_bucket = bucket_from_gs_uri(args.vds_uri)
            rp_buckets = [inferred_bucket] if inferred_bucket else []
        requester_pays_config = (
            (args.requester_pays_project, rp_buckets)
            if rp_buckets
            else args.requester_pays_project
        )

    log(
        "Starting singleton extraction with "
        f"vds_uri={args.vds_uri}, output={args.output_parquet_uri}, "
        f"tmp_dir={args.tmp_dir}, ab_min={args.ab_min}, ab_max={args.ab_max}, "
        f"contigs={args.contigs or 'autosomes'}, split_multi={args.split_multi}, "
        f"overwrite={args.overwrite}, requester_pays_project={args.requester_pays_project}, "
        f"requester_pays_buckets={args.requester_pays_buckets or bucket_from_gs_uri(args.vds_uri)}"
    )

    hl.init(
        default_reference=args.reference_genome,
        tmp_dir=args.tmp_dir,
        log=args.log_uri,
        gcs_requester_pays_configuration=requester_pays_config,
    )

    try:
        log("Reading VDS")
        vds = hl.vds.read_vds(args.vds_uri)
        if args.split_multi:
            if not hasattr(hl.vds, "split_multi"):
                raise RuntimeError(
                    "This Hail build does not provide hl.vds.split_multi; rerun "
                    "with --no-split-multi or upgrade Hail."
                )
            log("Splitting multi-allelic variant rows")
            vds = hl.vds.split_multi(vds)

        mt = vds.variant_data
        log(f"Variant data entry fields: {sorted(mt.entry.dtype.fields)}")

        entry_fields = _entry_field_names(mt)
        gt_field = _pick_entry_field(mt, ["LGT", "GT"], "genotype")
        ad_field = _pick_entry_field(mt, ["LAD", "AD"], "allelic depth")
        log(f"Using entry fields gt={gt_field}, ad={ad_field}")

        gt = mt[gt_field]
        ad = mt[ad_field]

        if "DP" in entry_fields:
            dp = hl.int32(mt["DP"])
            dp_source = "DP"
        else:
            dp = hl.or_missing(
                hl.is_defined(ad),
                hl.int32(hl.sum(ad.map(lambda x: hl.int32(x)))),
            )
            dp_source = f"sum({ad_field})"
        log(f"Using depth source {dp_source}")

        if args.contigs:
            contigs = [c.strip() for c in args.contigs.split(",") if c.strip()]
            if not contigs:
                raise ValueError("--contigs was provided but no contig names were parsed")
            log(f"Filtering to pilot contigs: {contigs}")
            mt = mt.filter_rows(hl.literal(set(contigs)).contains(mt.locus.contig))
        else:
            # Restrict to canonical autosomes to avoid sex chromosomes, alt contigs,
            # decoys, and other non-autosomal reference sequences.
            log("Filtering to canonical autosomes only")
            mt = mt.filter_rows(mt.locus.in_autosome())
        log(f"Row count after contig/autosome filter: {mt.count_rows()}")

        # Keep only bi-allelic rows. If rows were split above this is effectively
        # a safety check; if not, it excludes unsplit multi-allelic sites.
        mt = mt.filter_rows(hl.len(mt.alleles) == 2)
        log(f"Row count after bi-allelic filter: {mt.count_rows()}")

        # AC over all alternate alleles in the current (optionally split) row.
        mt = mt.annotate_rows(ac=hl.agg.sum(hl.or_else(gt.n_alt_alleles(), 0)))
        mt = mt.filter_rows(mt.ac == 1)
        log(f"Row count after AC==1 singleton filter: {mt.count_rows()}")

        ad_ref = hl.or_missing(
            hl.is_defined(ad) & (hl.len(ad) > 0),
            hl.int32(ad[0]),
        )
        ad_alt = hl.or_missing(
            hl.is_defined(ad) & (hl.len(ad) > 1),
            hl.int32(ad[1]),
        )
        denom = ad_ref + ad_alt
        ab = hl.or_missing(
            hl.is_defined(denom) & (denom > 0),
            hl.float64(ad_alt) / hl.float64(denom),
        )

        carrier_pass = (
            hl.is_defined(gt)
            & (gt.n_alt_alleles() > 0)
            & hl.is_defined(ab)
            & (ab >= args.ab_min)
            & (ab <= args.ab_max)
        )

        # Keep up to 2 records to guard against unexpected >1 matching entries.
        mt = mt.annotate_rows(
            carriers=hl.agg.filter(
                carrier_pass,
                hl.agg.take(
                    hl.struct(
                        sample_id=mt.s,
                        ad_alt=ad_alt,
                        ad_ref=ad_ref,
                        dp=dp,
                    ),
                    2,
                ),
            )
        )
        mt = mt.filter_rows(hl.len(mt.carriers) == 1)
        log(f"Row count after carrier AB filter: {mt.count_rows()}")

        ht = mt.rows()
        ht = ht.key_by()
        ht = ht.select(
            sample_id=ht.carriers[0].sample_id,
            chrom=ht.locus.contig,
            pos=ht.locus.position,
            ref=ht.alleles[0],
            alt=ht.alleles[1],
            ad_alt=ht.carriers[0].ad_alt,
            ad_ref=ht.carriers[0].ad_ref,
            dp=ht.carriers[0].dp,
            is_snp=hl.is_snp(ht.alleles[0], ht.alleles[1]),
        )
        log("Converting Hail table to Spark DataFrame")

        df = ht.to_spark()
        write_mode = "overwrite" if args.overwrite else "errorifexists"
        log(f"Writing parquet to {args.output_parquet_uri} with mode={write_mode}")
        df.write.mode(write_mode).parquet(args.output_parquet_uri)
        log("Finished writing parquet output")
    finally:
        log("Stopping Hail")
        hl.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise
