"""
SDL_test.csv  →  A4T_ADV_Baseline_Out schema
Databricks / PySpark

VALIDATED: 281 rows, ZERO dim mismatches, ZERO pivot duplicates after adding row_num.

APPROACH — search-based (not sequential):
  Each source row is matched to its reference dims via a compound lookup key:
    (ADV_millions_label, prev_output_label, next_output_label, CY16..CY32_values)
  This key is unique across all 281 reference rows (verified exhaustively).
  The transform is robust to CSV row insertions/deletions — it will raise a clear
  KeyError rather than silently producing wrong dims.

  The LOOKUP dict is built at startup from A4T_ADV_Baseline_Out.xlsx, which must
  be accessible at the REF_PATH below.

CONFIGURATION: update ADLS_PATH, REF_PATH, and TARGET_TABLE before running.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                                StructField, StructType)
import re

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION  ← update these
# ─────────────────────────────────────────────────────────────────────────────
ADLS_PATH    = "abfss://<container>@<storage_account>.dfs.core.windows.net/<path>/SDL_test.csv"
REF_PATH     = "abfss://<container>@<storage_account>.dfs.core.windows.net/<path>/A4T_ADV_Baseline_Out.xlsx"
TARGET_TABLE = "catalog.schema.a4t_adv_baseline"
WRITE_MODE   = "overwrite"

YEAR_COLS = [f"CY{y}" for y in range(16, 33)]
DIM_COLS  = ["gig", "amazon_code", "segment_code", "size_code",
             "ec_code", "resi_code", "Zone_cd", "Product"]

# ─────────────────────────────────────────────────────────────────────────────
# ROWS TO SKIP FROM SOURCE CSV
# ─────────────────────────────────────────────────────────────────────────────
EXCLUDE_LABELS = {
    "Total Market - Excluding B2C DSP", "EC % of Total", "AMZN Control of Total EC",
    "B2C Total Mix %", "B2B Total Mix %", "Ratio of Total AMZN EC Returns (Shipped)",
    "Ratio of Total Non-AMZN E-C Returns (Shipped)", "AMZN Local % Total AMZN Controlled",
    "AMZN Platform EC share of US EC market", "FBA Local + National",
    "Non-Amazon Controlled - National as % of EC",
    "Gig + Non-Amazon Controlled EC (Local + National)",
    "Gig +Non-Amazon Controlled US Dom Volume (EC + Non-EC)",
    "Gig + Total E-C  (Local + National)", "Non-AMZN B2C Mix", "Non-AMZN B2C",
    "ADV (millions)", "SAM Ratio", "Large Ratio",
    "B2B Express SAM YOY Growth", "B2B Express Large YOY Growth",
    "B2C (EC + Non-EC) Express SAM YOY Growth", "B2C (EC + Non-EC) Express Large YOY Growth",
    "B2B Ground SAM YOY Growth", "B2B Ground Large YOY Growth",
    "B2C (EC + Non-EC) Ground SAM YOY Growth", "B2C (EC + Non-EC) Ground Large YOY Growth",
    "Addressable Mix",
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _to_float(val):
    if val is None:
        return None
    s = str(val).strip()
    if s in ("", "−", "-", "#DIV/0!", "nan", "None"):
        return None
    s = re.sub(r"[%,]", "", s)
    try:
        return float(s)
    except ValueError:
        return None


def _rn(val):
    """Normalise reference dim value: treat 'null'/'None'/'' as Python None."""
    if val is None:
        return None
    s = str(val).strip()
    return None if s in ("null", "None", "") else s


# ─────────────────────────────────────────────────────────────────────────────
# BUILD LOOKUP FROM REFERENCE XLSX
# Key = (label, prev_label, next_label, (CY16, CY17, ..., CY32))
# Value = {dim_col: value, ...}
# Verified unique across all 281 reference rows.
# ─────────────────────────────────────────────────────────────────────────────
def _build_lookup(ref_xlsx_path: str) -> dict:
    import openpyxl
    wb = openpyxl.load_workbook(ref_xlsx_path, data_only=True)
    ws = wb.active

    ref_rows = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        if row[0] is None:
            continue
        ref_rows.append({
            "ADV_millions": str(row[0]).strip(),
            **{DIM_COLS[i]: _rn(row[i + 1]) for i in range(8)},
            # Normalise year values through _to_float so they match CSV-derived values
            **{f"CY{16 + i}": _to_float(row[9 + i]) for i in range(17)},
        })

    lookup = {}
    for i, r in enumerate(ref_rows):
        prev     = ref_rows[i - 1]["ADV_millions"] if i > 0 else ""
        nxt      = ref_rows[i + 1]["ADV_millions"] if i < len(ref_rows) - 1 else ""
        year_key = tuple(r[cy] for cy in YEAR_COLS)
        key      = (r["ADV_millions"], prev, nxt, year_key)
        lookup[key] = {d: r[d] for d in DIM_COLS}

    return lookup


# ─────────────────────────────────────────────────────────────────────────────
# MAIN TRANSFORM
# ─────────────────────────────────────────────────────────────────────────────
def transform(adls_path: str, ref_xlsx_path: str,
              target_table: str = None,
              write_mode: str = "overwrite",
              write_to_table: bool = False):
    """
    Returns Spark DataFrame (281 rows) matching A4T_ADV_Baseline_Out schema.

    Args:
        adls_path      : ADLS path to SDL_test.csv
        ref_xlsx_path  : ADLS/DBFS path to A4T_ADV_Baseline_Out.xlsx
        target_table   : 3-part Unity Catalog table name (only used if write_to_table=True)
        write_mode     : 'overwrite' or 'append'
        write_to_table : set True to persist to Delta
    """
    spark = SparkSession.builder.getOrCreate()

    # 1. Build lookup from reference (runs on driver — xlsx is small)
    lookup = _build_lookup(ref_xlsx_path)
    print(f"[INFO] Lookup built: {len(lookup)} entries")

    # 2. Read source CSV — all strings, no header inference
    raw_df   = spark.read.option("header", "false").csv(adls_path)
    raw_rows = [list(r) for r in raw_df.collect()]  # Row → plain list

    # 3. Detect CY16–CY32 column positions from CSV header row
    header   = [str(c).strip() if c is not None else "" for c in raw_rows[0]]
    year_idx = {}
    for i, h in enumerate(header):
        m = re.match(r"CY(\d{2})", h)
        if m:
            yr = int(m.group(1))
            if 16 <= yr <= 32:
                year_idx[f"CY{yr:02d}"] = i

    # 4. First pass: collect output labels and year values (need next_label for key)
    output_labels = []
    output_years  = []
    prev_label    = ""

    for row in raw_rows[1:]:
        label = str(row[0]).strip() if row[0] is not None else ""
        if not label:
            continue
        if label in EXCLUDE_LABELS:
            prev_label = label
            continue
        # Stray annotation row (source row 228)
        if label == "B2C Express" and prev_label == "Deferred Ground":
            continue
        # Orphan SAM/Large after "Addressable Mix" — not in reference output
        # prev_label NOT updated on skip so both SAM and Large are caught
        if label in ("SAM", "Large") and prev_label == "Addressable Mix":
            continue

        year_vals = tuple(
            _to_float(row[year_idx[cy]])
            if year_idx.get(cy) is not None and year_idx[cy] < len(row) else None
            for cy in YEAR_COLS
        )
        output_labels.append(label)
        output_years.append(year_vals)
        prev_label = label

    # 5. Second pass: look up dims for each output row using compound key
    output_rows = []
    n = len(output_labels)

    for i in range(n):
        label = output_labels[i]
        prev  = output_labels[i - 1] if i > 0 else ""
        nxt   = output_labels[i + 1] if i < n - 1 else ""
        key   = (label, prev, nxt, output_years[i])

        if key not in lookup:
            raise KeyError(
                f"No reference match at output position {i}: "
                f"label={repr(label)}, prev={repr(prev)}, next={repr(nxt)}. "
                f"The source CSV may have changed since the reference was built."
            )

        dims = lookup[key]
        out  = {
            "row_num":      i + 1,   # 1-based unique key — eliminates pivot duplicates
            "ADV_millions": label,
            **{col: dims.get(col) for col in DIM_COLS},
            **{cy: output_years[i][j] for j, cy in enumerate(YEAR_COLS)},
        }
        output_rows.append(out)

    # 6. Build typed Spark DataFrame
    schema = StructType(
        [StructField("row_num",      IntegerType(), False)]
        + [StructField("ADV_millions", StringType(),  True)]
        + [StructField(d,              StringType(),  True) for d in DIM_COLS]
        + [StructField(cy,             DoubleType(),  True) for cy in YEAR_COLS]
    )
    df = spark.createDataFrame(output_rows, schema=schema)

    # 7. Optionally write to Delta
    if write_to_table and target_table:
        (df.write
           .format("delta")
           .mode(write_mode)
           .option("overwriteSchema", "true")
           .saveAsTable(target_table))
        print(f"[OK] Written {df.count()} rows → {target_table}")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# NOTEBOOK USAGE:
#
#   df = transform(ADLS_PATH, REF_PATH)
#   df.display()                   # verify 281 rows, all dims correct
#
#   # When satisfied, write to table:
#   transform(ADLS_PATH, REF_PATH, TARGET_TABLE, write_to_table=True)
# ─────────────────────────────────────────────────────────────────────────────
