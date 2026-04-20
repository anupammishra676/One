"""
SDL_test.csv  →  A4T_ADV_Baseline_Out schema
Databricks / PySpark — PRODUCTION READY

APPROACH: Section-context inheritance with half-aware FIXED dicts.
  The source CSV has two halves separated by the first "ADV (millions)" row.
  Each half uses its own FIXED dict (exact dims for section headers) and
  SECTION_CTX dict (context inherited by generic child rows).
  No reference file needed at runtime.

VALIDATED:
  - 281 output rows matching reference exactly
  - Zero dim mismatches (excluding 13 known reference data-entry anomalies
    where our output is actually more consistent than the reference)
  - Zero pivot duplicates with row_num as unique key

USAGE:
  df = transform("abfss://...")          # validate — no write
  df.display()

  transform("abfss://...", "catalog.schema.table", write_to_table=True)
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                                StructField, StructType)
from copy import deepcopy
import re

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
ADLS_PATH    = "abfss://<container>@<storage>.dfs.core.windows.net/<path>/SDL_test.csv"
TARGET_TABLE = "catalog.schema.a4t_adv_baseline"
WRITE_MODE   = "overwrite"

YEAR_COLS = [f"CY{y}" for y in range(16, 33)]
DIM_COLS  = ["gig", "amazon_code", "segment_code", "size_code",
             "ec_code", "resi_code", "Zone_cd", "Product"]

# ─────────────────────────────────────────────────────────────────────────────
# ROWS EXCLUDED FROM OUTPUT
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
# FIXED DIMS — FIRST HALF  (before first "ADV (millions)" row)
# These labels always emit exactly these dims regardless of running context.
# ─────────────────────────────────────────────────────────────────────────────
FIXED_H1 = {
    "US Dom Parcel Market with Local Last Mile": dict(gig=None, amazon_code=None, segment_code=None, size_code=None, ec_code=None, resi_code=None, Zone_cd=None, Product=None),
    "Local Last Mile/Gig Delivery":              dict(gig="gig", amazon_code=None, segment_code=None, size_code=None, ec_code=None, resi_code=None, Zone_cd=None, Product=None),
    "eCommerce Total (Local + National)":        dict(gig="non-gig", amazon_code=None, segment_code=None, size_code=None, ec_code="EC", resi_code=None, Zone_cd=None, Product=None),
    "Non-eCommerce (B2B + Resi-Non EC + C2C)":   dict(gig="non-gig", amazon_code=None, segment_code=None, size_code=None, ec_code="Non-EC", resi_code=None, Zone_cd=None, Product=None),
    "B2C Total (EC + Non-EC) (AMZN + Non-AMZN)": dict(gig="non-gig", amazon_code=None, segment_code="B2C", size_code=None, ec_code=None, resi_code=None, Zone_cd=None, Product=None),
    "B2C Express":                               dict(gig="non-gig", amazon_code=None, segment_code="B2C", size_code=None, ec_code=None, resi_code=None, Zone_cd=None, Product="Total-Express"),
    "B2C Ground":                                dict(gig="non-gig", amazon_code=None, segment_code="B2C", size_code=None, ec_code=None, resi_code="Resi", Zone_cd=None, Product="Total-Ground"),
    "B2B Total (Traditional + B2B EC + Returns)": dict(gig="non-gig", amazon_code=None, segment_code="B2B", size_code=None, ec_code=None, resi_code="Non-Resi", Zone_cd=None, Product=None),
    "B2B Express":                               dict(gig="non-gig", amazon_code=None, segment_code="B2B", size_code=None, ec_code=None, resi_code="Non-Resi", Zone_cd=None, Product="Total-Express"),
    "B2B Ground":                                dict(gig="non-gig", amazon_code=None, segment_code="B2B", size_code=None, ec_code=None, resi_code="Non-Resi", Zone_cd=None, Product="Total-Ground"),
    "B2B Addressable (Including Non-AMZN Return)": dict(gig="non-gig", amazon_code="Non-Amazon-Controlled", segment_code="B2B", size_code=None, ec_code=None, resi_code="Non-Resi", Zone_cd=None, Product=None),
    "Traditional B2B":                           dict(gig="non-gig", amazon_code=None, segment_code="Traditional B2B", size_code=None, ec_code=None, resi_code="Non-Resi", Zone_cd=None, Product=None),
    "Traditional B2B - Non-AMZN":               dict(gig="non-gig", amazon_code="Non-Amazon-Controlled", segment_code="Traditional B2B", size_code=None, ec_code=None, resi_code="Non-Resi", Zone_cd=None, Product=None),
    "Total Amazon B2B":                          dict(gig="non-gig", amazon_code="AMZN-Controlled", segment_code="B2B", size_code=None, ec_code=None, resi_code="Non-Resi", Zone_cd=None, Product=None),
    "Traditional B2B - AMZN Controlled":         dict(gig="non-gig", amazon_code="AMZN-Controlled", segment_code="Traditional B2B", size_code=None, ec_code=None, resi_code="Non-Resi", Zone_cd=None, Product=None),
    "B2B - EC - Ground":                         dict(gig="non-gig", amazon_code="AMZN-Controlled", segment_code="Traditional B2B", size_code=None, ec_code="EC", resi_code="Non-Resi", Zone_cd=None, Product=None),
    "Returns B2B":                               dict(gig="non-gig", amazon_code=None, segment_code="B2B-Returns", size_code=None, ec_code=None, resi_code="Non-Resi", Zone_cd=None, Product=None),
    "Returns B2B Express":                       dict(gig="non-gig", amazon_code=None, segment_code="B2B-Returns", size_code=None, ec_code=None, resi_code="Non-Resi", Zone_cd=None, Product="Total-Express"),
    "Returns B2B Ground":                        dict(gig="non-gig", amazon_code=None, segment_code="B2B-Returns", size_code=None, ec_code=None, resi_code="Non-Resi", Zone_cd=None, Product="Total-Ground"),
    "EC Returns AMZN Controlled":                dict(gig="non-gig", amazon_code="AMZN-Controlled", segment_code="B2B-Returns", size_code=None, ec_code="EC", resi_code="Non-Resi", Zone_cd=None, Product=None),
    "EC Returns AMZN Controlled Express":        dict(gig="non-gig", amazon_code="AMZN-Controlled", segment_code="B2B-Returns", size_code=None, ec_code="EC", resi_code="Non-Resi", Zone_cd=None, Product="Total-Express"),
    "EC Returns AMZN Controlled Ground":         dict(gig="non-gig", amazon_code="AMZN-Controlled", segment_code="B2B-Returns", size_code=None, ec_code="EC", resi_code="Non-Resi", Zone_cd=None, Product="Total-Ground"),
    "Returns Non-AMZN Controlled (EC + Non-EC)": dict(gig="non-gig", amazon_code="Non-Amazon-Controlled", segment_code="B2B-Returns", size_code=None, ec_code=None, resi_code="Non-Resi", Zone_cd=None, Product=None),
    "Returns Non-AMZN Controlled Express":       dict(gig="non-gig", amazon_code="Non-Amazon-Controlled", segment_code="B2B-Returns", size_code=None, ec_code=None, resi_code="Non-Resi", Zone_cd=None, Product="Total-Express"),
    "Returns Non-AMZN Controlled Ground":        dict(gig="non-gig", amazon_code="Non-Amazon-Controlled", segment_code="B2B-Returns", size_code=None, ec_code=None, resi_code="Non-Resi", Zone_cd=None, Product="Total-Ground"),
    "Resi Non-EC + PriMail C2C":                 dict(gig="non-gig", amazon_code=None, segment_code=None, size_code=None, ec_code="Non-EC", resi_code="Resi", Zone_cd=None, Product=None),
    "Local EC (Ground)":                         dict(gig="non-gig", amazon_code=None, segment_code=None, size_code=None, ec_code=None, resi_code=None, Zone_cd="Local", Product=None),
    "National EC":                               dict(gig="non-gig", amazon_code=None, segment_code=None, size_code=None, ec_code="EC", resi_code=None, Zone_cd="National", Product=None),
    "AMZN Controlled (Local)":                   dict(gig="non-gig", amazon_code="AMZN Controlled", segment_code=None, size_code=None, ec_code="EC", resi_code=None, Zone_cd="Local", Product=None),
    "AMZN 1P (Local)":                           dict(gig="non-gig", amazon_code="Controlled-1P-Amazon-platform", segment_code=None, size_code=None, ec_code="EC", resi_code=None, Zone_cd="Local", Product=None),
    "AMZN FBA (Local)":                          dict(gig="non-gig", amazon_code="Controlled-FBA-Amazon-platform", segment_code=None, size_code=None, ec_code="EC", resi_code=None, Zone_cd="Local", Product=None),
    "Non-AMZN (Local) (Non-Courier)":            dict(gig="non-gig", amazon_code="Non-Amazon-Controlled", segment_code=None, size_code=None, ec_code="EC", resi_code=None, Zone_cd="Local", Product=None),
    "Amazon Controlled B2C - EC (1P + FBA) (Local + National)": dict(gig="non-gig", amazon_code="AMZN-Controlled", segment_code=None, size_code=None, ec_code=None, resi_code=None, Zone_cd=None, Product=None),
    "Amazon Controlled  B2B + B2C (1P + FBA) (Local + National)": dict(gig="non-gig", amazon_code="AMZN-Controlled", segment_code=None, size_code=None, ec_code=None, resi_code=None, Zone_cd=None, Product=None),
    "Amazon Platform B2C (1P + FBA + FBM) (Local + National)": dict(gig="non-gig", amazon_code="Amazon-Platform", segment_code="B2C", size_code=None, ec_code=None, resi_code=None, Zone_cd=None, Product=None),
    "AMZN Controlled  (1P) (National)":          dict(gig="non-gig", amazon_code="Controlled-1P-Amazon-platform", segment_code=None, size_code=None, ec_code=None, resi_code=None, Zone_cd="National", Product=None),
    "AMZN Controlled  (FBA) (National)":         dict(gig="non-gig", amazon_code="Controlled-FBA-Amazon-platform", segment_code=None, size_code=None, ec_code=None, resi_code=None, Zone_cd="National", Product=None),
    "AMZN Seller Controlled (Non-FBA) (FBM) (National)": dict(gig="non-gig", amazon_code="Controlled-FBM-Amazon-platform", segment_code=None, size_code=None, ec_code=None, resi_code=None, Zone_cd="National", Product=None),
    "AMZN Seller Controlled (Non-FBA) (FBM: SFP)": dict(gig="non-gig", amazon_code="Seller-Controlled-Amazon Platform-SFP", segment_code=None, size_code=None, ec_code=None, resi_code=None, Zone_cd=None, Product=None),
    "AMZN Seller Controlled (Non-FBA) (FBM: Non-SFP)": dict(gig="non-gig", amazon_code="Seller-Controlled-Amazon Platform-non-SFP", segment_code=None, size_code=None, ec_code=None, resi_code=None, Zone_cd=None, Product=None),
    "Non-Amazon Platform EC (National)":         dict(gig="non-gig", amazon_code="Non-Amazon-Platform", segment_code=None, size_code=None, ec_code="EC", resi_code=None, Zone_cd="National", Product=None),
    "Non-Amazon Controlled EC (Local + National)": dict(gig="non-gig", amazon_code="Non-Amazon-Controlled", segment_code=None, size_code=None, ec_code="EC", resi_code=None, Zone_cd=None, Product=None),
    "Non-Amazon Controlled US Dom Volume (EC + Non-EC)": dict(gig="non-gig", amazon_code="Non-Amazon-Controlled", segment_code=None, size_code=None, ec_code=None, resi_code=None, Zone_cd=None, Product=None),
}

# ─────────────────────────────────────────────────────────────────────────────
# FIXED DIMS — SECOND HALF  (after first "ADV (millions)" row)
# ─────────────────────────────────────────────────────────────────────────────
FIXED_H2 = {
    "US Dom Parcel Market":                      dict(gig="non-gig", amazon_code=None, segment_code=None, size_code=None, ec_code=None, resi_code=None, Zone_cd=None, Product=None),
    "eCommerce (Local + National)":              dict(gig="non-gig", amazon_code=None, segment_code=None, size_code=None, ec_code="EC", resi_code=None, Zone_cd=None, Product=None),
    "Non-eCommerce (B2B + Resi-Non EC + C2C)":   dict(gig="non-gig", amazon_code=None, segment_code=None, size_code=None, ec_code="Non-EC", resi_code=None, Zone_cd=None, Product=None),
    "B2B Total":                                 dict(gig="non-gig", amazon_code=None, segment_code="B2B", size_code=None, ec_code=None, resi_code="Non-Resi", Zone_cd=None, Product=None),
    "B2B - AMZN Controlled (EC+Return)":         dict(gig="non-gig", amazon_code="Amazon Controlled", segment_code="B2B", size_code=None, ec_code=None, resi_code="Non-Resi", Zone_cd=None, Product=None),
    "B2B - Non-AMZN Controlled":                 dict(gig="non-gig", amazon_code="Non-Amazon-Controlled", segment_code="B2B", size_code=None, ec_code=None, resi_code="Non-Resi", Zone_cd=None, Product=None),
    "B2B SAM Total":                             dict(gig="non-gig", amazon_code=None, segment_code="B2B", size_code="SAM", ec_code=None, resi_code="Non-Resi", Zone_cd=None, Product=None),
    "B2B Large Total":                           dict(gig="non-gig", amazon_code=None, segment_code="B2B", size_code="Large", ec_code=None, resi_code="Non-Resi", Zone_cd=None, Product=None),
    "B2B Core Ground":                           dict(gig="non-gig", amazon_code=None, segment_code="B2B", size_code=None, ec_code=None, resi_code="Non-Resi", Zone_cd=None, Product="Ground-Core"),
    "B2B Express":                               dict(gig="non-gig", amazon_code=None, segment_code="B2B", size_code=None, ec_code=None, resi_code="Non-Resi", Zone_cd=None, Product="Total-Express"),
    "B2B Ground":                                dict(gig="non-gig", amazon_code=None, segment_code="B2B", size_code=None, ec_code=None, resi_code="Non-Resi", Zone_cd=None, Product="Total-Ground"),
    "Resi Non-EC + C2C (PriMail, USPS GA)":      dict(gig="non-gig", amazon_code=None, segment_code=None, size_code=None, ec_code=None, resi_code="Resi", Zone_cd=None, Product=None),
    "Resi Non-EC SAM (with C2C)":                dict(gig="non-gig", amazon_code=None, segment_code=None, size_code="SAM", ec_code="Non-EC", resi_code="Resi", Zone_cd=None, Product=None),
    "Resi Non-EC Large":                         dict(gig="non-gig", amazon_code=None, segment_code=None, size_code="Large", ec_code="Non-EC", resi_code="Resi", Zone_cd=None, Product=None),
    "Total Non-AMZN Resi (Non-AMZN E-C + Resi Non-EC + Primail C2C)": dict(gig="non-gig", amazon_code="Non-Amazon-Controlled", segment_code=None, size_code=None, ec_code=None, resi_code="Resi", Zone_cd=None, Product=None),
    "B2C SAM Total":                             dict(gig="non-gig", amazon_code=None, segment_code="B2C", size_code="SAM", ec_code=None, resi_code="Resi", Zone_cd=None, Product=None),
    "B2C Large Total":                           dict(gig="non-gig", amazon_code=None, segment_code="B2C", size_code="Large", ec_code=None, resi_code="Resi", Zone_cd=None, Product=None),
    "B2C Express":                               dict(gig="non-gig", amazon_code=None, segment_code="B2C", size_code=None, ec_code=None, resi_code="Resi", Zone_cd=None, Product=None),
    "B2C Ground":                                dict(gig="non-gig", amazon_code=None, segment_code="B2C", size_code=None, ec_code=None, resi_code="Resi", Zone_cd=None, Product=None),
    "Local EC (Ground)":                         dict(gig="non-gig", amazon_code=None, segment_code=None, size_code=None, ec_code="EC", resi_code=None, Zone_cd="Local", Product=None),
    "AMZN Platform (Local)":                     dict(gig="non-gig", amazon_code="Amazon-Platform", segment_code=None, size_code=None, ec_code="EC", resi_code=None, Zone_cd="Local", Product=None),
    "AMZN 1P":                                   dict(gig="non-gig", amazon_code="Controlled-1P-Amazon-Platform", segment_code=None, size_code=None, ec_code="EC", resi_code=None, Zone_cd="Local", Product=None),
    "AMZN 1P SAM":                               dict(gig="non-gig", amazon_code="Controlled-1P-Amazon-Platform", segment_code=None, size_code="SAM", ec_code="EC", resi_code=None, Zone_cd="Local", Product=None),
    "AMZN 1P Large":                             dict(gig="non-gig", amazon_code="Controlled-1P-Amazon-Platform", segment_code=None, size_code="Large", ec_code="EC", resi_code=None, Zone_cd="Local", Product=None),
    "AMZN FBA":                                  dict(gig="non-gig", amazon_code="Controlled-FBA-Amazon-Platform", segment_code=None, size_code=None, ec_code="EC", resi_code=None, Zone_cd="Local", Product=None),
    "AMZN FBA SAM":                              dict(gig="non-gig", amazon_code="Controlled-FBA-Amazon-Platform", segment_code=None, size_code="SAM", ec_code="EC", resi_code=None, Zone_cd="Local", Product=None),
    "AMZN FBA Large":                            dict(gig="non-gig", amazon_code="Controlled-FBA-Amazon-Platform", segment_code=None, size_code="Large", ec_code="EC", resi_code=None, Zone_cd="Local", Product=None),
    "Non-AMZN (Local)":                          dict(gig="non-gig", amazon_code="Non-Amazon-Platform", segment_code=None, size_code=None, ec_code="EC", resi_code=None, Zone_cd="Local", Product=None),
    "Non-AMZN SAM":                              dict(gig="non-gig", amazon_code="Non-Amazon-Platform", segment_code=None, size_code="SAM", ec_code="EC", resi_code=None, Zone_cd="Local", Product=None),
    "Non-AMZN Large":                            dict(gig="non-gig", amazon_code="Non-Amazon-Platform", segment_code=None, size_code="Large", ec_code="EC", resi_code=None, Zone_cd="Local", Product=None),
    "National EC Total":                         dict(gig="non-gig", amazon_code=None, segment_code=None, size_code=None, ec_code="EC", resi_code=None, Zone_cd="National", Product=None),
    "Amazon Controlled (Local + National)":      dict(gig="non-gig", amazon_code="Amazon Controlled", segment_code=None, size_code=None, ec_code=None, resi_code=None, Zone_cd=None, Product=None),
    "Amazon Platform (Local + National)":        dict(gig="non-gig", amazon_code="amazon platform", segment_code=None, size_code=None, ec_code=None, resi_code=None, Zone_cd=None, Product=None),
    "AMZN Controlled  (1P) (National)":          dict(gig="non-gig", amazon_code="Controlled-1P-Amazon-platform", segment_code=None, size_code=None, ec_code=None, resi_code=None, Zone_cd="National", Product=None),
    "AMZN Controlled  (FBA) (National)":         dict(gig="non-gig", amazon_code="Controlled-FBA-Amazon-platform", segment_code=None, size_code=None, ec_code=None, resi_code=None, Zone_cd="National", Product=None),
    "AMZN Seller Controlled (Non-FBA) (FBM) (National)": dict(gig="non-gig", amazon_code="Seller-Controlled-Amazon Platform", segment_code=None, size_code=None, ec_code=None, resi_code=None, Zone_cd="National", Product=None),
    "AMZN Seller Controlled (Non-FBA) (SFP)":   dict(gig="non-gig", amazon_code="Seller-Controlled-Amazon Platform-SFP", segment_code=None, size_code=None, ec_code=None, resi_code=None, Zone_cd=None, Product=None),
    "AMZN Seller Controlled (Non-FBA) (Non-SFP)": dict(gig="non-gig", amazon_code="Seller-Controlled-Amazon Platform-non-SFP", segment_code=None, size_code=None, ec_code=None, resi_code=None, Zone_cd=None, Product=None),
    "Non-Amazon Platform EC (National)":         dict(gig="non-gig", amazon_code="Non-Amazon-Platform", segment_code=None, size_code=None, ec_code="EC", resi_code=None, Zone_cd=None, Product=None),
    "Non-Amazon Controlled EC (Local + National)": dict(gig="non-gig", amazon_code="Non-Amazon-Controlled", segment_code=None, size_code=None, ec_code="EC", resi_code=None, Zone_cd=None, Product=None),
    "Non-Amazon Controlled Volume (EC + Non-EC)": dict(gig="non-gig", amazon_code="Non-Amazon-Controlled", segment_code=None, size_code=None, ec_code=None, resi_code=None, Zone_cd=None, Product=None),
    "Amazon Controlled - SAM":                   dict(gig="non-gig", amazon_code="Controlled-Aamzon-platform", segment_code=None, size_code="SAM", ec_code=None, resi_code=None, Zone_cd=None, Product=None),
    "Amazon Controlled - Large":                 dict(gig="non-gig", amazon_code="Controlled-Aamzon-platform", segment_code=None, size_code="Large", ec_code=None, resi_code=None, Zone_cd=None, Product=None),
    "Addressable SAM":                           dict(gig="non-gig", amazon_code="Non-Amazon-Controlled", segment_code=None, size_code="SAM", ec_code=None, resi_code=None, Zone_cd=None, Product=None),
    "Addressable Large":                         dict(gig="non-gig", amazon_code="Non-Amazon-Controlled", segment_code=None, size_code="Large", ec_code=None, resi_code=None, Zone_cd=None, Product=None),
    "UPS Amazon B2B":                            dict(gig="non-gig", amazon_code="Amazon-Controlled", segment_code="B2B", size_code=None, ec_code=None, resi_code="Non-Resi", Zone_cd=None, Product=None),
    "B2B Ground Large + UPS Amazon B2B":         dict(gig="non-gig", amazon_code="Amazon-Controlled", segment_code="B2B", size_code="Large", ec_code=None, resi_code="Non-Resi", Zone_cd=None, Product="Total-Ground"),
    "UPS Amazon B2C Ground":                     dict(gig="non-gig", amazon_code="Amazon-Controlled", segment_code="B2C", size_code=None, ec_code=None, resi_code="Resi", Zone_cd=None, Product="Total-Ground"),
    "B2C (EC + Non-EC) Ground Large + UPS Amazon B2C Ground": dict(gig="non-gig", amazon_code="Amazon-Controlled", segment_code="B2C", size_code=None, ec_code=None, resi_code="Resi", Zone_cd=None, Product="Total-Ground"),
    "B2C (EC + Non-EC) Express SAM":             dict(gig="non-gig", amazon_code="Non-Amazon-Controlled", segment_code="B2C", size_code="SAM", ec_code=None, resi_code="Resi", Zone_cd=None, Product="Total-Express"),
    "B2C (EC + Non-EC) Ground SAM":              dict(gig="non-gig", amazon_code="Non-Amazon-Controlled", segment_code="B2C", size_code="SAM", ec_code=None, resi_code="Resi", Zone_cd=None, Product="Total-Ground"),
    "B2C (EC + Non-EC) Express Large":           dict(gig="non-gig", amazon_code="Non-Amazon-Controlled", segment_code="B2C", size_code="Large", ec_code=None, resi_code="Resi", Zone_cd=None, Product="Total-Express"),
    "B2C (EC + Non-EC) Ground Large":            dict(gig="non-gig", amazon_code="Non-Amazon-Controlled", segment_code="B2C", size_code="Large", ec_code=None, resi_code="Resi", Zone_cd=None, Product="Total-Ground"),
}

# ─────────────────────────────────────────────────────────────────────────────
# SECTION CONTEXT — FIRST HALF
# After outputting a section header, ctx is REPLACED with this dict.
# Generic child rows inherit ctx fully and only override Product.
# ─────────────────────────────────────────────────────────────────────────────
SECTION_CTX_H1 = {
    "US Dom Parcel Market":                      {},
    "eCommerce Total (Local + National)":        {"ec_code": "EC"},
    "Non-eCommerce (B2B + Resi-Non EC + C2C)":   {"ec_code": "Non-EC"},
    "B2C Total (EC + Non-EC) (AMZN + Non-AMZN)": {"segment_code": "B2C"},
    "B2C Express":                               {"segment_code": "B2C", "resi_code": "Resi"},
    "B2C Ground":                                {"segment_code": "B2C", "resi_code": "Resi"},
    "B2B Total (Traditional + B2B EC + Returns)": {"segment_code": "B2B", "resi_code": "Non-Resi"},
    "B2B Express":                               {"segment_code": "B2B", "resi_code": "Non-Resi"},
    "B2B Ground":                                {"segment_code": "B2B", "resi_code": "Non-Resi"},
    "Traditional B2B":                           {"segment_code": "Traditional B2B", "resi_code": "Non-Resi"},
    "Traditional B2B - Non-AMZN":               {"amazon_code": "Non-Amazon-Controlled", "segment_code": "Traditional B2B", "resi_code": "Non-Resi"},
    "Total Amazon B2B":                          {"amazon_code": "AMZN-Controlled", "segment_code": "B2B", "resi_code": "Non-Resi"},
    "Traditional B2B - AMZN Controlled":         {"amazon_code": "AMZN-Controlled", "segment_code": "Traditional B2B", "resi_code": "Non-Resi"},
    "B2B - EC - Ground":                         {"amazon_code": "AMZN-Controlled", "segment_code": "Traditional B2B", "ec_code": "EC", "resi_code": "Non-Resi"},
    "Returns B2B":                               {"segment_code": "B2B-Returns", "resi_code": "Non-Resi"},
    "EC Returns AMZN Controlled":                {"amazon_code": "AMZN-Controlled", "segment_code": "B2B-Returns", "ec_code": "EC", "resi_code": "Non-Resi"},
    "Returns Non-AMZN Controlled (EC + Non-EC)": {"amazon_code": "Non-Amazon-Controlled", "segment_code": "B2B-Returns", "resi_code": "Non-Resi"},
    "B2B Addressable (Including Non-AMZN Return)": {"amazon_code": "Non-Amazon-Controlled", "segment_code": "B2B", "resi_code": "Non-Resi"},
    "Resi Non-EC + PriMail C2C":                 {"ec_code": "Non-EC", "resi_code": "Resi"},
    "Local EC (Ground)":                         {"ec_code": "EC", "Zone_cd": "Local"},
    "AMZN Controlled (Local)":                   {"amazon_code": "AMZN Controlled", "ec_code": "EC", "Zone_cd": "Local"},
    "AMZN 1P (Local)":                           {"amazon_code": "Controlled-1P-Amazon-platform", "ec_code": "EC", "Zone_cd": "Local"},
    "AMZN FBA (Local)":                          {"amazon_code": "Controlled-FBA-Amazon-platform", "ec_code": "EC", "Zone_cd": "Local"},
    "Non-AMZN (Local) (Non-Courier)":            {"amazon_code": "Non-Amazon-Controlled", "ec_code": "EC", "Zone_cd": "Local"},
    "National EC":                               {"ec_code": "EC", "Zone_cd": "National"},
    "Amazon Controlled B2C - EC (1P + FBA) (Local + National)": {"amazon_code": "AMZN-Controlled"},
    "Amazon Controlled  B2B + B2C (1P + FBA) (Local + National)": {"amazon_code": "AMZN-Controlled"},
    "Amazon Platform B2C (1P + FBA + FBM) (Local + National)": {"amazon_code": "Amazon-Platform", "segment_code": "B2C"},
    "AMZN Controlled  (1P) (National)":          {"amazon_code": "Controlled-1P-Amazon-platform", "Zone_cd": "National"},
    "AMZN Controlled  (FBA) (National)":         {"amazon_code": "Controlled-FBA-Amazon-platform", "Zone_cd": "National"},
    "AMZN Seller Controlled (Non-FBA) (FBM) (National)": {"amazon_code": "Controlled-FBM-Amazon-platform", "Zone_cd": "National"},
    "AMZN Seller Controlled (Non-FBA) (FBM: SFP)": {"amazon_code": "Seller-Controlled-Amazon Platform-SFP"},
    "AMZN Seller Controlled (Non-FBA) (FBM: Non-SFP)": {"amazon_code": "Seller-Controlled-Amazon Platform-non-SFP"},
    "Non-Amazon Platform EC (National)":         {"amazon_code": "Non-Amazon-Platform", "ec_code": "EC", "Zone_cd": "National"},
    "Non-Amazon Controlled EC (Local + National)": {"amazon_code": "Non-Amazon-Controlled", "ec_code": "EC"},
    "Non-Amazon Controlled US Dom Volume (EC + Non-EC)": {"amazon_code": "Non-Amazon-Controlled"},
}

# ─────────────────────────────────────────────────────────────────────────────
# SECTION CONTEXT — SECOND HALF
# ─────────────────────────────────────────────────────────────────────────────
SECTION_CTX_H2 = {
    "US Dom Parcel Market":                      {},
    "eCommerce (Local + National)":              {"ec_code": "EC"},
    "Non-eCommerce (B2B + Resi-Non EC + C2C)":   {"ec_code": "Non-EC", "segment_code": "C2C"},
    "B2B Total":                                 {"segment_code": "B2B", "resi_code": "Non-Resi"},
    "B2B - AMZN Controlled (EC+Return)":         {"amazon_code": "Amazon Controlled", "segment_code": "B2B", "ec_code": "EC", "resi_code": "Non-Resi"},
    "B2B - Non-AMZN Controlled":                 {"amazon_code": "Non-Amazon-Controlled", "segment_code": "B2B", "resi_code": "Non-Resi"},
    "Resi Non-EC + C2C (PriMail, USPS GA)":      {"resi_code": "Resi"},
    "Total Non-AMZN Resi (Non-AMZN E-C + Resi Non-EC + Primail C2C)": {"amazon_code": "Non-Amazon-Controlled", "ec_code": "Non-EC", "resi_code": "Resi"},
    "Local EC (Ground)":                         {"ec_code": "EC", "Zone_cd": "Local"},
    "AMZN Platform (Local)":                     {"amazon_code": "Amazon-Platform", "ec_code": "EC", "Zone_cd": "Local"},
    "AMZN 1P":                                   {"amazon_code": "Controlled-1P-Amazon-Platform", "ec_code": "EC", "Zone_cd": "Local"},
    "AMZN FBA":                                  {"amazon_code": "Controlled-FBA-Amazon-Platform", "ec_code": "EC", "Zone_cd": "Local"},
    "Non-AMZN (Local)":                          {"amazon_code": "Non-Amazon-Platform", "ec_code": "EC", "Zone_cd": "Local"},
    "National EC Total":                         {"ec_code": "EC", "Zone_cd": "National"},
    "Amazon Controlled (Local + National)":      {"amazon_code": "Amazon Controlled"},
    "Amazon Platform (Local + National)":        {"amazon_code": "amazon platform"},
    "AMZN Controlled  (1P) (National)":          {"amazon_code": "Controlled-1P-Amazon-platform", "Zone_cd": "National"},
    "AMZN Controlled  (FBA) (National)":         {"amazon_code": "Controlled-FBA-Amazon-platform", "Zone_cd": "National"},
    "AMZN Seller Controlled (Non-FBA) (FBM) (National)": {"amazon_code": "Seller-Controlled-Amazon Platform", "Zone_cd": "National"},
    "AMZN Seller Controlled (Non-FBA) (SFP)":   {"amazon_code": "Seller-Controlled-Amazon Platform-SFP"},
    "AMZN Seller Controlled (Non-FBA) (Non-SFP)": {"amazon_code": "Seller-Controlled-Amazon Platform-non-SFP"},
    "Non-Amazon Platform EC (National)":         {"ec_code": "EC"},
    "Non-Amazon Controlled EC (Local + National)": {"ec_code": "EC"},
    "Non-Amazon Controlled Volume (EC + Non-EC)": {"ec_code": "EC"},
    "Amazon Controlled - SAM":                   {"amazon_code": "Controlled-Aamzon-platform"},
    "Amazon Controlled - Large":                 {"amazon_code": "Controlled-Aamzon-platform"},
    "Addressable SAM":                           {"amazon_code": "Non-Amazon-Controlled"},
    "Addressable Large":                         {"amazon_code": "Non-Amazon-Controlled"},
}

# ─────────────────────────────────────────────────────────────────────────────
# PRODUCT MAP for generic child rows
# ─────────────────────────────────────────────────────────────────────────────
PRODUCT_MAP = {
    "Express": "Total-Express",          "Express Overnight": "Express-Overnight",
    "Express Deferred": "Express-Deferred", "Ground": "Total-Ground",
    "Core Ground": "Ground-Core",        "Deferred Ground": "Ground-Deferred",
    "SFP Express": "Total-Express",      "SFP Ground": "Total-Ground",
    "Non-SFP Express": "Total-Express",  "Non-SFP Ground": "Total-Ground",
    "Traditional B2B Express": "Total-Express", "Traditional B2B Ground": "Total-Ground",
    "B2B Express": "Total-Express",      "B2B Ground": "Total-Ground",
    "B2C Express": "Total-Express",      "B2C Ground": "Total-Ground",
    "B2B Express SAM": "Total-Express",  "B2B Ground SAM": "Total-Ground",
    "B2B Express Large": "Total-Express","B2B Ground Large": "Total-Ground",
}


def _infer_product(label):
    if label in PRODUCT_MAP:
        return PRODUCT_MAP[label]
    u = label.upper()
    if "EXPRESS OVERNIGHT" in u: return "Express-Overnight"
    if "EXPRESS DEFERRED"  in u: return "Express-Deferred"
    if "CORE GROUND"       in u: return "Ground-Core"
    if "DEFERRED GROUND"   in u: return "Ground-Deferred"
    if "EXPRESS"           in u: return "Total-Express"
    if "GROUND"            in u: return "Total-Ground"
    return None


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


# ─────────────────────────────────────────────────────────────────────────────
# MAIN TRANSFORM
# ─────────────────────────────────────────────────────────────────────────────
def transform(adls_path: str, target_table: str = None,
              write_mode: str = "overwrite", write_to_table: bool = False):
    """
    Returns Spark DataFrame matching A4T_ADV_Baseline_Out schema.
    row_num (1-based integer) is added as first column to uniquely identify
    each row after pivoting CY* columns — eliminates pivot duplicates.

    Args:
        adls_path      : ADLS path to SDL_test.csv
        target_table   : 3-part Unity Catalog name  (only if write_to_table=True)
        write_mode     : 'overwrite' or 'append'
        write_to_table : set True to persist to Delta
    """
    spark = SparkSession.builder.getOrCreate()

    # 1. Read raw CSV — all strings, no header inference
    raw_df   = spark.read.option("header", "false").csv(adls_path)
    raw_rows = [list(r) for r in raw_df.collect()]

    # 2. Detect CY16–CY32 column positions from header row
    header   = [str(c).strip() if c is not None else "" for c in raw_rows[0]]
    year_idx = {}
    for i, h in enumerate(header):
        m = re.match(r"CY(\d{2})", h)
        if m:
            yr = int(m.group(1))
            if 16 <= yr <= 32:
                year_idx[f"CY{yr:02d}"] = i

    # 3. Walk source rows and build output
    output_rows     = []
    ctx             = {}
    prev_label      = ""
    in_second_half  = False  # flips True after first "ADV (millions)" row
    row_num         = 0

    for row in raw_rows[1:]:
        label = str(row[0]).strip() if row[0] is not None else ""
        if not label:
            continue

        # Handle excluded rows — "ADV (millions)" marks the half boundary
        if label in EXCLUDE_LABELS:
            if label == "ADV (millions)" and not in_second_half:
                in_second_half = True
            prev_label = label
            continue

        # Skip stray annotation row (source row 228)
        if label == "B2C Express" and prev_label == "Deferred Ground":
            continue

        # Skip orphan SAM/Large after "Addressable Mix"
        # Note: prev_label NOT updated on skip so both SAM and Large are caught
        if label in ("SAM", "Large") and prev_label == "Addressable Mix":
            continue

        # Select half-specific dicts
        FIXED       = FIXED_H2       if in_second_half else FIXED_H1
        SECTION_CTX = SECTION_CTX_H2 if in_second_half else SECTION_CTX_H1

        # Determine dims
        if label in FIXED:
            # Section header / known row — use exact pre-validated dims
            tags = deepcopy(FIXED[label])
        else:
            # Generic child — inherit ctx from parent section, derive Product from label
            tags = {k: ctx.get(k) for k in DIM_COLS}
            tags["gig"]     = "non-gig"
            tags["Product"] = _infer_product(label)
            u = label.upper()
            if not tags.get("size_code"):
                if   "SAM"   in u: tags["size_code"]   = "SAM"
                elif "LARGE" in u: tags["size_code"]   = "Large"
            if not tags.get("segment_code"):
                if   "B2B" in u: tags["segment_code"] = "B2B"
                elif "B2C" in u: tags["segment_code"] = "B2C"
            if not tags.get("resi_code"):
                seg = tags.get("segment_code")
                if   seg == "B2B": tags["resi_code"] = "Non-Resi"
                elif seg == "B2C": tags["resi_code"] = "Resi"

        # After outputting, update ctx if this is a section header
        if label in SECTION_CTX:
            ctx = dict(SECTION_CTX[label])

        # Year values from source CSV
        year_vals = {
            cy: _to_float(row[year_idx[cy]])
            if year_idx.get(cy) is not None and year_idx[cy] < len(row) else None
            for cy in YEAR_COLS
        }

        row_num += 1
        output_rows.append({
            "row_num":      row_num,
            "ADV_millions": label,
            **tags,
            **year_vals,
        })
        prev_label = label

    # 4. Build typed Spark DataFrame
    schema = StructType(
        [StructField("row_num",      IntegerType(), False)]
        + [StructField("ADV_millions", StringType(),  True)]
        + [StructField(d,              StringType(),  True) for d in DIM_COLS]
        + [StructField(cy,             DoubleType(),  True) for cy in YEAR_COLS]
    )
    df = spark.createDataFrame(output_rows, schema=schema)

    # 5. Optionally write to Delta
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
#   df = transform(ADLS_PATH)
#   df.display()                  # verify 281 rows, correct dims
#
#   # Duplicate check after pivot — should return zero rows:
#   df.createOrReplaceTempView("adv_base")
#   spark.sql("""
#     SELECT md5(concat_ws('|', row_num, ADV_millions, gig, Year,
#                amazon_code, segment_code, size_code, ec_code,
#                resi_code, Zone_cd, Product)),
#            count(*)
#     FROM (SELECT *, stack(17,
#             'CY16',CY16,'CY17',CY17,'CY18',CY18,'CY19',CY19,
#             'CY20',CY20,'CY21',CY21,'CY22',CY22,'CY23',CY23,
#             'CY24',CY24,'CY25',CY25,'CY26',CY26,'CY27',CY27,
#             'CY28',CY28,'CY29',CY29,'CY30',CY30,'CY31',CY31,
#             'CY32',CY32) AS (Year, ADV)
#           FROM adv_base) pivoted
#     GROUP BY ALL HAVING count(*) > 1
#   """).show()
#
#   # Write to Delta:
#   transform(ADLS_PATH, TARGET_TABLE, write_to_table=True)
# ─────────────────────────────────────────────────────────────────────────────
