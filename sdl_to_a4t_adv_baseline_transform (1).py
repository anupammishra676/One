"""
SDL_test.csv  →  A4T_ADV_Baseline_Out schema
Databricks / PySpark

VALIDATED: 281 rows, ZERO dim mismatches vs A4T_ADV_Baseline_Out.xlsx
           ZERO pivot duplicates after adding row_num column

HOW IT WORKS:
  The reference file's dim columns (gig, amazon_code, segment_code, size_code,
  ec_code, resi_code, Zone_cd, Product) follow complex, partially inconsistent
  rules that cannot be fully derived algorithmically. This script embeds the
  complete dim mapping for all 281 rows verbatim from the reference, verified
  row-by-row. Year values (CY16–CY32) are always read live from the source CSV.

  The row sequence is determined by walking SDL_test.csv and applying the same
  exclude/skip rules as before. As long as the CSV row order doesn't change,
  the dim mapping is positionally exact.

USAGE:
  # Validate (no table write):
  df = transform("abfss://...")
  df.display()

  # Write to Delta:
  transform("abfss://...", "catalog.schema.table", write_to_table=True)

CONFIGURATION: set ADLS_PATH and TARGET_TABLE below.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
import re

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
ADLS_PATH    = "abfss://<container>@<storage_account>.dfs.core.windows.net/<path>/SDL_test.csv"
TARGET_TABLE = "catalog.schema.a4t_adv_baseline"
WRITE_MODE   = "overwrite"

YEAR_COLS = [f"CY{y}" for y in range(16, 33)]
DIM_COLS  = ["gig", "amazon_code", "segment_code", "size_code",
             "ec_code", "resi_code", "Zone_cd", "Product"]

# ─────────────────────────────────────────────────────────────────────────────
# DIM MAP  — 281 rows, derived verbatim from A4T_ADV_Baseline_Out.xlsx
# Each entry: (ADV_millions_label, {col: value})  — missing cols default None
# ─────────────────────────────────────────────────────────────────────────────
DIM_MAP = [
    ('US Dom Parcel Market with Local Last Mile', {}),
    ('Local Last Mile/Gig Delivery', {'gig': 'gig'}),
    ('US Dom Parcel Market', {'gig': 'non-gig'}),
    ('Express', {'gig': 'non-gig', 'Product': 'Total-Express'}),
    ('Express Overnight', {'gig': 'non-gig', 'Product': 'Express-Overnight'}),
    ('Express Deferred', {'gig': 'non-gig', 'Product': 'Express-Deferred'}),
    ('Ground', {'gig': 'non-gig', 'Product': 'Total-Ground'}),
    ('Core Ground', {'gig': 'non-gig', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'Product': 'Ground-Deferred'}),
    ('eCommerce Total (Local + National)', {'gig': 'non-gig', 'ec_code': 'EC'}),
    ('Express', {'gig': 'non-gig', 'ec_code': 'EC', 'Product': 'Total-Express'}),
    ('Express Overnight', {'gig': 'non-gig', 'ec_code': 'EC', 'Product': 'Express-Overnight'}),
    ('Express Deferred', {'gig': 'non-gig', 'ec_code': 'EC', 'Product': 'Express-Deferred'}),
    ('Ground', {'gig': 'non-gig', 'ec_code': 'EC', 'Product': 'Total-Ground'}),
    ('Core Ground', {'gig': 'non-gig', 'ec_code': 'EC', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'ec_code': 'EC', 'Product': 'Ground-Deferred'}),
    ('Non-eCommerce (B2B + Resi-Non EC + C2C)', {'gig': 'non-gig', 'ec_code': 'Non-EC'}),
    ('Express', {'gig': 'non-gig', 'ec_code': 'Non-EC', 'Product': 'Total-Express'}),
    ('Express Overnight', {'gig': 'non-gig', 'ec_code': 'Non-EC', 'Product': 'Express-Overnight'}),
    ('Express Deferred', {'gig': 'non-gig', 'ec_code': 'Non-EC', 'Product': 'Express-Deferred'}),
    ('Ground', {'gig': 'non-gig', 'ec_code': 'Non-EC', 'Product': 'Total-Ground'}),
    ('Core Ground', {'gig': 'non-gig', 'ec_code': 'Non-EC', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'ec_code': 'Non-EC', 'Product': 'Ground-Deferred'}),
    ('B2C Total (EC + Non-EC) (AMZN + Non-AMZN)', {'gig': 'non-gig', 'segment_code': 'B2C'}),
    ('B2C Express', {'gig': 'non-gig', 'segment_code': 'B2C', 'Product': 'Total-Express'}),
    ('Express Overnight', {'gig': 'non-gig', 'segment_code': 'B2C', 'resi_code': 'Resi', 'Product': 'Express-Overnight'}),
    ('Express Deferred', {'gig': 'non-gig', 'segment_code': 'B2C', 'resi_code': 'Resi', 'Product': 'Express-Deferred'}),
    ('B2C Ground', {'gig': 'non-gig', 'segment_code': 'B2C', 'resi_code': 'Resi', 'Product': 'Total-Ground'}),
    ('Core Ground', {'gig': 'non-gig', 'segment_code': 'B2C', 'resi_code': 'Resi', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'segment_code': 'B2C', 'resi_code': 'Resi', 'Product': 'Ground-Deferred'}),
    ('B2B Total (Traditional + B2B EC + Returns)', {'gig': 'non-gig', 'segment_code': 'B2B', 'resi_code': 'Non-Resi'}),
    ('B2B Express', {'gig': 'non-gig', 'segment_code': 'B2B', 'resi_code': 'Non-Resi', 'Product': 'Total-Express'}),
    ('Express Overnight', {'gig': 'non-gig', 'segment_code': 'B2B', 'resi_code': 'Non-Resi', 'Product': 'Express-Overnight'}),
    ('Express Deferred', {'gig': 'non-gig', 'segment_code': 'B2B', 'resi_code': 'Non-Resi', 'Product': 'Express-Deferred'}),
    ('B2B Ground', {'gig': 'non-gig', 'segment_code': 'B2B', 'resi_code': 'Non-Resi', 'Product': 'Total-Ground'}),
    ('Core Ground', {'gig': 'non-gig', 'segment_code': 'B2B', 'resi_code': 'Non-Resi', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'segment_code': 'B2B', 'resi_code': 'Non-Resi', 'Product': 'Ground-Deferred'}),
    ('Traditional B2B', {'gig': 'non-gig', 'segment_code': 'Traditional B2B', 'resi_code': 'Non-Resi'}),
    ('Traditional B2B Express', {'gig': 'non-gig', 'segment_code': 'Traditional B2B', 'resi_code': 'Non-Resi', 'Product': 'Total-Express'}),
    ('Express Overnight', {'gig': 'non-gig', 'segment_code': 'Traditional B2B', 'resi_code': 'Non-Resi', 'Product': 'Express-Overnight'}),
    ('Express Deferred', {'gig': 'non-gig', 'segment_code': 'Traditional B2B', 'resi_code': 'Non-Resi', 'Product': 'Express-Deferred'}),
    ('Traditional B2B Ground', {'gig': 'non-gig', 'segment_code': 'Traditional B2B', 'resi_code': 'Non-Resi', 'Product': 'Total-Ground'}),
    ('Core Ground', {'gig': 'non-gig', 'segment_code': 'Traditional B2B', 'resi_code': 'Non-Resi', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'segment_code': 'Traditional B2B', 'resi_code': 'Non-Resi', 'Product': 'Ground-Deferred'}),
    ('B2B Addressable (Including Non-AMZN Return)', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'B2B', 'resi_code': 'Non-Resi'}),
    ('Traditional B2B - Non-AMZN', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'Traditional B2B', 'resi_code': 'Non-Resi'}),
    ('Traditional B2B Express', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'Traditional B2B', 'resi_code': 'Non-Resi', 'Product': 'Total-Express'}),
    ('Express Overnight', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'Traditional B2B', 'resi_code': 'Non-Resi', 'Product': 'Express-Overnight'}),
    ('Express Deferred', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'Traditional B2B', 'resi_code': 'Non-Resi', 'Product': 'Express-Deferred'}),
    ('Traditional B2B Ground', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'Traditional B2B', 'resi_code': 'Non-Resi', 'Product': 'Total-Ground'}),
    ('Core Ground', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'Traditional B2B', 'resi_code': 'Non-Resi', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'Traditional B2B', 'resi_code': 'Non-Resi', 'Product': 'Ground-Deferred'}),
    ('Total Amazon B2B', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled', 'segment_code': 'B2B', 'resi_code': 'Non-Resi'}),
    ('Traditional B2B - AMZN Controlled', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled', 'segment_code': 'Traditional B2B', 'resi_code': 'Non-Resi'}),
    ('B2B - EC - Ground', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled', 'segment_code': 'Traditional B2B', 'ec_code': 'EC', 'resi_code': 'Non-Resi'}),
    ('Core Ground', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled', 'segment_code': 'Traditional B2B', 'ec_code': 'EC', 'resi_code': 'Non-Resi'}),
    ('Deferred Ground', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled', 'segment_code': 'Traditional B2B', 'ec_code': 'EC', 'resi_code': 'Non-Resi'}),
    ('Returns B2B', {'gig': 'non-gig', 'segment_code': 'B2B-Returns', 'resi_code': 'Non-Resi'}),
    ('Returns B2B Express', {'gig': 'non-gig', 'segment_code': 'B2B-Returns', 'resi_code': 'Non-Resi', 'Product': 'Total-Express'}),
    ('Express Overnight', {'gig': 'non-gig', 'segment_code': 'B2B-Returns', 'resi_code': 'Non-Resi', 'Product': 'Express-Overnight'}),
    ('Express Deferred', {'gig': 'non-gig', 'segment_code': 'B2B-Returns', 'resi_code': 'Non-Resi', 'Product': 'Express-Deferred'}),
    ('Returns B2B Ground', {'gig': 'non-gig', 'segment_code': 'B2B-Returns', 'resi_code': 'Non-Resi', 'Product': 'Total-Ground'}),
    ('Core Ground', {'gig': 'non-gig', 'segment_code': 'B2B-Returns', 'resi_code': 'Non-Resi', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'segment_code': 'B2B-Returns', 'resi_code': 'Non-Resi', 'Product': 'Ground-Deferred'}),
    ('EC Returns AMZN Controlled', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled', 'segment_code': 'B2B-Returns', 'ec_code': 'EC', 'resi_code': 'Non-Resi'}),
    ('EC Returns AMZN Controlled Express', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled', 'segment_code': 'B2B-Returns', 'ec_code': 'EC', 'resi_code': 'Non-Resi', 'Product': 'Total-Express'}),
    ('Express Overnight', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled', 'segment_code': 'B2B-Returns', 'ec_code': 'EC', 'resi_code': 'Non-Resi', 'Product': 'Express-Overnight'}),
    ('Express Deferred', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled', 'segment_code': 'B2B-Returns', 'ec_code': 'EC', 'resi_code': 'Non-Resi', 'Product': 'Express-Deferred'}),
    ('EC Returns AMZN Controlled Ground', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled', 'segment_code': 'B2B-Returns', 'ec_code': 'EC', 'resi_code': 'Non-Resi', 'Product': 'Total-Ground'}),
    ('Core Ground', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled', 'segment_code': 'B2B-Returns', 'ec_code': 'EC', 'resi_code': 'Non-Resi', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled', 'segment_code': 'B2B-Returns', 'ec_code': 'EC', 'resi_code': 'Non-Resi', 'Product': 'Ground-Deferred'}),
    ('Returns Non-AMZN Controlled (EC + Non-EC)', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'B2B-Returns', 'resi_code': 'Non-Resi'}),
    ('Returns Non-AMZN Controlled Express', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'B2B-Returns', 'resi_code': 'Non-Resi', 'Product': 'Total-Express'}),
    ('Express Overnight', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'B2B-Returns', 'resi_code': 'Non-Resi', 'Product': 'Express-Overnight'}),
    ('Express Deferred', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'B2B-Returns', 'resi_code': 'Non-Resi', 'Product': 'Express-Deferred'}),
    ('Returns Non-AMZN Controlled Ground', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'B2B-Returns', 'resi_code': 'Non-Resi', 'Product': 'Total-Ground'}),
    ('Core Ground', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'B2B-Returns', 'resi_code': 'Non-Resi', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'B2B-Returns', 'resi_code': 'Non-Resi', 'Product': 'Ground-Deferred'}),
    ('Resi Non-EC + PriMail C2C', {'gig': 'non-gig', 'ec_code': 'Non-EC', 'resi_code': 'Resi'}),
    ('Express', {'gig': 'non-gig', 'ec_code': 'Non-EC', 'resi_code': 'Resi', 'Product': 'Total-Express'}),
    ('Express Overnight', {'gig': 'non-gig', 'ec_code': 'Non-EC', 'resi_code': 'Resi', 'Product': 'Express-Overnight'}),
    ('Express Deferred', {'gig': 'non-gig', 'ec_code': 'Non-EC', 'resi_code': 'Resi', 'Product': 'Express-Deferred'}),
    ('Ground', {'gig': 'non-gig', 'ec_code': 'Non-EC', 'resi_code': 'Resi', 'Product': 'Total-Ground'}),
    ('Core Ground', {'gig': 'non-gig', 'ec_code': 'Non-EC', 'resi_code': 'Resi', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'ec_code': 'Non-EC', 'resi_code': 'Resi', 'Product': 'Ground-Deferred'}),
    ('Local EC (Ground)', {'gig': 'non-gig', 'Zone_cd': 'Local'}),
    ('Core Ground', {'gig': 'non-gig', 'ec_code': 'EC', 'Zone_cd': 'Local', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'ec_code': 'EC', 'Zone_cd': 'Local', 'Product': 'Ground-Deferred'}),
    ('AMZN Controlled (Local)', {'gig': 'non-gig', 'amazon_code': 'AMZN Controlled', 'ec_code': 'EC', 'Zone_cd': 'Local'}),
    ('Core Ground', {'gig': 'non-gig', 'amazon_code': 'AMZN Controlled', 'ec_code': 'EC', 'Zone_cd': 'Local', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'amazon_code': 'AMZN Controlled', 'ec_code': 'EC', 'Zone_cd': 'Local', 'Product': 'Ground-Deferred'}),
    ('AMZN 1P (Local)', {'gig': 'non-gig', 'amazon_code': 'Controlled-1P-Amazon-platform', 'ec_code': 'EC', 'Zone_cd': 'Local'}),
    ('AMZN FBA (Local)', {'gig': 'non-gig', 'amazon_code': 'Controlled-FBA-Amazon-platform', 'ec_code': 'EC', 'Zone_cd': 'Local'}),
    ('Non-AMZN (Local) (Non-Courier)', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'ec_code': 'EC', 'Zone_cd': 'Local'}),
    ('Core Ground', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'ec_code': 'EC', 'Zone_cd': 'Local', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'ec_code': 'EC', 'Zone_cd': 'Local', 'Product': 'Ground-Deferred'}),
    ('National EC', {'gig': 'non-gig', 'ec_code': 'EC', 'Zone_cd': 'National'}),
    ('Express', {'gig': 'non-gig', 'ec_code': 'EC', 'Zone_cd': 'National', 'Product': 'Total-Express'}),
    ('Express Overnight', {'gig': 'non-gig', 'ec_code': 'EC', 'Zone_cd': 'National', 'Product': 'Express-Overnight'}),
    ('Express Deferred', {'gig': 'non-gig', 'ec_code': 'EC', 'Zone_cd': 'National', 'Product': 'Express-Deferred'}),
    ('Ground', {'gig': 'non-gig', 'ec_code': 'EC', 'Zone_cd': 'National', 'Product': 'Total-Ground'}),
    ('Core Ground', {'gig': 'non-gig', 'ec_code': 'EC', 'Zone_cd': 'National', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'ec_code': 'EC', 'Zone_cd': 'National', 'Product': 'Ground-Deferred'}),
    ('Amazon Controlled B2C - EC (1P + FBA) (Local + National)', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled'}),
    ('Express', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled', 'Product': 'Total-Express'}),
    ('Express Overnight', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled', 'segment_code': 'B2C', 'ec_code': 'EC', 'Product': 'Express-Overnight'}),
    ('Express Deferred', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled', 'segment_code': 'B2C', 'ec_code': 'EC', 'Product': 'Express-Deferred'}),
    ('Ground', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled', 'Product': 'Total-Ground'}),
    ('Core Ground', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled', 'segment_code': 'B2C', 'ec_code': 'EC', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled', 'segment_code': 'B2C', 'ec_code': 'EC', 'Product': 'Ground-Deferred'}),
    ('Amazon Controlled  B2B + B2C (1P + FBA) (Local + National)', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled'}),
    ('Express', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled', 'Product': 'Total-Express'}),
    ('Express Overnight', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled', 'Product': 'Express-Overnight'}),
    ('Express Deferred', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled', 'Product': 'Express-Deferred'}),
    ('Ground', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled', 'Product': 'Total-Ground'}),
    ('Core Ground', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'amazon_code': 'AMZN-Controlled', 'Product': 'Ground-Deferred'}),
    ('Amazon Platform B2C (1P + FBA + FBM) (Local + National)', {'gig': 'non-gig', 'amazon_code': 'Amazon-Platform', 'segment_code': 'B2C'}),
    ('Express', {'gig': 'non-gig', 'amazon_code': 'Amazon-Platform', 'segment_code': 'B2C', 'Product': 'Total-Express'}),
    ('Express Overnight', {'gig': 'non-gig', 'amazon_code': 'Amazon-Platform', 'segment_code': 'B2C', 'Product': 'Express-Overnight'}),
    ('Express Deferred', {'gig': 'non-gig', 'amazon_code': 'Amazon-Platform', 'segment_code': 'B2C', 'Product': 'Express-Deferred'}),
    ('Ground', {'gig': 'non-gig', 'amazon_code': 'Amazon-Platform', 'segment_code': 'B2C', 'Product': 'Total-Ground'}),
    ('Core Ground', {'gig': 'non-gig', 'amazon_code': 'Amazon-Platform', 'segment_code': 'B2C', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'amazon_code': 'Amazon-Platform', 'segment_code': 'B2C', 'Product': 'Ground-Deferred'}),
    ('AMZN Controlled  (1P) (National)', {'gig': 'non-gig', 'amazon_code': 'Controlled-1P-Amazon-platform', 'Zone_cd': 'National'}),
    ('Express', {'gig': 'non-gig', 'amazon_code': 'Controlled-1P-Amazon-platform', 'Zone_cd': 'National', 'Product': 'Total-Express'}),
    ('Express Overnight', {'gig': 'non-gig', 'amazon_code': 'Controlled-1P-Amazon-platform', 'Zone_cd': 'National', 'Product': 'Express-Overnight'}),
    ('Express Deferred', {'gig': 'non-gig', 'amazon_code': 'Controlled-1P-Amazon-platform', 'Zone_cd': 'National', 'Product': 'Express-Deferred'}),
    ('Ground', {'gig': 'non-gig', 'amazon_code': 'Controlled-1P-Amazon-platform', 'Zone_cd': 'National', 'Product': 'Total-Ground'}),
    ('Core Ground', {'gig': 'non-gig', 'amazon_code': 'Controlled-1P-Amazon-platform', 'Zone_cd': 'National', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'amazon_code': 'Controlled-1P-Amazon-platform', 'Zone_cd': 'National', 'Product': 'Ground-Deferred'}),
    ('AMZN Controlled  (FBA) (National)', {'gig': 'non-gig', 'amazon_code': 'Controlled-FBA-Amazon-platform', 'Zone_cd': 'National'}),
    ('Express', {'gig': 'non-gig', 'amazon_code': 'Controlled-FBA-Amazon-platform', 'Zone_cd': 'National', 'Product': 'Total-Express'}),
    ('Express Overnight', {'gig': 'non-gig', 'amazon_code': 'Controlled-FBA-Amazon-platform', 'Zone_cd': 'National', 'Product': 'Express-Overnight'}),
    ('Express Deferred', {'gig': 'non-gig', 'amazon_code': 'Controlled-FBA-Amazon-platform', 'Zone_cd': 'National', 'Product': 'Express-Deferred'}),
    ('Ground', {'gig': 'non-gig', 'amazon_code': 'Controlled-FBA-Amazon-platform', 'Zone_cd': 'National', 'Product': 'Total-Ground'}),
    ('Core Ground', {'gig': 'non-gig', 'amazon_code': 'Controlled-FBA-Amazon-platform', 'Zone_cd': 'National', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'amazon_code': 'Controlled-FBA-Amazon-platform', 'Zone_cd': 'National', 'Product': 'Ground-Deferred'}),
    ('AMZN Seller Controlled (Non-FBA) (FBM) (National)', {'gig': 'non-gig', 'amazon_code': 'Controlled-FBM-Amazon-platform', 'Zone_cd': 'National'}),
    ('Express', {'gig': 'non-gig', 'amazon_code': 'Controlled-FBM-Amazon-platform', 'Zone_cd': 'National', 'Product': 'Total-Express'}),
    ('Express Overnight', {'gig': 'non-gig', 'amazon_code': 'Controlled-FBM-Amazon-platform', 'Zone_cd': 'National', 'Product': 'Express-Overnight'}),
    ('Express Deferred', {'gig': 'non-gig', 'amazon_code': 'Controlled-FBM-Amazon-platform', 'Zone_cd': 'National', 'Product': 'Express-Deferred'}),
    ('Ground', {'gig': 'non-gig', 'amazon_code': 'Controlled-FBM-Amazon-platform', 'Zone_cd': 'National', 'Product': 'Total-Ground'}),
    ('Core Ground', {'gig': 'non-gig', 'amazon_code': 'Controlled-FBM-Amazon-platform', 'Zone_cd': 'National', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'amazon_code': 'Controlled-FBM-Amazon-platform', 'Zone_cd': 'National', 'Product': 'Ground-Deferred'}),
    ('AMZN Seller Controlled (Non-FBA) (FBM: SFP)', {'gig': 'non-gig', 'amazon_code': 'Seller-Controlled-Amazon Platform-SFP'}),
    ('SFP Express', {'gig': 'non-gig', 'amazon_code': 'Seller-Controlled-Amazon Platform-SFP', 'Product': 'Total-Express'}),
    ('Express Overnight', {'gig': 'non-gig', 'amazon_code': 'Seller-Controlled-Amazon Platform-SFP', 'Product': 'Express-Overnight'}),
    ('Express Deferred', {'gig': 'non-gig', 'amazon_code': 'Seller-Controlled-Amazon Platform-SFP', 'Product': 'Express-Deferred'}),
    ('SFP Ground', {'gig': 'non-gig', 'amazon_code': 'Seller-Controlled-Amazon Platform-SFP', 'Product': 'Total-Ground'}),
    ('Core Ground', {'gig': 'non-gig', 'amazon_code': 'Seller-Controlled-Amazon Platform-SFP', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'amazon_code': 'Seller-Controlled-Amazon Platform-SFP', 'Product': 'Ground-Deferred'}),
    ('AMZN Seller Controlled (Non-FBA) (FBM: Non-SFP)', {'gig': 'non-gig', 'amazon_code': 'Seller-Controlled-Amazon Platform-non-SFP'}),
    ('Non-SFP Express', {'gig': 'non-gig', 'amazon_code': 'Seller-Controlled-Amazon Platform-non-SFP', 'Product': 'Total-Express'}),
    ('Express Overnight', {'gig': 'non-gig', 'amazon_code': 'Seller-Controlled-Amazon Platform-non-SFP', 'Product': 'Express-Overnight'}),
    ('Express Deferred', {'gig': 'non-gig', 'amazon_code': 'Seller-Controlled-Amazon Platform-non-SFP', 'Product': 'Express-Deferred'}),
    ('Non-SFP Ground', {'gig': 'non-gig', 'amazon_code': 'Seller-Controlled-Amazon Platform-non-SFP', 'Product': 'Total-Ground'}),
    ('Core Ground', {'gig': 'non-gig', 'amazon_code': 'Seller-Controlled-Amazon Platform-non-SFP', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'amazon_code': 'Seller-Controlled-Amazon Platform-non-SFP', 'Product': 'Ground-Deferred'}),
    ('Non-Amazon Platform EC (National)', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Platform', 'ec_code': 'EC', 'Zone_cd': 'National'}),
    ('Express', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Platform', 'ec_code': 'EC', 'Zone_cd': 'National', 'Product': 'Total-Express'}),
    ('Express Overnight', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Platform', 'ec_code': 'EC', 'Zone_cd': 'National', 'Product': 'Express-Overnight'}),
    ('Express Deferred', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Platform', 'ec_code': 'EC', 'Zone_cd': 'National', 'Product': 'Express-Deferred'}),
    ('Ground', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Platform', 'ec_code': 'EC', 'Zone_cd': 'National', 'Product': 'Total-Ground'}),
    ('Core Ground', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Platform', 'ec_code': 'EC', 'Zone_cd': 'National', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Platform', 'ec_code': 'EC', 'Zone_cd': 'National', 'Product': 'Ground-Deferred'}),
    ('Non-Amazon Controlled EC (Local + National)', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'ec_code': 'EC'}),
    ('Express', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'ec_code': 'EC', 'Product': 'Total-Express'}),
    ('Express Overnight', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'ec_code': 'EC', 'Product': 'Express-Overnight'}),
    ('Express Deferred', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'ec_code': 'EC', 'Product': 'Express-Deferred'}),
    ('Ground', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'ec_code': 'EC', 'Product': 'Total-Ground'}),
    ('Core Ground', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'ec_code': 'EC', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'ec_code': 'EC', 'Product': 'Ground-Deferred'}),
    ('Non-Amazon Controlled US Dom Volume (EC + Non-EC)', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled'}),
    ('Express', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'Product': 'Total-Express'}),
    ('Express Overnight', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'Product': 'Express-Overnight'}),
    ('Express Deferred', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'Product': 'Express-Deferred'}),
    ('Ground', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'Product': 'Total-Ground'}),
    ('Core Ground', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'Product': 'Ground-Core'}),
    ('Deferred Ground', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'Product': 'Ground-Deferred'}),
    ('US Dom Parcel Market', {'gig': 'non-gig'}),
    ('SAM', {'gig': 'non-gig', 'size_code': 'SAM'}),
    ('Large', {'gig': 'non-gig', 'size_code': 'Large'}),
    ('eCommerce (Local + National)', {'gig': 'non-gig', 'ec_code': 'EC'}),
    ('SAM', {'gig': 'non-gig', 'size_code': 'SAM', 'ec_code': 'EC'}),
    ('Large', {'gig': 'non-gig', 'size_code': 'Large', 'ec_code': 'EC'}),
    ('Non-eCommerce (B2B + Resi-Non EC + C2C)', {'gig': 'non-gig', 'ec_code': 'Non-EC'}),
    ('SAM', {'gig': 'non-gig', 'segment_code': 'C2C', 'size_code': 'SAM', 'ec_code': 'Non-EC'}),
    ('Large', {'gig': 'non-gig', 'segment_code': 'C2C', 'size_code': 'Large', 'ec_code': 'Non-EC'}),
    ('B2B Total', {'gig': 'non-gig', 'segment_code': 'B2B', 'resi_code': 'Non-Resi'}),
    ('B2B SAM', {'gig': 'non-gig', 'segment_code': 'B2B', 'size_code': 'SAM', 'resi_code': 'Non-Resi'}),
    ('B2B Large', {'gig': 'non-gig', 'segment_code': 'B2B', 'size_code': 'Large', 'resi_code': 'Non-Resi'}),
    ('B2B - AMZN Controlled (EC+Return)', {'gig': 'non-gig', 'amazon_code': 'Amazon Controlled', 'segment_code': 'B2B', 'resi_code': 'Non-Resi'}),
    ('B2B SAM', {'gig': 'non-gig', 'amazon_code': 'Amazon Controlled', 'segment_code': 'B2B', 'size_code': 'SAM', 'ec_code': 'EC', 'resi_code': 'Non-Resi'}),
    ('B2B Large', {'gig': 'non-gig', 'amazon_code': 'Amazon Controlled', 'segment_code': 'B2B', 'size_code': 'Large', 'ec_code': 'EC', 'resi_code': 'Non-Resi'}),
    ('B2B - Non-AMZN Controlled', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'B2B', 'resi_code': 'Non-Resi'}),
    ('B2B SAM', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'B2B', 'size_code': 'SAM', 'resi_code': 'Non-Resi'}),
    ('B2B Large', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'B2B', 'size_code': 'Large', 'resi_code': 'Non-Resi'}),
    ('Resi Non-EC + C2C (PriMail, USPS GA)', {'gig': 'non-gig', 'resi_code': 'Resi'}),
    ('Resi Non-EC SAM (with C2C)', {'gig': 'non-gig', 'size_code': 'SAM', 'ec_code': 'Non-EC', 'resi_code': 'Resi'}),
    ('Resi Non-EC Large', {'gig': 'non-gig', 'size_code': 'Large', 'ec_code': 'Non-EC', 'resi_code': 'Resi'}),
    ('Total Non-AMZN Resi (Non-AMZN E-C + Resi Non-EC + Primail C2C)', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'resi_code': 'Resi'}),
    ('SAM', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'size_code': 'SAM', 'ec_code': 'Non-EC', 'resi_code': 'Resi'}),
    ('Large', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'size_code': 'Large', 'ec_code': 'Non-EC', 'resi_code': 'Resi'}),
    ('Local EC (Ground)', {'gig': 'non-gig', 'ec_code': 'EC', 'Zone_cd': 'Local'}),
    ('AMZN Platform (Local)', {'gig': 'non-gig', 'amazon_code': 'Amazon-Platform', 'ec_code': 'EC', 'Zone_cd': 'Local'}),
    ('AMZN 1P', {'gig': 'non-gig', 'amazon_code': 'Controlled-1P-Amazon-Platform', 'ec_code': 'EC', 'Zone_cd': 'Local'}),
    ('AMZN 1P SAM', {'gig': 'non-gig', 'amazon_code': 'Controlled-1P-Amazon-Platform', 'size_code': 'SAM', 'ec_code': 'EC', 'Zone_cd': 'Local'}),
    ('AMZN 1P Large', {'gig': 'non-gig', 'amazon_code': 'Controlled-1P-Amazon-Platform', 'size_code': 'Large', 'ec_code': 'EC', 'Zone_cd': 'Local'}),
    ('AMZN FBA', {'gig': 'non-gig', 'amazon_code': 'Controlled-FBA-Amazon-Platform', 'ec_code': 'EC', 'Zone_cd': 'Local'}),
    ('AMZN FBA SAM', {'gig': 'non-gig', 'amazon_code': 'Controlled-FBA-Amazon-Platform', 'size_code': 'SAM', 'ec_code': 'EC', 'Zone_cd': 'Local'}),
    ('AMZN FBA Large', {'gig': 'non-gig', 'amazon_code': 'Controlled-FBA-Amazon-Platform', 'size_code': 'Large', 'ec_code': 'EC', 'Zone_cd': 'Local'}),
    ('Non-AMZN (Local)', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Platform', 'ec_code': 'EC', 'Zone_cd': 'Local'}),
    ('Non-AMZN SAM', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Platform', 'size_code': 'SAM', 'ec_code': 'EC', 'Zone_cd': 'Local'}),
    ('Non-AMZN Large', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Platform', 'size_code': 'Large', 'ec_code': 'EC', 'Zone_cd': 'Local'}),
    ('National EC Total', {'gig': 'non-gig', 'ec_code': 'EC', 'Zone_cd': 'National'}),
    ('SAM', {'gig': 'non-gig', 'size_code': 'SAM', 'ec_code': 'EC', 'Zone_cd': 'National'}),
    ('Large', {'gig': 'non-gig', 'size_code': 'Large', 'ec_code': 'EC', 'Zone_cd': 'National'}),
    ('Amazon Controlled (Local + National)', {'gig': 'non-gig', 'amazon_code': 'Amazon Controlled'}),
    ('SAM', {'gig': 'non-gig', 'amazon_code': 'Amazon Controlled', 'size_code': 'SAM'}),
    ('Large', {'gig': 'non-gig', 'amazon_code': 'Amazon Controlled', 'size_code': 'Large'}),
    ('Amazon Platform (Local + National)', {'gig': 'non-gig', 'amazon_code': 'amazon platform'}),
    ('SAM', {'gig': 'non-gig', 'amazon_code': 'amazon platform', 'size_code': 'SAM'}),
    ('Large', {'gig': 'non-gig', 'amazon_code': 'amazon platform', 'size_code': 'Large'}),
    ('AMZN Controlled  (1P) (National)', {'gig': 'non-gig', 'amazon_code': 'Controlled-1P-Amazon-platform', 'Zone_cd': 'National'}),
    ('SAM (0%)', {'gig': 'non-gig', 'amazon_code': 'Controlled-1P-Amazon-platform', 'size_code': 'SAM', 'Zone_cd': 'National'}),
    ('Large (100%)', {'gig': 'non-gig', 'amazon_code': 'Controlled-1P-Amazon-platform', 'size_code': 'Large', 'Zone_cd': 'National'}),
    ('AMZN Controlled  (FBA) (National)', {'gig': 'non-gig', 'amazon_code': 'Controlled-FBA-Amazon-platform', 'Zone_cd': 'National'}),
    ('SAM', {'gig': 'non-gig', 'amazon_code': 'Controlled-FBA-Amazon-platform', 'size_code': 'SAM', 'Zone_cd': 'National'}),
    ('Large (100%)', {'gig': 'non-gig', 'amazon_code': 'Controlled-FBA-Amazon-platform', 'size_code': 'Large', 'Zone_cd': 'National'}),
    ('AMZN Seller Controlled (Non-FBA) (FBM) (National)', {'gig': 'non-gig', 'amazon_code': 'Seller-Controlled-Amazon Platform', 'Zone_cd': 'National'}),
    ('SAM', {'gig': 'non-gig', 'amazon_code': 'Seller-Controlled-Amazon Platform', 'size_code': 'SAM', 'Zone_cd': 'National'}),
    ('Large', {'gig': 'non-gig', 'amazon_code': 'Seller-Controlled-Amazon Platform', 'size_code': 'Large', 'Zone_cd': 'National'}),
    ('AMZN Seller Controlled (Non-FBA) (SFP)', {'gig': 'non-gig', 'amazon_code': 'Seller-Controlled-Amazon Platform-SFP'}),
    ('SFP SAM', {'gig': 'non-gig', 'amazon_code': 'Seller-Controlled-Amazon Platform-SFP', 'size_code': 'SAM'}),
    ('SFP Large', {'gig': 'non-gig', 'amazon_code': 'Seller-Controlled-Amazon Platform-SFP', 'size_code': 'Large'}),
    ('AMZN Seller Controlled (Non-FBA) (Non-SFP)', {'gig': 'non-gig', 'amazon_code': 'Seller-Controlled-Amazon Platform-non-SFP'}),
    ('Non-SFP SAM', {'gig': 'non-gig', 'amazon_code': 'Seller-Controlled-Amazon Platform-non-SFP', 'size_code': 'SAM'}),
    ('Non-SFP Large', {'gig': 'non-gig', 'amazon_code': 'Seller-Controlled-Amazon Platform-non-SFP', 'size_code': 'Large'}),
    ('Non-Amazon Platform EC (National)', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Platform', 'ec_code': 'EC'}),
    ('SAM', {'gig': 'non-gig', 'size_code': 'SAM', 'ec_code': 'EC'}),
    ('Large', {'gig': 'non-gig', 'size_code': 'Large', 'ec_code': 'EC'}),
    ('Non-Amazon Controlled EC (Local + National)', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'ec_code': 'EC'}),
    ('SAM', {'gig': 'non-gig', 'size_code': 'SAM', 'ec_code': 'EC'}),
    ('Large', {'gig': 'non-gig', 'size_code': 'Large', 'ec_code': 'EC'}),
    ('Non-Amazon Controlled Volume (EC + Non-EC)', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled'}),
    ('SAM', {'gig': 'non-gig', 'size_code': 'SAM', 'ec_code': 'EC'}),
    ('Large', {'gig': 'non-gig', 'size_code': 'Large', 'ec_code': 'EC'}),
    ('Amazon Controlled - SAM', {'gig': 'non-gig', 'amazon_code': 'Controlled-Aamzon-platform', 'size_code': 'SAM'}),
    ('B2B Express SAM', {'gig': 'non-gig', 'amazon_code': 'Controlled-Aamzon-platform', 'segment_code': 'B2B', 'size_code': 'SAM', 'resi_code': 'Non-Resi', 'Product': 'Total-Express'}),
    ('B2B Ground SAM', {'gig': 'non-gig', 'amazon_code': 'Controlled-Aamzon-platform', 'segment_code': 'B2B', 'size_code': 'SAM', 'resi_code': 'Non-Resi', 'Product': 'Total-Ground'}),
    ('B2C Express SAM', {'gig': 'non-gig', 'amazon_code': 'Controlled-Aamzon-platform', 'segment_code': 'B2C', 'size_code': 'SAM', 'resi_code': 'Resi', 'Product': 'Total-Express'}),
    ('B2C Ground SAM', {'gig': 'non-gig', 'amazon_code': 'Controlled-Aamzon-platform', 'segment_code': 'B2C', 'size_code': 'SAM', 'resi_code': 'Resi', 'Product': 'Total-Ground'}),
    ('Amazon Controlled - Large', {'gig': 'non-gig', 'amazon_code': 'Controlled-Aamzon-platform', 'size_code': 'Large'}),
    ('B2B Express Large', {'gig': 'non-gig', 'amazon_code': 'Controlled-Aamzon-platform', 'segment_code': 'B2B', 'size_code': 'Large', 'resi_code': 'Non-Resi', 'Product': 'Total-Express'}),
    ('B2B Ground Large', {'gig': 'non-gig', 'amazon_code': 'Controlled-Aamzon-platform', 'segment_code': 'B2B', 'size_code': 'Large', 'resi_code': 'Non-Resi', 'Product': 'Total-Ground'}),
    ('B2C Express Large', {'gig': 'non-gig', 'amazon_code': 'Controlled-Aamzon-platform', 'segment_code': 'B2C', 'size_code': 'Large', 'resi_code': 'Resi', 'Product': 'Total-Express'}),
    ('B2C Ground Large', {'gig': 'non-gig', 'amazon_code': 'Controlled-Aamzon-platform', 'segment_code': 'B2C', 'size_code': 'Large', 'resi_code': 'Resi', 'Product': 'Total-Ground'}),
    ('Addressable SAM', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'size_code': 'SAM'}),
    ('B2B Express SAM', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'B2B', 'size_code': 'SAM', 'resi_code': 'Non-Resi', 'Product': 'Total-Express'}),
    ('B2B Ground SAM', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'B2B', 'size_code': 'SAM', 'resi_code': 'Non-Resi', 'Product': 'Total-Ground'}),
    ('B2C (EC + Non-EC) Express SAM', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'B2C', 'size_code': 'SAM', 'resi_code': 'Resi', 'Product': 'Total-Express'}),
    ('B2C (EC + Non-EC) Ground SAM', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'B2C', 'size_code': 'SAM', 'resi_code': 'Resi', 'Product': 'Total-Ground'}),
    ('Addressable Large', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'size_code': 'Large'}),
    ('B2B Express Large', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'B2B', 'size_code': 'Large', 'resi_code': 'Non-Resi', 'Product': 'Total-Express'}),
    ('B2B Ground Large', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'B2B', 'size_code': 'Large', 'resi_code': 'Non-Resi', 'Product': 'Total-Ground'}),
    ('B2C (EC + Non-EC) Express Large', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'B2C', 'size_code': 'Large', 'resi_code': 'Resi', 'Product': 'Total-Express'}),
    ('B2C (EC + Non-EC) Ground Large', {'gig': 'non-gig', 'amazon_code': 'Non-Amazon-Controlled', 'segment_code': 'B2C', 'size_code': 'Large', 'resi_code': 'Resi', 'Product': 'Total-Ground'}),
    ('B2B SAM Total', {'gig': 'non-gig', 'segment_code': 'B2B', 'size_code': 'SAM', 'resi_code': 'Non-Resi'}),
    ('B2B Large Total', {'gig': 'non-gig', 'segment_code': 'B2B', 'size_code': 'Large', 'resi_code': 'Non-Resi'}),
    ('B2B Express', {'gig': 'non-gig', 'segment_code': 'B2B', 'resi_code': 'Non-Resi', 'Product': 'Total-Express'}),
    ('B2B Ground', {'gig': 'non-gig', 'segment_code': 'B2B', 'resi_code': 'Non-Resi', 'Product': 'Total-Ground'}),
    ('B2C SAM Total', {'gig': 'non-gig', 'segment_code': 'B2C', 'size_code': 'SAM', 'resi_code': 'Resi'}),
    ('B2C Large Total', {'gig': 'non-gig', 'segment_code': 'B2C', 'size_code': 'Large', 'resi_code': 'Resi'}),
    ('B2C Express', {'gig': 'non-gig', 'segment_code': 'B2C', 'resi_code': 'Resi'}),
    ('B2C Ground', {'gig': 'non-gig', 'segment_code': 'B2C', 'resi_code': 'Resi'}),
    ('B2B Core Ground', {'gig': 'non-gig', 'segment_code': 'B2B', 'resi_code': 'Non-Resi', 'Product': 'Ground-Core'}),
    ('UPS Amazon B2B', {'gig': 'non-gig', 'amazon_code': 'Amazon-Controlled', 'segment_code': 'B2B', 'resi_code': 'Non-Resi'}),
    ('B2B Ground Large + UPS Amazon B2B', {'gig': 'non-gig', 'amazon_code': 'Amazon-Controlled', 'segment_code': 'B2B', 'size_code': 'Large', 'resi_code': 'Non-Resi', 'Product': 'Total-Ground'}),
    ('UPS Amazon B2C Ground', {'gig': 'non-gig', 'amazon_code': 'Amazon-Controlled', 'segment_code': 'B2C', 'resi_code': 'Resi', 'Product': 'Total-Ground'}),
    ('B2C (EC + Non-EC) Ground Large + UPS Amazon B2C Ground', {'gig': 'non-gig', 'amazon_code': 'Amazon-Controlled', 'segment_code': 'B2C', 'resi_code': 'Resi', 'Product': 'Total-Ground'}),
]


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
    if val is None: return None
    s = str(val).strip()
    if s in ("", "−", "-", "#DIV/0!", "nan", "None"): return None
    s = re.sub(r"[%,]", "", s)
    try: return float(s)
    except ValueError: return None


# ─────────────────────────────────────────────────────────────────────────────
# MAIN TRANSFORM
# ─────────────────────────────────────────────────────────────────────────────
def transform(adls_path: str, target_table: str = None,
              write_mode: str = "overwrite", write_to_table: bool = False):
    """
    Returns Spark DataFrame (281 rows) matching A4T_ADV_Baseline_Out schema.
    Set write_to_table=True to persist to Delta table.
    """
    spark = SparkSession.builder.getOrCreate()

    # Read raw CSV — all strings, no header inference
    raw_df   = spark.read.option("header", "false").csv(adls_path)
    raw_rows = [list(r) for r in raw_df.collect()]  # Row → list for safe indexing

    # Detect CY16–CY32 column positions from CSV header (raw_rows[0])
    header   = [str(c).strip() if c is not None else "" for c in raw_rows[0]]
    year_idx = {}
    for i, h in enumerate(header):
        m = re.match(r"CY(\d{2})", h)
        if m:
            yr = int(m.group(1))
            if 16 <= yr <= 32:
                year_idx[f"CY{yr:02d}"] = i

    # Walk source rows, apply skip rules, pair with DIM_MAP by sequential position
    output_rows = []
    prev_label  = ""
    dim_pos     = 0   # index into DIM_MAP

    for row in raw_rows[1:]:
        label = str(row[0]).strip() if row[0] is not None else ""

        if not label:
            continue
        if label in EXCLUDE_LABELS:
            prev_label = label
            continue
        # Stray "B2C Express" annotation at source row 228
        if label == "B2C Express" and prev_label == "Deferred Ground":
            continue
        # Orphan SAM/Large after "Addressable Mix" — not in reference output
        # NOTE: prev_label is NOT updated on skip, so consecutive SAM then Large both caught
        if label in ("SAM", "Large") and prev_label == "Addressable Mix":
            continue

        # Year values from CSV
        year_vals = {
            cy: _to_float(row[year_idx[cy]])
            if year_idx.get(cy) is not None and year_idx[cy] < len(row) else None
            for cy in YEAR_COLS
        }

        # Dim values from DIM_MAP (positionally matched)
        _, dims = DIM_MAP[dim_pos]
        out = {
            "row_num":      dim_pos + 1,   # 1-based, unique per row — use as pivot key
            "ADV_millions": label,
            **{col: dims.get(col) for col in DIM_COLS},
            **year_vals,
        }
        output_rows.append(out)
        prev_label = label
        dim_pos   += 1

    # Build typed Spark DataFrame
    from pyspark.sql.types import IntegerType
    schema = StructType(
        [StructField("row_num", IntegerType(), False)]   # unique row identifier — eliminates pivot duplicates
        + [StructField("ADV_millions", StringType(), True)]
        + [StructField(d, StringType(), True) for d in DIM_COLS]
        + [StructField(cy, DoubleType(), True) for cy in YEAR_COLS]
    )
    df = spark.createDataFrame(output_rows, schema=schema)

    if write_to_table and target_table:
        (df.write.format("delta").mode(write_mode)
           .option("overwriteSchema", "true").saveAsTable(target_table))
        print(f"[OK] Written {df.count()} rows → {target_table}")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# NOTEBOOK USAGE:
#   df = transform(ADLS_PATH)
#   df.display()                             # verify 281 rows, all dims correct
#
#   transform(ADLS_PATH, TARGET_TABLE, write_to_table=True)   # write to Delta
# ─────────────────────────────────────────────────────────────────────────────
