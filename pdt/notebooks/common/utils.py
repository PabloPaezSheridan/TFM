from pyspark.sql import functions as F, Window as W
from pyspark.sql.types import *
import datetime as dt

def ensure_catalog_schema(catalog: str, schema: str):
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

def to_date_col(ts_col, fmt="yyyyMMddHHmmss"):
    return F.to_date(F.to_timestamp(ts_col.cast("string"), fmt))

def parse_tone_first(tone_col):
    # V2Tone campo: "tone,polarity,activityRefDensity, ..."; nos quedamos con tone
    return F.when(F.length(tone_col) > 0, F.split(tone_col, ",")[0].cast("double")).otherwise(None)

def replace_for_yfinance(sym: str) -> str:
    # Yahoo usa '-' en vez de '.' (BRK.B -> BRK-B), mantiene '^' como prefijo de índices
    if sym.startswith("^"):
        return sym
    return sym.replace(".", "-")

def normalize_symbol_col(col):
    return F.upper(F.trim(F.regexp_replace(col, r"\s+", "")))