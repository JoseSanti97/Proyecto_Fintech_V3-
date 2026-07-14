import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg as _avg, count as _count, when, max as _max, ntile, expr
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent
SILVER_ROOT = ROOT / "storage" / "silver"
GOLD_ROOT = ROOT / "storage" / "gold"

