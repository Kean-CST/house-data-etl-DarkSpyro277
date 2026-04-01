"""
House Sale Data ETL Pipeline
============================
Implement the three functions below to complete the ETL pipeline.

Steps:
  1. EXTRACT  – load the CSV into a PySpark DataFrame
  2. TRANSFORM – split the data by neighborhood and save each as a separate CSV
  3. LOAD      – insert each neighborhood DataFrame into its own PostgreSQL table
"""
from __future__ import annotations

import csv  # noqa: F401
import os  # KEEP ONLY ONCE HERE
import glob
import shutil
import re
from pathlib import Path

from dotenv import load_dotenv  # noqa: F401
from pyspark.sql import DataFrame, SparkSession  # noqa: F401
from pyspark.sql import functions as F  # noqa: F401

ROOT = Path(__file__).resolve().parent.parent

NEIGHBORHOODS = [
    "Downtown", "Green Valley", "Hillcrest", "Lakeside", "Maple Heights",
    "Oakwood", "Old Town", "Riverside", "Suburban Park", "University District",
]

OUTPUT_DIR = ROOT / "output" / "by_neighborhood"
OUTPUT_FILES = {hood: OUTPUT_DIR / f"{hood.replace(' ', '_').lower()}.csv" for hood in NEIGHBORHOODS}

PG_TABLES = {hood: f"public.{hood.replace(' ', '_').lower()}" for hood in NEIGHBORHOODS}

PG_COLUMN_SCHEMA = (
    "house_id TEXT, neighborhood TEXT, price INTEGER, square_feet INTEGER, "
    "num_bedrooms INTEGER, num_bathrooms INTEGER, house_age INTEGER, "
    "garage_spaces INTEGER, lot_size_acres NUMERIC(6,2), has_pool BOOLEAN, "
    "recently_renovated BOOLEAN, energy_rating TEXT, location_score INTEGER, "
    "school_rating INTEGER, crime_rate INTEGER, "
    "distance_downtown_miles NUMERIC(6,2), sale_date DATE, days_on_market INTEGER"
)

# ── FUNCTIONS ──────────────────────────────────────

def extract(spark: SparkSession, csv_path: str) -> DataFrame:
    """Load the CSV dataset into a PySpark DataFrame with correct data types.""" 
    df = spark.read.csv(csv_path, header=True, inferSchema=True)
    df = df.withColumn("sale_date", F.to_date(F.col("sale_date"), "M/d/yy"))
    return df


def transform(df: DataFrame) -> dict[str, DataFrame]:
    """Split the data by neighborhood."""

    partitions: dict[str, DataFrame] = {}

    for hood in NEIGHBORHOODS:
        hood_df = df.filter(F.col("neighborhood") == hood).orderBy("house_id")
        partitions[hood] = hood_df

    return partitions


def load(partitions: dict[str, DataFrame], jdbc_url: str, pg_props: dict) -> None:
    """Insert each neighborhood dataset into PostgreSQL and save CSV files."""

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    clean = re.sub(r"\([^)]+\)", "", PG_COLUMN_SCHEMA)
    pg_cols = [c.strip().split()[0] for c in clean.split(",")]

    for hood, sdf in partitions.items():
        safe_name = hood.replace(" ", "_").lower()

        # ── Save to PostgreSQL ──
        table_name = PG_TABLES[hood]
        pg_df = sdf.select(*pg_cols)

        pg_df.write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode="overwrite",
            properties=pg_props
        )


        bool_cols = [
            "has_pool",
            "recently_renovated",
            "has_children",
            "first_time_buyer",
        ]

        for col in bool_cols:
            sdf = sdf.withColumn(
                col,
                F.when(F.col(col), F.lit("True")).otherwise(F.lit("False"))
            )


        # ── Save CSV correctly (single file) ──
        temp_dir = OUTPUT_DIR / f"tmp_{safe_name}"

        sdf.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", True) \
            .csv(str(temp_dir))

        # Find Spark output file
        part_file = glob.glob(f"{temp_dir}/part-*.csv")[0]

        final_path = OUTPUT_FILES[hood]

        # Move to correct filename
        shutil.move(part_file, final_path)

        # Clean up temp folder
        shutil.rmtree(temp_dir)




# ── Main (do not modify) ───────────────────────────────────────────────────────
def main() -> None:
    load_dotenv(ROOT / ".env")

    jdbc_url = (
        f"jdbc:postgresql://{os.getenv('PG_HOST', 'localhost')}:"
        f"{os.getenv('PG_PORT', '5432')}/{os.environ['PG_DATABASE']}"
    )
    pg_props = {
        "user":     os.environ["PG_USER"],
        "password": os.getenv("PG_PASSWORD", ""),
        "driver":   "org.postgresql.Driver",
    }
    csv_path = str(ROOT / os.getenv("DATASET_DIR", "dataset") / os.getenv("DATASET_FILE", "historical_purchases.csv"))

    spark = (
        SparkSession.builder.appName("HouseSaleETL")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df         = extract(spark, csv_path)
    partitions = transform(df)
    load(partitions, jdbc_url, pg_props)

    spark.stop()


if __name__ == "__main__":
    main()
