"""
Ingestion Script: Audio Ecosystem Pipeline
===========================================
This script handles the Extract and Load stages of our ELT pipeline:
  1. Download raw datasets from Kaggle
  2. Read and filter to 2022-2025
  3. Convert to Parquet format
  4. Upload to Google Cloud Storage

Usage:
  python ingest.py

Prerequisites:
  - Kaggle API credentials at ~/.kaggle/kaggle.json
  - GOOGLE_APPLICATION_CREDENTIALS env var pointing to your GCP service account key
  - pip install kaggle pandas pyarrow google-cloud-storage
"""

import os
import zipfile
import sqlite3
import pandas as pd
from pathlib import Path
from google.cloud import storage

# ---------------------------------------------------------
# CONFIGURATION
# All the knobs for the script in one place. If you need to
# change the bucket name, date range, or dataset identifiers,
# you only change them here — not scattered through the code.
# ---------------------------------------------------------
GCS_BUCKET_NAME = "audio-trends-raw-data"
START_DATE = "2022-01-01"
END_DATE = "2025-12-31"
TEMP_DIR = Path("./temp_data")  # local staging folder, gets cleaned up

# Kaggle dataset identifiers — these are the "owner/dataset-name"
# strings you see in the Kaggle URL
# format below is "datasetkey": "kaggle_id"
KAGGLE_DATASETS = {
    "spotify_charts": "asaniczka/top-spotify-songs-in-73-countries-daily-updated",
    "podcast_reviews": "thoughtvector/podcastreviews",
    "podcast_charts": "daniilmiheev/top-spotify-podcasts-daily-updated",
}

# Where each dataset will land in your GCS bucket
# e.g. gs://audio-trends-raw-data/raw/spotify_charts/data.parquet
GCS_PREFIXES = {
    "spotify_charts": "raw/spotify_charts",
    "podcast_reviews": "raw/podcast_reviews",
    "podcast_charts": "raw/podcast_charts",
}


# ---------------------------------------------------------
# STAGE 1: DOWNLOAD FROM KAGGLE
#
# The Kaggle API downloads datasets as zip files. We:
#   - Create a temp directory for each dataset
#   - Call the Kaggle API to download and unzip
#   - Return the path so the next stage can read the files
# ---------------------------------------------------------
def download_dataset(dataset_key: str, kaggle_id: str) -> Path:
    """Download and unzip a Kaggle dataset to a local temp folder."""
    dest = TEMP_DIR / dataset_key
    dest.mkdir(parents=True, exist_ok=True)

    print(f"  Downloading {kaggle_id}...")

    # The kaggle CLI command downloads + unzips in one step.
    # We shell out to it because it handles auth automatically
    # from ~/.kaggle/kaggle.json
    os.system(f"kaggle datasets download -d {kaggle_id} -p {dest} --unzip --quiet")

    # Show what files landed
    files = list(dest.iterdir())
    print(f"  Downloaded {len(files)} file(s): {[f.name for f in files]}")
    return dest


# ---------------------------------------------------------
# STAGE 2 & 3: READ, EXPLORE, AND FILTER
#
# Each dataset has a different format and structure, so we
# handle them separately. The key steps for each:
#   - Read the raw file (CSV or SQLite)
#   - Print basic info (shape, date range) so you can verify
#   - Filter to our 2022-2025 date window
#   - Return a clean DataFrame (or dict of DataFrames)
# ---------------------------------------------------------
def process_spotify_charts(data_dir: Path) -> pd.DataFrame:
    """
    Read the Spotify 73 Countries dataset.

    This dataset has one big CSV with columns like:
    spotify_id, name, artists, daily_rank, daily_movement,
    weekly_movement, country, snapshot_date, popularity,
    is_explicit, duration_ms, danceability, energy, etc.

    We filter on snapshot_date to keep only 2022-2025.
    """
    # Find the CSV file — the exact filename may vary
    csv_files = list(data_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")

    print(f"  Reading {csv_files[0].name}...")
    df = pd.read_csv(csv_files[0], low_memory=False)

    print(f"  Raw shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    print(f"  Columns: {list(df.columns)}")

    # Parse the date column and filter
    # snapshot_date is the column that tells us when each chart entry was recorded
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"], errors="coerce")
    df = df.dropna(subset=["snapshot_date"])

    before_count = len(df)
    df = df[(df["snapshot_date"] >= START_DATE) & (df["snapshot_date"] <= END_DATE)]
    print(f"  Filtered to {START_DATE} – {END_DATE}: {len(df):,} rows (dropped {before_count - len(df):,})")

    return df


def process_podcast_reviews(data_dir: Path) -> dict[str, pd.DataFrame]:
    """
    Read the Podcast Reviews dataset.
 
    This dataset contains JSON files:
      - podcasts.json: podcast_id, title, url, etc.
      - reviews.json: podcast_id, author_id, rating, title, content, created_at
      - categories.json: category metadata for podcasts
 
    We return podcasts and reviews as separate DataFrames since
    they'll become separate staging models in Bruin.
 
    We filter reviews on created_at to keep only 2022-2025.
    """
    # Show what files we're working with
    files = list(data_dir.iterdir())
    print(f"  Available files: {[f.name for f in files]}")
 
    # --- Read podcasts ---
    podcasts_file = data_dir / "podcasts.json"
    if not podcasts_file.exists():
        raise FileNotFoundError(f"podcasts.json not found in {data_dir}")
 
    # Podcast JSON files from Kaggle can be large — read line by line
    # (JSON Lines format: one JSON object per line)
    print(f"  Reading podcasts.json...")
    try:
        df_podcasts = pd.read_json(podcasts_file)
    except ValueError:
        # If standard JSON fails, try JSON Lines format
        df_podcasts = pd.read_json(podcasts_file, lines=True)
 
    print(f"  Podcasts table: {df_podcasts.shape[0]:,} rows × {df_podcasts.shape[1]} columns")
    print(f"  Podcast columns: {list(df_podcasts.columns)}")
 
    # --- Read categories (if present) and merge with podcasts ---
    categories_file = data_dir / "categories.json"
    if categories_file.exists():
        print(f"  Reading categories.json...")
        try:
            df_categories = pd.read_json(categories_file)
        except ValueError:
            df_categories = pd.read_json(categories_file, lines=True)
        print(f"  Categories table: {df_categories.shape[0]:,} rows × {df_categories.shape[1]} columns")
        print(f"  Category columns: {list(df_categories.columns)}")
 
        # If categories has a podcast_id, merge it with podcasts
        if "podcast_id" in df_categories.columns and "podcast_id" in df_podcasts.columns:
            df_podcasts = df_podcasts.merge(df_categories, on="podcast_id", how="left")
            print(f"  Merged categories into podcasts: {df_podcasts.shape[0]:,} rows × {df_podcasts.shape[1]} columns")
 
    # --- Read reviews ---
    reviews_file = data_dir / "reviews.json"
    if not reviews_file.exists():
        raise FileNotFoundError(f"reviews.json not found in {data_dir}")
 
    print(f"  Reading reviews.json (this may take a moment — large file)...")
    try:
        df_reviews = pd.read_json(reviews_file)
    except ValueError:
        df_reviews = pd.read_json(reviews_file, lines=True)
 
    print(f"  Reviews table (raw): {df_reviews.shape[0]:,} rows × {df_reviews.shape[1]} columns")
    print(f"  Review columns: {list(df_reviews.columns)}")
 
    # Parse date and filter
    df_reviews["created_at"] = pd.to_datetime(df_reviews["created_at"], errors="coerce")
    df_reviews = df_reviews.dropna(subset=["created_at"])
 
    before_count = len(df_reviews)
    df_reviews = df_reviews[
        (df_reviews["created_at"] >= START_DATE) & (df_reviews["created_at"] <= END_DATE)
    ]
    print(f"  Reviews filtered to {START_DATE} – {END_DATE}: {len(df_reviews):,} rows (dropped {before_count - len(df_reviews):,})")
 
    return {"podcasts": df_podcasts, "reviews": df_reviews}


def process_podcast_charts(data_dir: Path) -> pd.DataFrame:
    """
    Read the Top Spotify Podcast Episodes dataset.

    This is a CSV with daily podcast chart rankings.
    We'll discover the exact columns when we read it,
    then filter on whatever date column exists.
    """
    csv_files = list(data_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")

    print(f"  Reading {csv_files[0].name}...")
    df = pd.read_csv(csv_files[0], low_memory=False)

    print(f"  Raw shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    print(f"  Columns: {list(df.columns)}")

    # Replace dots in column names — BigQuery doesn't allow them
    df.columns = df.columns.str.replace(".", "_", regex=False)
    print(f"  Renamed columns: {list(df.columns)}")

    # Try to find and parse a date column
    # The exact column name may vary — we check common possibilities
    date_col = None
    for candidate in ["snapshot_date", "date", "rank_date", "created_at"]:
        if candidate in df.columns:
            date_col = candidate
            break

    if date_col:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.dropna(subset=[date_col])

        before_count = len(df)
        df = df[(df[date_col] >= START_DATE) & (df[date_col] <= END_DATE)]
        print(f"  Filtered on '{date_col}' to {START_DATE} – {END_DATE}: {len(df):,} rows (dropped {before_count - len(df):,})")
    else:
        print(f"  ⚠ No date column found — keeping all {len(df):,} rows")

    return df


# ---------------------------------------------------------
# STAGE 4: CONVERT TO PARQUET
#
# Parquet is a columnar file format that:
#   - Compresses 5-10x smaller than CSV
#   - Preserves data types (dates, numbers, strings)
#   - Loads into BigQuery much faster
#
# We save each DataFrame as a .parquet file in the temp dir.
# ---------------------------------------------------------
def save_to_parquet(df: pd.DataFrame, name: str) -> Path:
    """Save a DataFrame to a Parquet file and return the path."""
    output_path = TEMP_DIR / f"{name}.parquet"
    df.to_parquet(output_path, index=False, engine="pyarrow")
    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Saved {name}.parquet ({size_mb:.1f} MB, {len(df):,} rows)")
    return output_path


# ---------------------------------------------------------
# STAGE 5: UPLOAD TO GCS
#
# The google-cloud-storage library authenticates using the
# GOOGLE_APPLICATION_CREDENTIALS environment variable you set.
#
# We upload each Parquet file to a specific "folder" in the
# bucket (GCS doesn't actually have folders — it uses key
# prefixes that look like folders, e.g. "raw/spotify_charts/data.parquet")
# ---------------------------------------------------------
def upload_to_gcs(local_path: Path, gcs_prefix: str, filename: str):
    """Upload a local file to GCS."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob_path = f"{gcs_prefix}/{filename}"
    blob = bucket.blob(blob_path)

    print(f"  Uploading to gs://{GCS_BUCKET_NAME}/{blob_path}...")
    blob.upload_from_filename(str(local_path))
    print(f"  ✓ Upload complete")


# ---------------------------------------------------------
# MAIN: Orchestrate all 5 stages
# ---------------------------------------------------------
def main():
    print("=" * 60)
    print("Audio Ecosystem Pipeline — Data Ingestion")
    print("=" * 60)

    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    # --- Spotify Charts ---
    print("\n[1/3] Spotify Charts (73 Countries)")
    print("-" * 40)
    data_dir = download_dataset("spotify_charts", KAGGLE_DATASETS["spotify_charts"])
    df_spotify = process_spotify_charts(data_dir)
    parquet_path = save_to_parquet(df_spotify, "spotify_charts")
    upload_to_gcs(parquet_path, GCS_PREFIXES["spotify_charts"], "spotify_charts.parquet")

    # --- Podcast Reviews ---
    print("\n[2/3] Podcast Reviews")
    print("-" * 40)
    data_dir = download_dataset("podcast_reviews", KAGGLE_DATASETS["podcast_reviews"])
    podcast_dfs = process_podcast_reviews(data_dir)

    # Upload podcasts (dimension table) and reviews (fact table) separately
    parquet_path = save_to_parquet(podcast_dfs["podcasts"], "podcast_shows")
    upload_to_gcs(parquet_path, GCS_PREFIXES["podcast_reviews"], "podcast_shows.parquet")

    parquet_path = save_to_parquet(podcast_dfs["reviews"], "podcast_reviews")
    upload_to_gcs(parquet_path, GCS_PREFIXES["podcast_reviews"], "podcast_reviews.parquet")

    # --- Podcast Charts ---
    print("\n[3/3] Top Spotify Podcast Episodes")
    print("-" * 40)
    data_dir = download_dataset("podcast_charts", KAGGLE_DATASETS["podcast_charts"])
    df_podcast_charts = process_podcast_charts(data_dir)
    parquet_path = save_to_parquet(df_podcast_charts, "podcast_charts")
    upload_to_gcs(parquet_path, GCS_PREFIXES["podcast_charts"], "podcast_charts.parquet")

    # --- Summary ---
    print("\n" + "=" * 60)
    print("✓ Ingestion complete! Files in GCS:")
    print(f"  gs://{GCS_BUCKET_NAME}/raw/spotify_charts/spotify_charts.parquet")
    print(f"  gs://{GCS_BUCKET_NAME}/raw/podcast_reviews/podcast_shows.parquet")
    print(f"  gs://{GCS_BUCKET_NAME}/raw/podcast_reviews/podcast_reviews.parquet")
    print(f"  gs://{GCS_BUCKET_NAME}/raw/podcast_charts/podcast_charts.parquet")
    print("=" * 60)

    # Clean up temp files
    import shutil
    shutil.rmtree(TEMP_DIR, ignore_errors=True)
    print("Temp files cleaned up.")


if __name__ == "__main__":
    main()
