"""@bruin
name: audio_trends.load_raw_data
type: python
@bruin"""
 
# ---------------------------------------------------------
# LOAD RAW DATA: GCS Parquet → BigQuery Tables
#
# This asset takes the parquet files sitting in GCS (uploaded
# by our ingestion script) and loads them into BigQuery as
# native tables. We add partitioning and clustering here
# because this is the most efficient place to do it — the
# tables are created with the right structure from the start.
#
# Why not just query the parquet files directly from GCS?
# You can (BigQuery supports external tables), but native
# tables are faster to query, support partitioning/clustering,
# and cost less per query since BigQuery can skip irrelevant
# data partitions.
# ---------------------------------------------------------
 
from google.cloud import bigquery
 
PROJECT = "audio-patterns"
DATASET = "audio_trends"
BUCKET = "audio-trends-raw-data"
 
 
def load_gcs_to_bigquery(client, gcs_uri, table_name, partition_field=None, cluster_fields=None):
    """
    Load a single parquet file from GCS into a BigQuery table.
 
    Parameters:
        client: BigQuery client
        gcs_uri: full GCS path (gs://bucket/path/file.parquet)
        table_name: just the table name (e.g. "raw_spotify_charts")
        partition_field: column to partition by (date/timestamp column)
        cluster_fields: list of columns to cluster by
    """
    table_id = f"{PROJECT}.{DATASET}.{table_name}"
 
    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # overwrite if exists
    )
 
    # Add partitioning if specified
    # Partitioning splits the table into segments by date, so queries
    # that filter on date only scan the relevant partitions — much cheaper
    if partition_field:
        job_config.time_partitioning = bigquery.TimePartitioning(
            field=partition_field
        )
 
    # Add clustering if specified
    # Clustering sorts data within each partition by these columns,
    # so queries filtering on these columns skip even more data
    if cluster_fields:
        job_config.clustering_fields = cluster_fields
 
    print(f"  Loading {gcs_uri} → {table_id}")
    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()  # wait for completion
 
    table = client.get_table(table_id)
    print(f"  ✓ Loaded {table.num_rows:,} rows into {table_name}")
 
 
# ---------------------------------------------------------
# Main execution
# ---------------------------------------------------------

client = bigquery.Client(project=PROJECT)

# --- Spotify Charts ---
# No partitioning on raw — dates came through as INT64 in parquet.
# We'll cast and partition properly in the staging layer.
load_gcs_to_bigquery(
    client,
    gcs_uri=f"gs://{BUCKET}/raw/spotify_charts/spotify_charts.parquet",
    table_name="raw_spotify_charts",
)

# --- Podcast Shows (dimension table) ---
load_gcs_to_bigquery(
    client,
    gcs_uri=f"gs://{BUCKET}/raw/podcast_reviews/podcast_shows.parquet",
    table_name="raw_podcast_shows",
)

# --- Podcast Reviews ---
load_gcs_to_bigquery(
    client,
    gcs_uri=f"gs://{BUCKET}/raw/podcast_reviews/podcast_reviews.parquet",
    table_name="raw_podcast_reviews",
)

# --- Podcast Charts ---
load_gcs_to_bigquery(
    client,
    gcs_uri=f"gs://{BUCKET}/raw/podcast_charts/podcast_charts.parquet",
    table_name="raw_podcast_charts",
)

print("\n✓ All raw tables loaded into BigQuery")