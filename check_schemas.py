"""Quick check of raw table schemas in BigQuery."""
from google.cloud import bigquery

client = bigquery.Client(project="audio-patterns")

tables = [
    "audio_trends.raw_spotify_charts",
    "audio_trends.raw_podcast_shows",
    "audio_trends.raw_podcast_reviews",
    "audio_trends.raw_podcast_charts",
]

for table_name in tables:
    print(f"\n{'=' * 50}")
    print(f"TABLE: {table_name}")
    print(f"{'=' * 50}")
    table = client.get_table(f"audio-patterns.{table_name}")
    for field in table.schema:
        print(f"  {field.name:30s} {field.field_type}")