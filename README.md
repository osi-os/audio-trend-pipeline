# audio-trend-data-project

Exploring content consumption patterns across music and podcasts on streaming platforms.

Analyzing the audio ecosystem from three vantage points across 2022-2025. Podcast reviews capture historical listener sentiment (2022-2023), Spotify charts show music consumption trends (late 2023-2025), and podcast charts reveal spoken-word content trends (late 2024-2025). Where these datasets overlap, we can directly compare music and podcast consumption patterns and correlate music genres with podcast categories.

This pipeline uses batch processing to ingest data from Kaggle, load it into a GCS data lake, and transform it through staging and mart layers in BigQuery using Bruin as the orchestrator. The pipeline is designed to run on a daily schedule.

## Architecture

```
Kaggle Datasets
      |
  [INGEST] scripts/ingest.py
      |  (download, filter 2022-2025, convert to Parquet)
      v
Google Cloud Storage (audio-trends-raw-data)
      |
  [LOAD] load_raw_data.py
      |  (GCS -> BigQuery via load jobs)
      v
BigQuery raw_* tables
      |
  [STAGING] stg_*.sql (Bruin assets)
      |  (type casting, cleaning, data quality checks)
      v
BigQuery stg_* tables
      |
  [MARTS] fct_*.sql (Bruin assets)
      |  (aggregation, business logic, cross-dataset joins)
      v
BigQuery fct_* tables
      |
  [DASHBOARD] streamlit_app.py
      v
Interactive Streamlit App
```

## Data Sources

All datasets are sourced from Kaggle and filtered to a 2022-2025 window:

| Dataset | Coverage | Key Fields |
|---------|----------|------------|
| Spotify Charts (73 Countries) | Oct 2023 - Jun 2025 | track, artist, daily rank, audio features (danceability, energy, valence, etc.), country |
| Spotify Charts Historical | 2022 - 2023 | track, artist, artist_genres, streams, country |
| Podcast Reviews | Jan 2022 - Mar 2023 | podcast, rating (1-5), review text, author |
| Podcast Shows | Jan 2022 - Mar 2023 | podcast title, author, category, average rating |
| Spotify Podcast Charts | Sep 2024 - Dec 2025 | show, episode, rank, region, duration, is_explicit |

### Analysis Windows

- **Correlation analysis** (Jan 2022 - Mar 2023): Historical Spotify charts + podcast reviews overlap, enabling genre-to-category correlation
- **Ecosystem comparison** (Oct 2024 - Jun 2025): Current Spotify charts + podcast charts overlap, enabling direct music vs. podcast trend comparison

## Mart Tables

| Table | Description |
|-------|-------------|
| `fct_music_trends` | Monthly music metrics by country (unique tracks/artists, avg audio features, explicit ratio) |
| `fct_podcast_trends` | Monthly podcast metrics by category (review volume, ratings, chart entries) |
| `fct_music_podcast_correlation` | Music genre signals correlated with podcast category metrics (15-month overlap) |
| `fct_audio_ecosystem` | Unified timeline combining both analysis windows with music-to-podcast ratios |

## Dashboard

The Streamlit app visualizes:

- Podcast categories ranked by total ratings and average rating
- Music speechiness trends over time
- Audio feature radar charts by country (73 countries available)
- Music genre vs. podcast review volume (dual-axis, 15-month correlation window)

## Tech Stack

- **Orchestration**: [Bruin](https://github.com/bruin-data/bruin) (SQL + Python asset pipeline)
- **Warehouse**: Google BigQuery
- **Storage**: Google Cloud Storage (Parquet files)
- **Infrastructure**: Terraform
- **Dashboard**: Streamlit + Plotly
- **Language**: Python, SQL

## Prerequisites

- Python 3.9+
- A Google Cloud Platform project with BigQuery and GCS enabled
- A GCP service account with BigQuery Admin and Storage Admin roles
- [Kaggle API credentials](https://www.kaggle.com/docs/api) (`~/.kaggle/kaggle.json`)
- [Bruin CLI](https://github.com/bruin-data/bruin) installed
- [Terraform](https://developer.hashicorp.com/terraform/install) installed

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/<your-username>/audio-trend-pipeline.git
cd audio-trend-pipeline
```

### 2. Install Python dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install kaggle pyarrow google-cloud-storage
```

### 3. Provision GCP infrastructure

Create a `terraform/terraform.tfvars` file:

```hcl
project         = "your-gcp-project-id"
credentials     = "/path/to/your/service-account-key.json"
gcs_bucket_name = "your-bucket-name"
bq_dataset_name = "audio_trends"
```

Then apply:

```bash
cd terraform
terraform init
terraform apply
cd ..
```

### 4. Configure credentials

Set the environment variable for GCP authentication:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"
```

Create a `.bruin.yml` file at the project root:

```yaml
default_environment: default
environments:
  default:
    connections:
      google_cloud_platform:
        - name: "bigquery"
          project_id: "your-gcp-project-id"
          service_account_file: "/path/to/your/service-account-key.json"
```

### 5. Ingest data

```bash
python scripts/ingest.py
```

This downloads datasets from Kaggle, filters to 2022-2025, converts to Parquet, and uploads to your GCS bucket. Update the `GCS_BUCKET_NAME` constant in `scripts/ingest.py` to match your bucket name.

### 6. Run the pipeline

```bash
bruin run pipelines/audio_trends/pipeline.yml
```

This loads raw data from GCS into BigQuery, then runs all staging and mart transformations.

### 7. Launch the dashboard

For local development, create `.streamlit/secrets.toml` with your service account JSON, then:

```bash
streamlit run streamlit_app.py
```

## Project Structure

```
audio-trend-pipeline/
├── scripts/
│   └── ingest.py                  # Kaggle -> GCS ingestion
├── pipelines/audio_trends/
│   ├── pipeline.yml               # Bruin pipeline definition
│   └── assets/
│       ├── load_raw_data.py       # GCS -> BigQuery loader
│       ├── stg_*.sql              # Staging transformations
│       └── fct_*.sql              # Mart aggregations
├── terraform/
│   ├── main.tf                    # GCS bucket + BigQuery dataset
│   └── variables.tf               # Terraform variables
├── streamlit_app.py               # Dashboard
├── requirements.txt               # Python dependencies
├── .bruin.yml                     # Bruin config (not committed)
└── .gitignore
```
