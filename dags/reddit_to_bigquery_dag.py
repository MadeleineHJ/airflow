"""
Reddit r/dataengineering → BigQuery DAG
----------------------------------------
Task 1 – scrape_reddit:
  Runs the Scrapy spider via subprocess and saves posts to a temp JSON file.

Task 2 – load_to_bigquery:
  Reads the JSON and streams rows into BigQuery, creating the table if needed.

Setup (one-time):
  1. Airflow Variables (Admin → Variables):
       GCP_PROJECT_ID  — your GCP project ID
       BQ_DATASET      — target dataset  (default: raw_reddit)
       BQ_TABLE        — target table    (default: dataengineering_posts)

  2. Airflow Connection (Admin → Connections):
       Conn Id:   google_cloud_default
       Conn Type: Google Cloud
       Keyfile JSON or leave empty to use Application Default Credentials (ADC)
"""

import json
import os
import subprocess
import tempfile

from airflow.sdk import dag, task
from pendulum import datetime

SPIDER_PATH = os.path.join(
    os.path.dirname(__file__), "..", "include", "spiders", "reddit_spider.py"
)


@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "data-team", "retries": 2},
    tags=["reddit", "bigquery"],
)
def reddit_to_bigquery():

    @task
    def scrape_reddit() -> str:
        output_file = tempfile.mktemp(suffix=".json")
        result = subprocess.run(
            ["scrapy", "runspider", SPIDER_PATH, "-o", output_file],
            capture_output=True,
            text=True,
            timeout=600,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Scrapy spider failed:\n{result.stderr}")
        return output_file

    @task
    def load_to_bigquery(output_file: str) -> int:
        from airflow.models import Variable
        from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
        from google.cloud import bigquery

        project_id = Variable.get("GCP_PROJECT_ID")
        dataset = Variable.get("BQ_DATASET", default_var="raw_reddit")
        table = Variable.get("BQ_TABLE", default_var="dataengineering_posts")
        table_ref = f"{project_id}.{dataset}.{table}"

        with open(output_file) as f:
            posts = json.load(f)

        if not posts:
            print("No posts scraped — nothing to load.")
            return 0

        hook = GoogleBaseHook(gcp_conn_id="google_cloud_default")
        credentials = hook.get_credentials()
        client = bigquery.Client(project=project_id, credentials=credentials)

        schema = [
            bigquery.SchemaField("post_id", "STRING"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("author", "STRING"),
            bigquery.SchemaField("flair", "STRING"),
            bigquery.SchemaField("score", "INTEGER"),
            bigquery.SchemaField("upvote_ratio", "FLOAT"),
            bigquery.SchemaField("num_comments", "INTEGER"),
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("permalink", "STRING"),
            bigquery.SchemaField("created_utc", "TIMESTAMP"),
            bigquery.SchemaField("scraped_at", "TIMESTAMP"),
        ]

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        )

        job = client.load_table_from_json(posts, table_ref, job_config=job_config)
        job.result()

        os.unlink(output_file)
        print(f"Loaded {len(posts)} rows into {table_ref}")
        return len(posts)

    posts_file = scrape_reddit()
    load_to_bigquery(posts_file)


reddit_to_bigquery()
