import yaml
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator



# load config
CONFIG_PATH = Path(__file__).parent / "spiders_config.yaml"
with open(CONFIG_PATH) as f:
    config = yaml.safe_load(f)


def make_dag(spider: dict) -> DAG:
    """Generate a DAG for a single spider from config."""

    default_args = {
        "owner":        "madeleine",
        "retries":      spider.get("retries", 2),
        "retry_delay":  timedelta(minutes=spider.get("retry_delay_minutes", 5)),
    }

    with DAG(
        dag_id      = f"spider__{spider['name']}",
        description = f"Runs the {spider['name']} Scrapy spider",
        schedule    = spider.get("schedule", None),
        start_date  = datetime(2026, 1, 1),
        catchup     = False,
        default_args = default_args,
        tags        = ["scrapers", spider.get("group", "ungrouped")],
    ) as dag:

        start = EmptyOperator(task_id="start")

        run_spider = DockerOperator(
            task_id         = f"run_{spider['name']}",
            image           = spider["image"],
            command         = f"scrapy crawl {spider['name']}",
            auto_remove     = "success",
            docker_url      = "tcp://host.docker.internal:2375",
            network_mode    = "bridge",
            environment     = {
                "GCP_PROJECT_ID":                "{{ var.value.GCP_PROJECT_ID }}",
                "BQ_DATASET":                    "{{ var.value.BQ_DATASET }}",
                "GOOGLE_APPLICATION_CREDENTIALS": "/tmp/sa.json",
                "GCP_SA_KEY":     "{{ var.value.GCP_SA_KEY }}",
            },
           
        )

        end = EmptyOperator(task_id="end")

        start >> run_spider >> end

    return dag


# register all DAGs in the global namespace
# Airflow discovers DAGs by scanning module globals
for spider_config in config["spiders"]:
    dag_id = f"spider__{spider_config['name']}"
    globals()[dag_id] = make_dag(spider_config)