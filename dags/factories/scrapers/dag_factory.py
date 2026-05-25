import yaml
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
from docker.types import Mount
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig

# ── Config ────────────────────────────────────────────────
CONFIG_PATH = Path(__file__).parent / "spiders_config.yaml"
with open(CONFIG_PATH) as f:
    config = yaml.safe_load(f)

SA_KEY_PATH      = "C:/Users/Madeleine Hammad/my personal  big query service account/my-project-153-370418-5e6887f9ee58.json"

# paths inside the Airflow container (mounted via docker-compose.override.yml)
DBT_PROJECT_PATH = Path("/usr/local/airflow/dbt/football")
DBT_SA_PATH      = "/usr/local/airflow/dbt/sa.json"


# ── Spider DAG factory ────────────────────────────────────
def make_spider_dag(spider_name: str, pipeline_config: dict) -> DAG:
    """Generate one DAG per spider."""

    default_args = {
        "owner":       "madeleine",
        "retries":     pipeline_config.get("retries", 2),
        "retry_delay": timedelta(minutes=pipeline_config.get("retry_delay_minutes", 5)),
    }

    with DAG(
        dag_id       = f"spider__{spider_name}",
        description  = f"Runs the {spider_name} Scrapy spider",
        schedule     = None,
        start_date   = datetime(2026, 1, 1),
        catchup      = False,
        default_args = default_args,
        tags         = ["scrapers", pipeline_config.get("name", "ungrouped")],
    ) as dag:

        start = EmptyOperator(task_id="start")

        run_spider = DockerOperator(
            task_id       = f"run_{spider_name}",
            image         = config["global_defaults"]["image"],
            command       = f"scrapy crawl {spider_name}",
            auto_remove   = "success",
            docker_url    = "tcp://host.docker.internal:2375",
            network_mode  = "bridge",
            mount_tmp_dir = False,
            force_pull    = True,
            mounts        = [
                Mount(
                    source = SA_KEY_PATH,
                    target = "/tmp/sa.json",
                    type   = "bind",
                )
            ],
            environment   = {
                "GCP_PROJECT_ID":                 "{{ var.value.GCP_PROJECT_ID }}",
                "BQ_DATASET":                     "{{ var.value.BQ_DATASET }}",
                "GOOGLE_APPLICATION_CREDENTIALS": "/tmp/sa.json",
                "FOOTBALL_API_KEY":               "{{ var.value.FOOTBALL_API_KEY }}",
            },
        )

        end = EmptyOperator(task_id="end")

        start >> run_spider >> end

    return dag


# ── dbt DAG factory using Cosmos ─────────────────────────
def make_dbt_dag(pipeline: dict) -> DAG:
    """Generate one dbt DAG per pipeline using Cosmos."""

    pipeline_name = pipeline["name"]
    dbt_selects   = pipeline.get("dbt_selects", [])

    profile_config = ProfileConfig(
        profile_name    = "dbt_project",
        target_name     = "dev",
        profile_mapping = GoogleCloudServiceAccountFileProfileMapping(
            conn_id      = "google_cloud_default",
            profile_args = {
                "project":  "my-project-153-370418",
                "dataset":  "dev_madeleine",
                "location": "US",
                "keyfile":  DBT_SA_PATH,
            },
        ),
    )

    project_config = ProjectConfig(
        dbt_project_path = DBT_PROJECT_PATH,
    )

    execution_config = ExecutionConfig(
        dbt_executable_path = "/usr/local/airflow/.venv/bin/dbt",
    )

    default_args = {
        "owner":       "madeleine",
        "retries":     1,
        "retry_delay": timedelta(minutes=5),
    }

    with DAG(
        dag_id       = f"dbt__{pipeline_name}_pipeline",
        description  = f"Runs dbt models for {pipeline_name} using Cosmos",
        schedule     = pipeline.get("dbt_schedule", "0 6 * * *"),
        start_date   = datetime(2026, 1, 1),
        catchup      = False,
        default_args = default_args,
        tags         = ["dbt", "cosmos", pipeline_name],
    ) as dag:

        start = EmptyOperator(task_id="start")
        end   = EmptyOperator(task_id="end")

        dbt_tasks = DbtTaskGroup(
            group_id         = f"dbt_{pipeline_name}",
            project_config   = project_config,
            profile_config   = profile_config,
            execution_config = execution_config,
            render_config    = RenderConfig(
                select = dbt_selects,
            ),
        )

        start >> dbt_tasks >> end

    return dag


# ── Register all DAGs ─────────────────────────────────────
for pipeline in config["pipelines"]:
    # one spider DAG per spider
    for spider_name in pipeline["spiders"]:
        dag_id = f"spider__{spider_name}"
        globals()[dag_id] = make_spider_dag(spider_name, pipeline)

    # one dbt DAG per pipeline (only if dbt_selects defined)
    if pipeline.get("dbt_selects"):
        dag_id = f"dbt__{pipeline['name']}_pipeline"
        globals()[dag_id] = make_dbt_dag(pipeline)
