import yaml
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
from docker.types import Mount

# ── Config ────────────────────────────────────────────────
CONFIG_PATH = Path(__file__).parent / "spiders_config.yaml"
with open(CONFIG_PATH) as f:
    config = yaml.safe_load(f)

DBT_PROJECT_PATH = "C:/Users/Madeleine Hammad/madeleine-portfolio/data_transformation"
DBT_EXECUTABLE   = "C:/Users/Madeleine Hammad/AppData/Local/Programs/Python/Python313/Scripts/dbt.exe"
DBT_PROFILE      = "dbt_project"
DBT_TARGET       = "dev"

SA_KEY_PATH      = "C:/Users/Madeleine Hammad/my personal  big query service account/my-project-153-370418-5e6887f9ee58.json"

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


# ── dbt DAG factory ───────────────────────────────────────
def make_dbt_dag(pipeline: dict) -> DAG:
    """Generate one dbt DAG per pipeline that has dbt_selects."""

    pipeline_name = pipeline["name"]
    dbt_selects   = pipeline.get("dbt_selects", [])

    default_args = {
        "owner":       "madeleine",
        "retries":     1,
        "retry_delay": timedelta(minutes=5),
    }

    with DAG(
        dag_id       = f"dbt__{pipeline_name}_pipeline",
        description  = f"Runs dbt models for {pipeline_name}",
        schedule     = pipeline.get("dbt_schedule", "0 6 * * *"),
        start_date   = datetime(2026, 1, 1),
        catchup      = False,
        default_args = default_args,
        tags         = ["dbt", pipeline_name],
    ) as dag:

        start = EmptyOperator(task_id="start")
        end   = EmptyOperator(task_id="end")

        previous_task = start

        for select in dbt_selects:
            # sanitise select for use as task_id
            task_name = select.replace(".", "__").replace("/", "_")

            dbt_run = BashOperator(
                task_id      = f"dbt_run__{task_name}",
                bash_command = (
                    f"{DBT_EXECUTABLE} run "
                    f"--project-dir {DBT_PROJECT_PATH} "
                    f"--profiles-dir {DBT_PROJECT_PATH} "
                    f"--profile {DBT_PROFILE} "
                    f"--target {DBT_TARGET} "
                    f"--select {select}"
                ),
            )

            dbt_test = BashOperator(
                task_id      = f"dbt_test__{task_name}",
                bash_command = (
                    f"{DBT_EXECUTABLE} test "
                    f"--project-dir {DBT_PROJECT_PATH} "
                    f"--profiles-dir {DBT_PROJECT_PATH} "
                    f"--profile {DBT_PROFILE} "
                    f"--target {DBT_TARGET} "
                    f"--select {select}"
                ),
            )

            previous_task >> dbt_run >> dbt_test
            previous_task = dbt_test

        previous_task >> end

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