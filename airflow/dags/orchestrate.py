import os
import time
from datetime import datetime

from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    RunLifeCycleState,
    RunResultState,
)
import pendulum

@dag(
    dag_id="orchestrate",
    schedule="0 11 * * *",
    start_date=pendulum.datetime(year=2026, month=7, day=11, tz="UTC"),
    catchup=False,
)
def orchestrate():

    @task
    def ingest_cdc():
        host = os.getenv("DATABRICKS_HOST")
        token = os.getenv("DATABRICKS_TOKEN")
        job_id = int(os.getenv("DATABRICKS_JOB_ID", "845638635241389"))

        if not host or not token:
            raise RuntimeError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")

        ws = WorkspaceClient(host=host, token=token)

        job_trigger = ws.jobs.run_now(job_id=job_id)

        while True:
            job_status = ws.jobs.get_run(run_id=job_trigger.run_id)

            print(
                f"Job status: {job_status.state.life_cycle_state}, "
                f"Result state: {job_status.state.result_state}"
            )

            if job_status.state.life_cycle_state in [
                RunLifeCycleState.TERMINATED,
                RunLifeCycleState.SKIPPED,
                RunLifeCycleState.INTERNAL_ERROR,
            ]:

                if job_status.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully.")
                    return "Success"

                raise Exception(
                    f"Databricks job failed with state: {job_status.state.result_state}"
                )

            time.sleep(10)

    @task.bash
    def clean_target():
        return (
            "rm -rf /opt/airflow/walmart_dbt_project/target && "
            "rm -rf /opt/airflow/walmart_dbt_project/logs"
        )

    @task.bash
    def source_freshness():
        return "cd /opt/airflow/walmart_dbt_project && dbt source freshness"

    silver_technical = BashOperator(
        task_id="silver_technical",
        cwd="/opt/airflow/walmart_dbt_project",
        bash_command="dbt run --select silver_t",
    )

    silver_technical_test = BashOperator(
        task_id="silver_technical_test",
        cwd="/opt/airflow/walmart_dbt_project",
        bash_command="dbt test --select silver_t",
    )

    silver_business = BashOperator(
        task_id="silver_business",
        cwd="/opt/airflow/walmart_dbt_project",
        bash_command="dbt run --select silver_b",
    )

    silver_business_tests = BashOperator(
        task_id="silver_business_tests",
        cwd="/opt/airflow/walmart_dbt_project",
        bash_command="dbt test --select silver_b",
    )

    gold_ephemeral = BashOperator(
        task_id="gold_ephemeral",
        cwd="/opt/airflow/walmart_dbt_project",
        bash_command="dbt run --select gold/ephemeral",
    )

    gold_dimensional = BashOperator(
        task_id="gold_dimensional",
        cwd="/opt/airflow/walmart_dbt_project",
        bash_command="dbt snapshot",
    )

    gold_facts = BashOperator(
        task_id="gold_facts",
        cwd="/opt/airflow/walmart_dbt_project",
        bash_command="dbt run --select gold/facts",
    )

    (
        ingest_cdc()
        >> clean_target()
        >> source_freshness()
        >> silver_technical
        >> silver_technical_test
        >> silver_business
        >> silver_business_tests
        >> gold_ephemeral
        >> gold_dimensional
        >> gold_facts
    )


orchestrate_dag = orchestrate()