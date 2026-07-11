

import os
import time

from databricks.sdk import WorkspaceClient

from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

host = os.getenv("DATABRICKS_HOST")
token = os.getenv("DATABRICKS_TOKEN")
job_id = int(os.getenv("DATABRICKS_JOB_ID", "845638635241389"))

if not host or not token:
    raise RuntimeError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")

ws = WorkspaceClient(host=host, token=token)

job_trigger = ws.jobs.run_now(job_id=job_id)

while True:
    job_status=ws.jobs.get_run(run_id=job_trigger.run_id)
    print(f"Job status: {job_status.state.life_cycle_state}, Result state: {job_status.state.result_state}")
    if job_status.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
        if job_status.state.result_state==RunResultState.SUCCESS:
            print("Job completed successfully.")
            break
        else:   
            print(f"Job failed with state: {job_status.state.result_state}")
            break
      # Wait for 10 seconds before checking the status again  
    time.sleep(10)
