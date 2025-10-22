from prefect import flow, task
from data_ingestion import run_data_ingestion
from etl_local import calculate_daily_revenue

@task
def ingest_data_task(project_id: str, bucket: str, color: str = 'yellow', year: int = 2025):
    run_data_ingestion(project_id, bucket, color, year)

@task
def revenue_task(project_id: str, bucket: str, color: str = 'yellow', year: int = 2025):
    daily_revenue, avg_revenue = calculate_daily_revenue(project_id, bucket, color, year)
    print(daily_revenue.head())
    print(f"Average daily revenue: {avg_revenue:.2f}")
    return daily_revenue, avg_revenue

@flow(name="NYC Taxi Data Pipeline")
def nyc_taxi_flow(project_id: str, bucket: str, color: str = 'yellow', year: int = 2025):
    ingest_data_task(project_id, bucket, color, year)
    revenue_task(project_id, bucket, color, year)

if __name__ == "__main__":
    nyc_taxi_flow(
        project_id="mle-project-big-data",
        bucket="mle-batch-real-time-process",
        color="yellow",
        year=2025
    )



### python prefect_pipeline.py

### or to register as Prefect deployment

### prefect deployment build prefect_pipeline.py:main_flow -n green-taxi-pipeline
### prefect deployment apply main_flow-deployment.yaml
### prefect agent start -q "default"
 