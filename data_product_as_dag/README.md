# Data Product as a DAG: Sales Marketing Pipeline at Astrafy

This folder showcases a comprehensive example of a real-world data product implemented at Astrafy. The DAG orchestrates an ETL pipeline that ingests, transforms, and ultimately prepares data for analytical purposes, leveraging various modern data engineering tools including Airbyte, Kubernetes, and dbt.

## Pipeline Overview
The DAG, titled "sales_marketing-k8s-dag-{ENV}", orchestrates the following steps to process the "Sales Marketing" data product:

1. **Data Ingestion:** Data is ingested into BigQuery using Airbyte.
2. **Data Transformation:** Data is processed using dbt in Kubernetes pods.
3. **Data Quality Checks:** dbt tests are conducted to ensure the integrity of the transformed data.
4. **Reporting:** Generates a report using the dbt models' results and uploads it to a Google Cloud Storage bucket.

Each component plays a crucial role in ensuring the data is accurate, timely, and ready for downstream consumption.

## Detailed Step-by-Step Breakdown

### Data Ingestion with Airbyte
Data is synchronized from various sources into BigQuery via Airbyte, an open-source data integration platform. The AirbyteTriggerSyncOperator is configured to perform this synchronization, ensuring data freshness and availability for transformation.

**Example Code Snippet:**
```python
airbyte_sync = AirbyteTriggerSyncOperator(
    task_id='airbyte_sync',
    airbyte_conn_id='airbyte_default',
    connection_id=airbyte_conn_id,
    asynchronous=False,
    timeout=3600,
    wait_seconds=3,
)
```

### Data Transformation with dbt in Kubernetes
Data is then transformed using dbt models. These models are orchestrated to run inside Kubernetes pods, allowing for scalable and isolated execution environments.

**Example Code Snippet:**
```python
with TaskGroup(group_id="Transformations") as Transformations:
    dbt_run_stg = KubernetesPodOperator(
        **dbt_run_defaults,
        task_id='dbt_run_stg',
        name='dbt_run_stg',
        arguments=["{{ ti.xcom_pull(task_ids='get_dbt_mode_parameter', key='return_value')[0] }}"],
    )
```

### Data Quality Checks
Following transformations, dbt tests are executed to ensure data quality. This step is critical for validating the transformations and ensuring that the data meets all defined quality standards.

**Example Code Snippet:**
```python
run_test = f"""
    dbt test {dbt_defaults.dbt_args};exit $?;
"""

dbt_run_test = KubernetesPodOperator(
    **dbt_run_defaults,
    task_id='dbt_run_test',
    name='dbt_run_test',
    arguments=[run_test],
)
```

### Reporting and Outputs
Finally, a report is generated using Elementary package and uploaded to a Google Cloud Storage bucket. This step involves running a custom script within a Kubernetes pod to handle the report generation and upload.

**Example Code Snippet:**
```python
edr_report_command = f"""
    export DBT_PROFILES_DIR=/app/artifacts
    edr send-report --gcs-bucket-name dbt_elementary_astrafy_{data_product}_{ENV} 
"""

edr_report = KubernetesPodOperator(
    **dbt_run_defaults,
    task_id='upload_edr_report',
    name='upload_edr_report',
    arguments=[edr_report_command],
)
```

This README provides a clear and comprehensive guide to implementing and understanding the Sales Marketing data product DAG, designed to facilitate sophisticated data workflows and generate actionable insights through automated processes.