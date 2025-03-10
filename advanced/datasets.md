# Leveraging Datasets in Airflow for Dynamic Task Scheduling

Airflow's datasets feature allows for the creation and management of datasets as first-class objects within the Airflow ecosystem. Datasets facilitate dynamic task scheduling based on data availability and can trigger other DAGs when new data arrives.

## How Datasets Enhance Airflow

- **Data-driven Scheduling:** Triggers tasks based on the presence or update of specific data.
- **Inter-DAG Dependencies:** Enables one DAG to trigger another upon data update, without hard-coding dependencies.

## Example of Using Datasets

```python
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import timedelta, datetime
from airflow.models import Variable, Param
from airflow import Dataset
from airflow.decorators import task

dataset_dm_global_sales = Dataset('dm-global-sales')
dataset_dm_global_stocks = Dataset('dm-global-stocks')
    
with DAG(
    dag_id='copy-tables-k8s-dag-prd',
    schedule=[
        dataset_dm_global_sales,
        dataset_dm_global_stocks,
    ],
    description='Copy tables from source to destination',
    catchup=False,
    start_date=datetime(2024, 4, 29),
    default_args={'owner': 'nawfel.bacha', 'retries': 1, 'retry_delay': timedelta(seconds=60), 'on_success_callback':None,'on_failure_callback':on_failure},
) as dag:

    copy_tables = KubernetesPodOperator(
        image_pull_policy="Always",
        is_delete_operator_pod=True,
        namespace = 'jlc-logistics-airflow-prd',
        get_logs = True,
        pod_template_file='/opt/airflow/pod_templates/pod_template_file.yaml',
        image="jfrog-platform.eu.aws.rccad.net/jlcl-docker-local/copy_tables:latest",
        service_account_name='jlc-logistics-airflow-prd-worker',
        cmds=["python", "copy_tables.py"],
        name="copy-tables",
        task_id="copy-tables",
    )
    
    copy_tables
```

This example shows how a DAG is getting triggered by datasets when they are updated.