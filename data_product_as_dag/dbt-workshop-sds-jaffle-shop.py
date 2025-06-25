
"""
---
author:

- name: Andrea Bombino
- email: andrea@astrafy.io
- updated on: 08-03-2025

---
HERE YOU CAN SET THE DOCUMENTATION FOR USERS 
IN ORDER TO EXPLAIN THE DAG IS DOING
"""
# IMPORT ASTRAFY AIRFLOW LIBRARY
from astrafy_airflow_utils.dbt import dbt,dbt_run_and_upload_cmd
from astrafy_airflow_utils.dag_utils import default_dag
from astrafy_airflow_utils.k8s_utils import gke_bash,node_affinity
from astrafy_airflow_utils.astrafy_environment import AstrafyEnvironment
from astrafy_airflow_utils.pub_sub import notification_job_for_table

# IMPORT NEEDED FOR THE DAG
from typing import Dict
from datetime import timedelta
import random
import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import bigquery

from faker import Faker

fake = Faker()

# Constants
environments = ["dev", "prd"]
environment_configs: Dict[str, Dict[str, str]] = {
    "dev": {
        "project": "prj-data-sds-lz-dev-4542",
        "dataset": "bqdts_company_lz"
    },
    "prd": {
        "project": "prj-data-sds-lz-prd-411a",
        "dataset": "bqdts_company_lz"
    }
    }
 

MAIN_COURSE_PROJECT="prj-astrafy-main-courses"
PACKAGE_NAME = "jaffle_shop"
COMPANY_NAME = "sds"
COMPANY_NAME = COMPANY_NAME.replace("_", "-").strip()

VERSION = "1.0.0" 

# Global configurations
VARIABLE_NAME=f"{PACKAGE_NAME.upper()}_{COMPANY_NAME.upper()}_VERSION"
DP_TRAINING_VERSION = Variable.get(VARIABLE_NAME.strip())
IMAGE_DOCKER=f"europe-west1-docker.pkg.dev/{MAIN_COURSE_PROJECT}/dbt-training/{COMPANY_NAME}/{PACKAGE_NAME}:{DP_TRAINING_VERSION}"


#BQUtility
def fetch_max_dates(bq_project,bq_dataset):
    """Fetch the max order_date and tweet_at from BigQuery."""
    client = bigquery.Client()

    # Fetch max date for orders
    max_order_query = f"SELECT MAX(ordered_at) as max_order_date FROM `{bq_project}.{bq_dataset}.orders`"
    max_order_result = client.query(max_order_query).result()
    max_order_date = list(max_order_result)[0].max_order_date

    # Fetch max date for tweets
    max_tweet_query = f"SELECT MAX(tweeted_at) as max_tweet_date FROM `{bq_project}.{bq_dataset}.tweets`"
    max_tweet_result = client.query(max_tweet_query).result()
    max_tweet_date = list(max_tweet_result)[0].max_tweet_date

    return max_order_date, max_tweet_date

def fetch_existing_data(bq_project,bq_dataset):
    """Fetch the list of existing customers, stores, and users from BigQuery."""
    client = bigquery.Client()

    # Fetch existing customers
    customers_query = f"SELECT id FROM `{bq_project}.{bq_dataset}.customers`"
    customers_result = client.query(customers_query).result()
    existing_customers = [row.id for row in customers_result]

    # Fetch existing stores
    stores_query = f"SELECT id FROM `{bq_project}.{bq_dataset}.stores`"
    stores_result = client.query(stores_query).result()
    existing_stores = [row.id for row in stores_result]

    # Fetch existing users
    users_query = f"SELECT user_id FROM `{bq_project}.{bq_dataset}.tweets`"  # Assuming a users table
    users_result = client.query(users_query).result()
    existing_users = [row.user_id for row in users_result]

    return existing_customers, existing_stores, existing_users

def generate_new_data(max_order_date, max_tweet_date, existing_customers, existing_stores, existing_users):
    """Generate new records for orders and tweets."""
    new_orders = []
    new_tweets = []

    if isinstance(max_order_date, str):
        # Adjust the format if needed; this example assumes ISO format.
        max_order_date = datetime.datetime.fromisoformat(max_order_date)
    
    if isinstance(max_tweet_date, str):
        max_tweet_date = datetime.datetime.fromisoformat(max_tweet_date)
    
    for _ in range(100):
        # Generate a new order
        customer_id = random.choice(existing_customers)
        store_id = random.choice(existing_stores)
        new_order_date = max_order_date + datetime.timedelta(days=random.randint(1, 30))
        new_orders.append({
            'id': fake.uuid4(),
            'customer': customer_id,
            'ordered_at': new_order_date.isoformat(), 
            'store_id': store_id,
            'subtotal': random.randint(10, 500),
            'tax_paid': random.randint(5, 25),
            'order_total': random.randint(15, 525)
        })

    for _ in range(500):
        # Generate a new tweet
        user_id = random.choice(existing_users)
        new_tweet_date = max_tweet_date + datetime.timedelta(days=random.randint(1, 30))
        new_tweets.append({
            'id': fake.uuid4(),
            'user_id': user_id,
            'tweeted_at': new_tweet_date.isoformat(),
            'content': fake.text(max_nb_chars=280)
        })
    
    return new_orders, new_tweets

def insert_into_bigquery(new_orders, new_tweets,bq_project,bq_dataset):
    """Insert the generated records into BigQuery."""
    # Assuming the BigQuery client is already initialized
    client = bigquery.Client()

    # Insert orders into the orders table
    orders_table = client.get_table(f"{bq_project}.{bq_dataset}.orders")
    errors = client.insert_rows_json(orders_table, new_orders)
    if errors:
        print("Error inserting orders:", errors)

    # Insert tweets into the tweets table
    tweets_table = client.get_table(f"{bq_project}.{bq_dataset}.tweets")
    errors = client.insert_rows_json(tweets_table, new_tweets)
    if errors:
        print("Error inserting tweets:", errors)


for environment in environments:
        
        environment_astrafy = AstrafyEnvironment("sds")
        environment_astrafy.env = environment
        environment_astrafy.bucket_folder_results =  f"dbt-target-{environment}-sds/dp-sds/"
        
        with DAG(**default_dag(
                    dag_id=f"dbt-workshop-sds-jaffle-shop-{environment}",
                    data_product=f"{COMPANY_NAME}-{PACKAGE_NAME}",
                    ENV=environment,
                    schedule=None,
                    tags=['sds'],
                    default_args = {
                        "owner": "andrea.bombino",
                        "retries": 1,
                        "retry_delay": timedelta(seconds=60),
                    },
            )) as dag:
            full_refresh_arg = (
            "{{'--full-refresh' if dag_run.conf.get('is_full_refresh') else ''}}"
            )
            with TaskGroup(group_id="Ingestion") as Ingestion:
                @task()
                def data_ingestion(bq_project,bq_dataset):
                    print("Fetching max dates for orders and tweets...")
                    max_order_date, max_tweet_date = fetch_max_dates(bq_project,bq_dataset)
                    print(f"Max order date: {max_order_date}")
                    print(f"Max tweet date: {max_tweet_date}")

                    print("Fetching existing data (customers, stores, users)...")
                    existing_customers, existing_stores, existing_users = fetch_existing_data(bq_project,bq_dataset)

                    print("Generating new data...")
                    new_orders, new_tweets = generate_new_data(max_order_date, max_tweet_date, existing_customers, existing_stores, existing_users)

                    print(f"Inserting {len(new_orders)} new orders and {len(new_tweets)} new tweets into BigQuery...")
                    insert_into_bigquery(new_orders, new_tweets,bq_project,bq_dataset)
                data_ingestion(environment_configs.get(environment).get("project"),environment_configs.get(environment).get("dataset"))

            

            with TaskGroup(group_id="Transformations") as Transformations:
                #staging
                dbt_run_stg_jaffle_shop = gke_bash(
                     dag, 
                    "dbt_run_stg_jaffle_shop", 
                    IMAGE_DOCKER,
                    dbt_run_and_upload_cmd(profile_arg="/app/", 
                                            target_arg=f"--target={environment}",
                                            environment=environment_astrafy,
                                            target_path="jaffle_shop",
                                            dbt_command=f"--select tag:stg_jaffle_shop",
                                            other_args=full_refresh_arg),
                    [], 
                    TriggerRule.ONE_SUCCESS,
                    node_affinity(),
                    'dbt-ksa')
                #intermediate
                dbt_run_int_jaffle_shop = gke_bash(
                     dag, 
                    "dbt_run_int_jaffle_shop",
                    IMAGE_DOCKER, 
                    dbt_run_and_upload_cmd(profile_arg="/app/", 
                                           target_arg=f"--target={environment}",
                                           environment=environment_astrafy,
                                           target_path="jaffle_shop",
                                           dbt_command=f"--select tag:int_jaffle_shop",
                                           other_args=full_refresh_arg),
                    [],
                    TriggerRule.ONE_SUCCESS,
                    node_affinity(),
                    'dbt-ksa')
                #mart
                dbt_run_mart_jaffle_shop = gke_bash(
                    dag, 
                    "dbt_run_mart_jaffle_shop",
                    IMAGE_DOCKER, 
                    dbt_run_and_upload_cmd(profile_arg="/app/", 
                                           target_arg=f"--target={environment}",
                                           environment=environment_astrafy,
                                           target_path="jaffle_shop",
                                           dbt_command=f"--select tag:datamart_jaffle_shop",
                                           other_args=full_refresh_arg),
                    [],
                    TriggerRule.ONE_SUCCESS,
                    node_affinity(),
                    'dbt-ksa')
                #snapshot
                dbt_snapshot_jaffle_shop = gke_bash(
                    dag, 
                    "dbt_snapshot_jaffle_shop",
                    IMAGE_DOCKER, 
                    dbt(profile_arg="/app/", 
                        target_arg=f"--target={environment}",
                        environment=environment_astrafy,
                        target_path="jaffle_shop",
                        dbt_command="snapshot",
                        other_args=full_refresh_arg),
                    [],
                    TriggerRule.ONE_SUCCESS,
                    node_affinity(),
                    'dbt-ksa')
                (
                    dbt_run_stg_jaffle_shop
                    >> Label("Run Intermediate tables")
                    >> dbt_run_int_jaffle_shop
                    >> Label("Run Data Mart tables")
                    >> dbt_run_mart_jaffle_shop
                    >> Label("Run Snapshot")
                    >> dbt_snapshot_jaffle_shop
                )

            with TaskGroup(group_id="Distribution") as Distribution:
                publish_message = notification_job_for_table("publish_message",
                                                                    f"Jaffle Shop updated",
                                                                    f"{MAIN_COURSE_PROJECT}",
                                                                    f"topic-{COMPANY_NAME}-training",[])
            (
                Ingestion 
                >> Label("Data ingestion")
                >> Transformations
                >> Label("Notify the downstream application")
                >> Distribution
            )

