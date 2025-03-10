## Getting Started

This subfolder contains examples that help you grasp the basic structure and functionality of DAGs in Airflow. If you are new to data engineering or just starting with Airflow, these examples will guide you through the initial steps of DAG construction.

## What You Will Learn

- **Basic Concepts:** Understand the foundational concepts behind a DAG and its role in Airflow.
- **Creating Your First DAG:** Employ simple operators like EmptyOperator to construct your first DAG.
- **Three Methods for Writing DAGs:** Learn two distinct approaches to define your DAGs in Airflow:
    - **Context Manager Method:** Utilizes the with statement to manage the DAG context, simplifying the setup and teardown process.
    - **Direct Assignment Method:** Directly assigns the DAG object to a variable, offering more explicit control over the DAG's lifecycle.
    - **Decorators Method:** Add dag/task decorators directly above the functions.

## Example

Here's a basic example of a DAG defined using the Context Manager method:

```python
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="easy_dag",
    start_date=datetime(2024, 4, 17),
) as dag:

    task1 = EmptyOperator(task_id="say_hello")

    task1 >> EmptyOperator(task_id="goodbye")
```

This DAG initializes with two tasks using EmptyOperator. The "say_hello" task is executed first, followed by "goodbye", showcasing the sequential execution of tasks.