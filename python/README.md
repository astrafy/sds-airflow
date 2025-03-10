This folder focuses on building ETL DAGs using Python operators, ideal for those looking to perform data manipulation tasks in Airflow using a non-decorator approach.

## Overview

Learn to construct DAGs that:

- Utilize **BashOperator** for data extraction.
- Employ **PythonOperator** and **BranchPythonOperator** for data transformation, quality checks, and conditional branching.

## Learning Objectives

**Extraction using BashOperator:** Learn how to perform basic data extraction tasks using shell commands.

**Transformation and Quality Checks using PythonOperator:** Use Python code to transform data and perform quality checks to ensure its integrity.

**Conditional Branching with BranchPythonOperator:** Implement logic-based task routing in your workflows depending on the outcomes of data checks.

## Example Description

The example DAG, etl_dag, consists of several steps that highlight each operator's role in building a robust ETL pipeline:

1. **Extraction:**

    - **BashOperator** is used to simulate the extraction phase, copying data from a hypothetical raw data source to a staging area.
2. **Transformation:**

    - **PythonOperator** handles data transformation, where Python code can manipulate data, such as cleaning or restructuring, typically using libraries like Pandas.
3. **Quality Checks and Branching:**

    - **BranchPythonOperator** performs a quality check, determining the flow of tasks based on data validity. This operator can route to different tasks, exemplifying conditional branching in the workflow.
4. **Loading (Example with KubernetesPodOperator):**

    - While not covered under the specified operators, an example with **KubernetesPodOperator** demonstrates potential integration for loading data into a system, showing how Airflow can interact with Kubernetes for task execution.