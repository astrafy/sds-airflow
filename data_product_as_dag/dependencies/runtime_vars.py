from os import environ
from airflow.models import Variable
from kubernetes.client import models as k8s


class ConfigVars:

    def __init__(self):
        COMMON_GITLAB_PAT = Variable.get("COMMON_GITLAB_PAT")
        env_vars = [
            set_env_vars(name="STG_PROJECT_ID", key="STG_PROJECT_ID"),
            set_env_vars(name="LZ_PROJECT_ID", key="LZ_PROJECT_ID"),
            set_env_vars(name="DM_PROJECT_ID", key="DM_PROJECT_ID"),
            set_env_vars(name="DW_PROJECT_ID", key="DW_PROJECT_ID"),
            set_env_vars(name="COMMON_GITLAB_PAT", value=COMMON_GITLAB_PAT)
        ]
        self.env_vars = env_vars


class DbtRunDefaults:
    image_pull_policy = "Always"
    cmds = ["/bin/bash", "-c"]

    get_logs = True
    service_account_name = 'dbt-ksa'
    namespace = "dbt"
    affinity = {'nodeAffinity': {'requiredDuringSchedulingIgnoredDuringExecution':
        {'nodeSelectorTerms': [
            {'matchExpressions': [
                {'key': 'node_pool',
                 'operator': 'In',
                 'values': ['dbt']
                 }
            ]
            }
        ]
        }
    }
    }

    def __init__(self, data_product, environment):
        self.dbt_target_gke = f'{data_product}-{environment}-ci-airflow'
        self.dbt_target = f""" --target="{self.dbt_target_gke}" """
        self.run_id = '{{ run_id }}'
        self.dag_ts = '{{ ts }}'
        self.dbt_profile = f""" --profiles-dir=/app/artifacts"""
        self.dbt_vars = f""" --vars '{{"dag_id": "{self.run_id}" , "dag_ts": "{self.dag_ts}", "orchestrator": "Airflow", 
        "job_run_id": "{self.run_id}"}}'"""
        self.dbt_args = f"""{self.dbt_vars}{self.dbt_target}{self.dbt_profile}"""
        self.docker_image = "europe-west1-docker.pkg.dev/prj-astrafy-artifacts/mds"


def cd_to_local_package(package: str) -> str:
    return f"cd /app/data_products/{package}/"


def set_env_vars(name: str, value=None, key=None):
    if value:
        env_var = k8s.V1EnvVar(name=name, value=value)
        return env_var
    if key:
        env_var = k8s.V1EnvVar(name=name, value_from=k8s.V1EnvVarSource(
            config_map_key_ref=k8s.V1ConfigMapKeySelector(name="internal-data-dbt-config-map", key=key)))
        return env_var
