# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Example Airflow DAG that creates a Cloud Dataproc cluster, runs the PySpark
wordcount example, and deletes the cluster.
This DAG relies on three Airflow variables
https://airflow.apache.org/docs/apache-airflow/stable/concepts/variables.html
* gcp_project - Google Cloud Project to use for the Cloud Dataproc cluster.
* gce_region - Google Compute Engine region where Cloud Dataproc cluster should be
  created.
"""

import datetime
import os

from airflow import models
from airflow.providers.google.cloud.operators import dataproc
from airflow.utils import trigger_rule

# Output file for Cloud Dataproc job.
# If you are running Airflow in more than one time zone
# see https://airflow.apache.org/docs/apache-airflow/stable/timezone.html
# for best practices
output_file = os.path.join(
    'gs://jezioro-danych-{{ var.value.gcp_project }}/composer', 'wordcount',
    datetime.datetime.now().strftime('%Y%m%d-%H%M%S')) + os.sep
# Path to PySpark wordcount example available on every Dataproc cluster.
WORDCOUNT_PATH = (
    'gs://jezioro-danych-{{ var.value.gcp_project }}/code/spark/wordcount.py'
)
# Arguments to pass to Cloud Dataproc job.
input_file = 'gs://jezioro-danych-{{ var.value.gcp_project }}/pan-tadeusz.txt'
wordcount_args = [input_file, output_file]

SPARK_JOB = {
    "reference": {"project_id": '{{ var.value.gcp_project }}'},
    "placement": {"cluster_name": 'composer-spark-tutorial-cluster-{{ ds_nodash }}'},
    "pyspark_job": {
        "main_python_file_uri": WORDCOUNT_PATH,
        "args": wordcount_args,
    },
}

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2"
    },
    "software_config": {
        "properties": {
            "dataproc:dataproc.allow.zero.workers": "true"
        }
    }
}

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': '{{ var.value.gcp_project }}',
    'region': '{{ var.value.gce_region }}',

}


with models.DAG(
        'spark_wordcounter',
        # Continue to run DAG once per day
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:


    # Create a Cloud Dataproc cluster.
    create_dataproc_cluster = dataproc.DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        # Give the cluster a unique name by appending the date scheduled.
        # See https://airflow.apache.org/docs/apache-airflow/stable/macros-ref.html
        cluster_name='composer-spark-tutorial-cluster-{{ ds_nodash }}',
        cluster_config=CLUSTER_CONFIG,
        region='{{ var.value.gce_region }}'
    )

    # Run the Spark wordcount example installed on the Cloud Dataproc cluster
    # master node.
    run_dataproc_spark = dataproc.DataprocSubmitJobOperator(
        task_id='run_dataproc_spark',
        job=SPARK_JOB)

    # Delete Cloud Dataproc cluster.
    delete_dataproc_cluster = dataproc.DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        cluster_name='composer-spark-tutorial-cluster-{{ ds_nodash }}',
        region='{{ var.value.gce_region }}',
        # Setting trigger_rule to ALL_DONE causes the cluster to be deleted
        # even if the Dataproc job fails.
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    # Define DAG dependencies.
    create_dataproc_cluster >> run_dataproc_spark >> delete_dataproc_cluster
    
