from airflow_client.client import ApiClient, Configuration
from airflow_client.client.api import dag_api, dag_run_api
from airflow_client.client.model.dag import DAG

from airflow.dags.utils.misc import mongo_get_collection
from datetime import datetime
from dotenv import dotenv_values

import airflow_client.client.exceptions

import pandas as pd

import time

""" 
This script allows the manual execution/triggering of Airflow DAGs. See airflow/dags/query_pacs_dags.py for
the actual definition of the workflows.
"""

config = {k: v for k, v in dotenv_values().items()}

ssr_collection = mongo_get_collection(
    "swiss_stroke_registry",
    user=config["MONGODB_USER"], password=config["MONGODB_PASSWORD"],
    url=config["DEPLOYMENT_URL"], port=config["MONGODB_PORT"], db=config["MONGODB_DATABASE_NAME"])

ssr_cursor = ssr_collection\
    .find({}, {"_id": 0})\
    .sort([("_SSRID", 1)])
df = pd.DataFrame(ssr_cursor)
df.index = df["_SSRID"]

df = df.iloc[:500, :]

list_of_IDs = df["_SSRID"]

configuration = Configuration(
    host=f"http://{config['DEPLOYMENT_URL']}:{config['AIRFLOW_WEBSERVER_PORT']}/api/v1",
    username='airflow',
    password='airflow'
)

studies_collection = mongo_get_collection(
    "studies",
    user=config["MONGODB_USER"], password=config["MONGODB_PASSWORD"],
    url=config["DEPLOYMENT_URL"], port=config["MONGODB_PORT"], db=config["MONGODB_DATABASE_NAME"])

with ApiClient(configuration) as api_client:
    
    dag_run_api_instance = dag_run_api.DAGRunApi(api_client)
    dag_api_instance = dag_api.DAGApi(api_client)

    for _id in list_of_IDs:

        # check if already in database
        if studies_collection.count_documents({"_SSRID": _id}) > 0:
            continue

        dag_id = "query_pacs_for_" + _id
        print(dag_id)
        
        with ApiClient(configuration) as api_client:            
            try:
                dag = DAG(is_paused=False) 
                api_response = dag_api_instance.patch_dag(dag_id, dag, update_mask=["is_paused"])
                print(api_response)
            except airflow_client.client.exceptions.NotFoundException:
                print(f"DAG {dag_id} not found!")
                continue
            
        time.sleep(10)
        
        # wait until success or failed
        initial_state = dag_run_api_instance.get_dag_run(
            dag_id, "scheduled__2022-06-17T00:00:00+00:00")["state"].value
        print(f"Initial state: {initial_state}")
        while initial_state == "running":
            print(f"{str(datetime.now())}: sleeping for 3 minutes...")
            time.sleep(3*50)
            current_state = dag_run_api_instance.get_dag_run(
                dag_id, "scheduled__2022-06-17T00:00:00+00:00")["state"].value
            print(f"Current state: {current_state}")
            initial_state = current_state
