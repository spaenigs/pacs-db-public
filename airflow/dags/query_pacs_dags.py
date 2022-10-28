from airflow.utils.trigger_rule import TriggerRule
from airflow.models import DAG
from datetime import datetime

from pacs.query_study_level import query_study_level
from pacs.query_series_level import query_series_level
from pacs.query_instance_level import query_instance_level
from pacs.move_images import \
    move_image, move_series, successful_images, failed_images

from database.database import *
from database.acquisition_state import acquisition_state
from database.sanitize_studies import sanitize_studies

from validation.external_imaging import test_external_imaging
from validation.first_internal_imaging import test_first_internal_imaging
from validation.second_internal_imaging import test_second_internal_imaging
from validation.door_to_image_time import test_door_to_image_time

from utils.misc import mongo_get_collection, get_time_frame
from utils.filter import *

import pandas as pd

import pymongo
import os

"""
DAG definition and creation. The file will be automatically parsed from the Airflow worker. All DAGs are configured 
to run once after un-pausing. This is also the reason why the start_date is in the past. 
"""

config = {k: v for k, v in os.environ.items()}

args = {
    'owner': 'Airflow',
    'start_date': datetime(2022, 6, 17)}

ssr_collection = mongo_get_collection(
    "swiss_stroke_registry",
    user=config["MONGODB_USER"],
    password=config["MONGODB_PASSWORD"],
    url=config["DEPLOYMENT_URL"],
    port=config["MONGODB_PORT"],
    db=config["MONGODB_DATABASE_NAME"])

ssr_cursor = ssr_collection.find(
    {},
    {"_id": 0}).sort([("_SSRID", pymongo.ASCENDING)])
df = pd.DataFrame(ssr_cursor)
df.index = df["_SSRID"]

# For performance reasons, only load around 500 cases at a time.
# More will increase the scheduling time between tasks.
# df = df.iloc[:500, :]


@task
def flatten(list_of_list):
    res = []
    for i in list_of_list:
        for j in i:
            res.append(j)
    return res


@task
def query_and_filter_series(patient_id, study_date) -> dict:
    """Combined function to get study and series data. Unrelated studies/series will be skipped."""
    studies = query_study_level.function(
        PatientID=patient_id, StudyDate=study_date, **config)
    filtered_studies = filter_studies.function(studies)
    series = []
    for study in filtered_studies:
        series_ = query_series_level.function(study_dataset=study, **config)
        series.extend(series_)
    filtered_series = filter_series.function(series)
    logging.info(f"Number of filtered studies is {len(filtered_studies)}")
    logging.info(f"Number of filtered series is {len(filtered_series)}")
    if len(filtered_studies) == 0 or len(filtered_series) == 0:
        raise ValueError
    return {"filtered_studies": filtered_studies,
            "filtered_series": filtered_series}


@task
def query_all_instances(series):
    """Query all instance information."""
    instances = []
    for s in series:
        instances_ = query_instance_level.function(series_dataset=s, **config)
        instances.append(instances_)
    logging.info(f"Found {len(instances)} instances.")
    return instances


@task
def move_all_images(instances):
    """Move (download) all reference images."""
    images = []
    for inst in instances:
        img = move_image.function(instance_dataset=inst, **config)
        images.append(img)
    return images


@task
def move_all_series(instances):
    """Move (download) complete series for reference image."""
    images = []
    for inst in instances:
        img = move_series.function(series_dataset=inst, **config)
        images.append(img)
    return images


@task
def dump_results(ssr_id, studies, series, failed_images, successful_images):
    """Save all studies/series/reference images in database. Will raise an error if no data is available."""
    if len(studies) == 0 or len(series) == 0 or len(successful_images) == 0:
        logging.warning(f"Number of studies to dump is {len(studies)}")
        logging.warning(f"Number of series to dump is {len(series)}")
        logging.warning(f"Number of images to dump is {len(successful_images)}")
        raise ValueError
    dump_studies_all.function(ssr_id=ssr_id, studies=studies, **config),
    dump_series_all.function(ssr_id=ssr_id, series=series, **config)
    dump_images_all.function(ssr_id=ssr_id, images=successful_images, **config)
    dump_failed_all.function(ssr_id=ssr_id, failed_queries=failed_images, **config)


@task
def run_all_tests(ssr_id) -> dict:
    """Run all integration tests. Requires manual curated data."""
    external_imaging = test_external_imaging.function(ssr_id, **config)
    first_internal_imaging = test_first_internal_imaging.function(ssr_id, **config)
    second_internal_imaging = test_second_internal_imaging.function(ssr_id, **config)
    door_to_image_time = test_door_to_image_time.function(ssr_id, **config)
    return {
        "external_imaging": external_imaging,
        "first_internal_imaging": first_internal_imaging,
        "second_internal_imaging": second_internal_imaging,
        "door_to_image_time": door_to_image_time
    }


# Iterate over cases, extract PatientID and time range to query for studies and define the DAGs
for idx, (n, s) in enumerate(df.iterrows()):

    # Create DAG and allow at most one running task and run per time.
    # This due to technical limitations of the PACS.
    with DAG(
        dag_id=f'query_pacs_for_{n}', tags=["query_pacs"],
        default_args=args, schedule_interval="@once",
        max_active_tasks=1, max_active_runs=1
    ) as dag:

        patient_id = s.PatientID

        try:
            dates = df.loc[df.PatientID == patient_id, "arrival_time_at_hospital"]
            dates = dates.sort_values()
            start_time, end_time = get_time_frame(s.arrival_time_at_hospital, dates)
            start_time_str = start_time.strftime("%Y%m%d")
            end_time_str = end_time.strftime("%Y%m%d")
        except TypeError:
            # in case an SSR entry is not available
            continue

        # -- Fast pipeline -- #

        res = query_and_filter_series(
            patient_id=patient_id, study_date=f"{start_time_str}-{end_time_str}")
        filtered_studies, filtered_series = res["filtered_studies"], res["filtered_series"]
        instances = query_all_instances.override(show_return_value_in_logs=False)(filtered_series)
        images_1 = move_all_images.override(show_return_value_in_logs=False)(instances)
        images_2 = move_all_series.override(show_return_value_in_logs=False)(failed_images(images_1))
        images = filter_images.override(trigger_rule=TriggerRule.ALL_DONE) \
            (flatten([successful_images(images_1), successful_images(images_2)]))
        failed_i = failed_images.override(task_id="failed_images_final")(images_2)
        get_acquisition_state = \
            acquisition_state(ssr_id=n, arrival_time_at_hospital=s.arrival_time_at_hospital, **config)
        res_tests = dump_results(n, filtered_studies, filtered_series, failed_i, images) \
                    >> get_acquisition_state \
                    >> sanitize_studies(ssr_id=n, **config) \
                    >> run_all_tests(ssr_id=n)
        dump_tests_all(res_tests["external_imaging"], res_tests["first_internal_imaging"],
                       res_tests["second_internal_imaging"], res_tests["door_to_image_time"], **config)

        # -- Exhaustive pipeline -- #

        # studies = query_study_level(
        #     PatientID=patient_id, StudyDate=f"{start_time_str}-{end_time_str}", **config)
        # filtered_studies = filter_studies(studies)
        # series = query_series_level.expand(study_dataset=filtered_studies, **config)
        # filtered_series = filter_series(flatten(series))
        # instances = query_instance_level.override(retries=3).expand(series_dataset=filtered_series, **config)
        # images_1 = move_image.expand(instance_dataset=instances, **config)
        # images_2 = move_series.expand(series_dataset=failed_images(images_1))
        # dump_to_database = [
        #     images >> dump_images.override(trigger_rule=TriggerRule.ALL_SUCCESS)(ssr_id=n, **config),
        #     filtered_studies >> dump_studies.override(trigger_rule=TriggerRule.ALL_SUCCESS)(ssr_id=n, **config),
        #     failed_images.override(task_id="failed_images_final")(images_2) >> dump_failed(ssr_id=n, **config),
        #     filtered_series >> dump_series.override(trigger_rule=TriggerRule.ALL_SUCCESS)(ssr_id=n, **config)
        # ]
        # tests = [
        #     test_external_imaging(n),
        #     test_first_internal_imaging(n),
        #     test_second_internal_imaging(n),
        #     test_door_to_image_time(n)
        #     ]
        # dump_to_database >> get_acquisition_state >> tests >> dump_tests(**config)
        # # get_acquisition_state >> tests >> dump_tests(, **config)

        globals()[n] = dag
