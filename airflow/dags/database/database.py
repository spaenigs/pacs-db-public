from typing import List, Union, Dict, Any

import pandas as pd
from airflow.decorators import task
from bson import InvalidDocument
from pydicom import Dataset
from datetime import datetime
from dicom_parser import Header
from pydicom.valuerep import DSfloat, IS, PersonName
from pydicom.multival import MultiValue
from pydicom.uid import UID
from pydicom.sequence import Sequence
from pydicom.tag import BaseTag
from collections import OrderedDict

import dateutil.parser
import logging
import pytz
import json
import requests

import sys
sys.path.append(".")

from utils.misc import mongo_get_collection


def predict_sequence_type(mod, tag, df_predict, url, port):
    """Creates and submits the prediction query to sequence-classification module."""
    if "t1" in df_predict[tag].values[0]:
        return "T1 Imaging"   # workaround until more training data is available
    elif mod not in ["CT", "MR"]:
        return "Other"
    else:
        predict_query = {
            "modality": mod,
            "tag": tag,
            "model_version": -1,  # use current version
            "dataset": df_predict.to_dict(orient="records")}
        predict_response = requests.post(
            f"http://{url}:{port}/predict", json=predict_query)
        predict_response = predict_response.json()
        return predict_response["prediction_dataset"][0]["y"]


def parse_date(date_str):
    """Parse different date and time strings to Date objects."""
    funcs = [
        lambda d: datetime.strptime(d, "%Y%m%d%H%M%S").replace(tzinfo=pytz.UTC),
        lambda d: dateutil.parser.parse(d).replace(tzinfo=pytz.UTC)]
    for f in funcs:
        try:
            return f(date_str)
        except ValueError:
            pass
    raise ValueError(f"{date_str} is an invalid date/time format!")


def parse_tag(v):
    """Parse DICOM tags and store them in a MongoDB-compatible format."""
    if type(v) is bytes:
        return v
    elif type(v) == list:
        return [parse_tag(e) for e in v]
    elif type(v) in [str, int, float]:
        return v
    elif type(v) == DSfloat:
        return float(v)
    elif type(v) == MultiValue:
        return [parse_tag(i) for i in v]
    elif type(v) in [UID, PersonName, BaseTag]:
        return str(v)
    elif type(v) is IS:
        return int(v)
    elif type(v) is Sequence:
        return [parse_tag(i) for i in v]
    elif type(v) is Dataset:
        return parse_tag(Header(v).to_dict(parsed=False))
    elif type(v) is dict:
        _v = {}
        for key, val in v.items():
            _v[key.replace(".", "")] = parse_tag(val)
        return _v
    elif v is None:
        return v
    else:
        logging.error(str(type(v)))
        raise ValueError("Unknown value type!")


def dataset_to_mongo_dict(image_dict):
    """Parse DICOM data and return MongoDB-compatible dictionary."""
    res = {}
    for k, v in image_dict.items():
        if type(v) is bytes:
            continue
        elif k == "":
            continue
        elif k == "Unknown":
            continue
        elif "UserData" in k:
            continue
        elif "." in k:
            k_new = k.replace(".", "")
            res[k_new] = parse_tag(image_dict[k])
        else:
            res[k] = parse_tag(image_dict[k])
    return res


def parse_datasets(datasets: List[Dict[str, Any]]):
    """Transforms the json datasets to dicts."""
    for ds in datasets:
        yield dataset_to_mongo_dict(Header(
            Dataset.from_json(ds, bulk_data_uri_handler=lambda _: None)).to_dict(parsed=False))


@task
def dump_studies(ssr_id, **kwargs):
    """Save studies for ssr_id in database. Uses XCOM to get studies from upstream task."""
    studies_collection = mongo_get_collection(
        "studies",
        user=kwargs["MONGODB_USER"], password=kwargs["MONGODB_PASSWORD"],
        url=kwargs["DEPLOYMENT_URL"], port=kwargs["MONGODB_PORT"], db=kwargs["MONGODB_DATABASE_NAME"])
    task_instance = kwargs["ti"]
    studies = task_instance.xcom_pull(task_ids=f"filter_studies")
    logging.info(f"Writing {len(studies)} studies to database.")
    for parsed_study in parse_datasets(studies):
        parsed_study["_SSRID"] = ssr_id
        d = parsed_study["StudyDate"][:8] + parsed_study["StudyTime"][:6]
        try:
            parsed_study["_StudyTimeExact"] = parse_date(d)
        except ValueError:
            logging.error(f"{d} is an invalid study date/time format! "
                          f"({parsed_study['AccessionNumber']})")
        studies_collection.insert_one(parsed_study)


@task
def dump_studies_all(ssr_id, studies, **kwargs):
    """Save studies for ssr_id in database. Uses returned values from upstream task."""
    studies_collection = mongo_get_collection(
        "studies",
        user=kwargs["MONGODB_USER"], password=kwargs["MONGODB_PASSWORD"],
        url=kwargs["DEPLOYMENT_URL"], port=kwargs["MONGODB_PORT"], db=kwargs["MONGODB_DATABASE_NAME"])
    logging.info(f"Writing {len(studies)} studies to database.")
    for parsed_study in parse_datasets(studies):
        parsed_study["_SSRID"] = ssr_id
        d = parsed_study["StudyDate"][:8] + parsed_study["StudyTime"][:6]
        try:
            parsed_study["_StudyTimeExact"] = parse_date(d)
        except ValueError:
            logging.error(f"{d} is an invalid study date/time format! "
                          f"({parsed_study['AccessionNumber']})")
        studies_collection.insert_one(parsed_study)


@task
def dump_series(ssr_id, **kwargs):
    """Save series for ssr_id in database. Uses XCOM to get studies from upstream task."""
    series_collection = mongo_get_collection(
        "series",
        user=kwargs["MONGODB_USER"], password=kwargs["MONGODB_PASSWORD"],
        url=kwargs["DEPLOYMENT_URL"], port=kwargs["MONGODB_PORT"], db=kwargs["MONGODB_DATABASE_NAME"])
    task_instance = kwargs["ti"]
    series = task_instance.xcom_pull(task_ids=f"filter_series")
    logging.info(f"Writing {len(series)} series to database.")
    for parsed_series in parse_datasets(series):
        d = parsed_series["StudyDate"][:8] + parsed_series["StudyTime"][:6]
        try:
            parsed_series["_StudyTimeExact"] = parse_date(d)
        except ValueError:
            logging.error(f"{d} is an invalid study date/time format! "
                          f"({parsed_series['AccessionNumber']})")
        if "SeriesDate" in parsed_series and "SeriesTime" in parsed_series:
            d = parsed_series["SeriesDate"][:8] + parsed_series["SeriesTime"][:6]
            try:
                parsed_series["_SeriesTimeExact"] = parse_date(d)
            except ValueError:
                logging.error(f"{d} is an invalid series date/time format! "
                              f"({parsed_series['AccessionNumber']})")
        parsed_series["_SSRID"] = ssr_id
        series_collection.insert_one(parsed_series)


@task
def dump_series_all(ssr_id, series, **kwargs):
    """Save series for ssr_id in database. Uses returned values from upstream task."""
    series_collection = mongo_get_collection(
        "series",
        user=kwargs["MONGODB_USER"], password=kwargs["MONGODB_PASSWORD"],
        url=kwargs["DEPLOYMENT_URL"], port=kwargs["MONGODB_PORT"], db=kwargs["MONGODB_DATABASE_NAME"])
    logging.info(f"Writing {len(series)} series to database.")
    for parsed_series in parse_datasets(series):
        d = parsed_series["StudyDate"][:8] + parsed_series["StudyTime"][:6]
        try:
            parsed_series["_StudyTimeExact"] = parse_date(d)
        except ValueError:
            logging.error(f"{d} is an invalid study date/time format! "
                          f"({parsed_series['AccessionNumber']})")
        if "SeriesDate" in parsed_series and "SeriesTime" in parsed_series:
            d = parsed_series["SeriesDate"][:8] + parsed_series["SeriesTime"][:6]
            try:
                parsed_series["_SeriesTimeExact"] = parse_date(d)
            except ValueError:
                logging.error(f"{d} is an invalid series date/time format! "
                              f"({parsed_series['AccessionNumber']})")
        parsed_series["_SSRID"] = ssr_id
        series_collection.insert_one(parsed_series)


@task
def dump_images(ssr_id, **kwargs):
    """Save images for ssr_id in database. Uses XCOM to get images from upstream task."""
    instances_collection = mongo_get_collection(
        "instances",
        user=kwargs["MONGODB_USER"], password=kwargs["MONGODB_PASSWORD"],
        url=kwargs["DEPLOYMENT_URL"], port=kwargs["MONGODB_PORT"], db=kwargs["MONGODB_DATABASE_NAME"])
    task_instance = kwargs["ti"]
    images = task_instance.xcom_pull(task_ids=f"filter_images")
    logging.info(f"Writing {len(images)} images to database.")
    for parsed_image in parse_datasets(images):
        parsed_image["_SSRID"] = ssr_id
        if "StudyDate" in parsed_image and "StudyTime" in parsed_image:
            d = parsed_image["StudyDate"][:8] + parsed_image["StudyTime"][:6]
            try:
                parsed_image["_StudyTimeExact"] = parse_date(d)
            except ValueError:
                logging.error(f"{d} is an invalid study date/time format! "
                              f"({parsed_image['AccessionNumber']})")
        if "SeriesDate" in parsed_image and "SeriesTime" in parsed_image:
            d = parsed_image["SeriesDate"][:8] + parsed_image["SeriesTime"][:6]
            try:
                parsed_image["_SeriesTimeExact"] = parse_date(d)
            except ValueError:
                logging.error(f"{d} is an invalid series date/time format! "
                              f"({parsed_image['AccessionNumber']})")
        if "AcquisitionDate" in parsed_image and "AcquisitionTime" in parsed_image:
            d = parsed_image["AcquisitionDate"][:8] + parsed_image["AcquisitionTime"][:6]
            try:
                parsed_image["_AcquisitionTimeExact"] = parse_date(d)
            except ValueError:
                logging.error(f"{d} is an invalid acquisition date/time format! "
                              f"({parsed_image['AccessionNumber']})")
        if "Modality" in parsed_image and "SeriesDescription" in parsed_image:
            df_predict = pd.DataFrame(
                {"SeriesDescription": [parsed_image["SeriesDescription"]]})
            parsed_image["_SequenceType"] = predict_sequence_type(
                mod=parsed_image["Modality"], tag="SeriesDescription",
                df_predict=df_predict, url=kwargs["DEPLOYMENT_URL"],
                port=kwargs["SEQUENCE_CLASSIFICATION_PORT"])
            logging.info(f"{parsed_image['SeriesDescription']} --> "
                         f"{parsed_image['_SequenceType']}")
        else:
            parsed_image["_SequenceType"] = "Other"
        odict = OrderedDict(sorted(
            [(k, v) for k, v in parsed_image.items()], key=lambda t: t[0]))
        if "Volumes_info" in odict:
            del odict["Volumes_info"]
        instances_collection.insert_one(odict)


@task
def dump_images_all(ssr_id, images, **kwargs):
    """Save images for ssr_id in database. Uses returned values from upstream task."""
    instances_collection = mongo_get_collection(
        "instances",
        user=kwargs["MONGODB_USER"], password=kwargs["MONGODB_PASSWORD"],
        url=kwargs["DEPLOYMENT_URL"], port=kwargs["MONGODB_PORT"], db=kwargs["MONGODB_DATABASE_NAME"])
    logging.info(f"Writing {len(images)} images to database.")
    for parsed_image in parse_datasets(images):
        parsed_image["_SSRID"] = ssr_id
        if "StudyDate" in parsed_image and "StudyTime" in parsed_image:
            d = parsed_image["StudyDate"][:8] + parsed_image["StudyTime"][:6]
            try:
                parsed_image["_StudyTimeExact"] = parse_date(d)
            except ValueError:
                logging.error(f"{d} is an invalid study date/time format! "
                              f"({parsed_image['AccessionNumber']})")
        if "SeriesDate" in parsed_image and "SeriesTime" in parsed_image:
            d = parsed_image["SeriesDate"][:8] + parsed_image["SeriesTime"][:6]
            try:
                parsed_image["_SeriesTimeExact"] = parse_date(d)
            except ValueError:
                logging.error(f"{d} is an invalid series date/time format! "
                              f"({parsed_image['AccessionNumber']})")
        if "AcquisitionDate" in parsed_image and "AcquisitionTime" in parsed_image:
            d = parsed_image["AcquisitionDate"][:8] + parsed_image["AcquisitionTime"][:6]
            try:
                parsed_image["_AcquisitionTimeExact"] = parse_date(d)
            except ValueError:
                logging.error(f"{d} is an invalid acquisition date/time format! "
                              f"({parsed_image['AccessionNumber']})")
        if "Modality" in parsed_image and "SeriesDescription" in parsed_image:
            df_predict = pd.DataFrame(
                {"SeriesDescription": [parsed_image["SeriesDescription"]]})
            parsed_image["_SequenceType"] = predict_sequence_type(
                mod=parsed_image["Modality"], tag="SeriesDescription",
                df_predict=df_predict, url=kwargs["DEPLOYMENT_URL"],
                port=kwargs["SEQUENCE_CLASSIFICATION_PORT"])
            logging.info(f"{parsed_image['SeriesDescription']} --> "
                         f"{parsed_image['_SequenceType']}")
        else:
            parsed_image["_SequenceType"] = "Other"
        odict = OrderedDict(sorted(
            [(k, v) for k, v in parsed_image.items()], key=lambda t: t[0]))
        if "Volumes_info" in odict:
            del odict["Volumes_info"]
        try:
            instances_collection.insert_one(odict)
        except InvalidDocument:
            raise RuntimeError("dot in k",
                               odict["AccessionNumber"], odict["StudyInstanceUID"],
                               odict["SeriesInstanceUID"], odict["SOPInstanceUID"])


@task
def dump_failed(ssr_id, **kwargs):
    """Save failed image retrievals for ssr_id in database. Uses XCOM to get data from upstream task."""
    errors_collection = mongo_get_collection(
        "errors",
        user=kwargs["MONGODB_USER"], password=kwargs["MONGODB_PASSWORD"],
        url=kwargs["DEPLOYMENT_URL"], port=kwargs["MONGODB_PORT"], db=kwargs["MONGODB_DATABASE_NAME"])
    task_instance = kwargs["ti"]
    failed_queries = task_instance.xcom_pull(task_ids=f"failed_images_final")
    logging.info(f"Writing {len(failed_queries)} failed queries to database.")
    for parsed_dataset in parse_datasets(failed_queries):
        if not parsed_dataset:
            continue
        parsed_dataset["_SSRID"] = ssr_id
        errors_collection.insert_one(parsed_dataset)


@task
def dump_failed_all(ssr_id, failed_queries, **kwargs):
    """Save failed image retrievals for ssr_id in database. Uses returned values from upstream task."""
    errors_collection = mongo_get_collection(
        "errors",
        user=kwargs["MONGODB_USER"], password=kwargs["MONGODB_PASSWORD"],
        url=kwargs["DEPLOYMENT_URL"], port=kwargs["MONGODB_PORT"], db=kwargs["MONGODB_DATABASE_NAME"])
    logging.info(f"Writing {len(failed_queries)} failed queries to database.")
    for parsed_dataset in parse_datasets(failed_queries):
        if not parsed_dataset:
            continue
        parsed_dataset["_SSRID"] = ssr_id
        errors_collection.insert_one(parsed_dataset)


@task
def dump_tests(**kwargs):
    """Save integration tests for ssr_id in database. Uses XCOM to get data from upstream task."""
    tests_collection = mongo_get_collection(
        "tests",
        user=kwargs["MONGODB_USER"], password=kwargs["MONGODB_PASSWORD"],
        url=kwargs["DEPLOYMENT_URL"], port=kwargs["MONGODB_PORT"], db=kwargs["MONGODB_DATABASE_NAME"])
    task_instance = kwargs["ti"]
    external_imaging = task_instance.xcom_pull(task_ids=f"test_external_imaging")
    first_internal_imaging = task_instance.xcom_pull(task_ids=f"test_first_internal_imaging")
    second_internal_imaging = task_instance.xcom_pull(task_ids=f"test_second_internal_imaging")
    door_to_image_time = task_instance.xcom_pull(task_ids=f"test_door_to_image_time")
    logging.info(external_imaging)
    logging.info(first_internal_imaging)
    logging.info(second_internal_imaging)
    logging.info(door_to_image_time)
    res = {**json.loads(external_imaging), **json.loads(first_internal_imaging),
           **json.loads(second_internal_imaging), **json.loads(door_to_image_time)}
    tests_collection.insert_one(res)


@task
def dump_tests_all(
        external_imaging, first_internal_imaging, second_internal_imaging,
        door_to_image_time, **kwargs):
    """Save integration tests for ssr_id in database. Uses returned values from upstream task."""
    tests_collection = mongo_get_collection(
        "tests",
        user=kwargs["MONGODB_USER"], password=kwargs["MONGODB_PASSWORD"],
        url=kwargs["DEPLOYMENT_URL"], port=kwargs["MONGODB_PORT"], db=kwargs["MONGODB_DATABASE_NAME"])
    logging.info(external_imaging)
    logging.info(first_internal_imaging)
    logging.info(second_internal_imaging)
    logging.info(door_to_image_time)
    res = {**json.loads(external_imaging), **json.loads(first_internal_imaging),
           **json.loads(second_internal_imaging), **json.loads(door_to_image_time)}
    tests_collection.insert_one(res)
