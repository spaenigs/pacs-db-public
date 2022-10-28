from airflow.decorators import task
from utils.misc import mongo_get_collection

import pandas as pd

import logging


def get_acquisition_state(instance_dict, arrival_time_at_hospital):
    """Gets the acquisition state based on different information."""
    internal, external = "Internal", "External"
    if instance_dict is None:
        return external
    study_time = None
    for tag in ["_AcquisitionTimeExact", "_SeriesTimeExact", "_StudyTimeExact"]:
        if tag in instance_dict:
            if instance_dict[tag] is None:
                continue
            study_time = instance_dict[tag]
            break
    logging.info(f"st: {study_time.tzinfo}; at: {arrival_time_at_hospital.tzinfo}")
    for tag in ["BodyPartExamined", "InstitutionName", "StationName", "InstitutionAddress"]:
        if tag in instance_dict:
            if instance_dict[tag] is None:
                instance_dict[tag] = ""
    if "extern" in instance_dict["BodyPartExamined"].lower():
        return external
    elif "import" in instance_dict["InstitutionName"].lower():
        return external
    elif "import" in instance_dict["StationName"].lower():
        return external
    elif study_time >= arrival_time_at_hospital:
        return internal
    elif study_time < arrival_time_at_hospital:
        return external
    elif "freiburgstrasse" in instance_dict["InstitutionAddress"].lower():
        return internal
    else:
        raise RuntimeWarning("Could not determine acquisition state. Please check manually!")


@task
def acquisition_state(ssr_id, arrival_time_at_hospital, **kwargs):
    """
    Assigns the acquisition state to studies. Baseline imaging would be Internal 1. Follow-up imaging would be 2, 3,
    and so forth. Imaging studies in other hospitals are denoted as External. The latest study before admission
    possesses the highest number.
    """

    config = {"user": kwargs["MONGODB_USER"], "password": kwargs["MONGODB_PASSWORD"],
              "url": kwargs["DEPLOYMENT_URL"], "port": kwargs["MONGODB_PORT"],
              "db": kwargs["MONGODB_DATABASE_NAME"]}

    studies_collection = mongo_get_collection("studies", **config)
    instances_collection = mongo_get_collection("instances", **config)

    instances_cursor = instances_collection.aggregate([
        {'$match': {'_SSRID': ssr_id}},
        {'$group': {
            '_id': '$AccessionNumber',
            'AccessionNumber': {'$first': '$AccessionNumber'},
            '_StudyTimeExact': {'$min': '$_StudyTimeExact'},
            '_SeriesTimeExact': {'$min': '$_SeriesTimeExact'},
            '_AcquisitionTimeExact': {'$min': '$_AcquisitionTimeExact'},
            'StudyInstanceUID': {'$first': '$StudyInstanceUID'},
            'BodyPartExamined': {'$first': '$BodyPartExamined'},
            'InstitutionName': {'$first': '$InstitutionName'},
            'StationName': {'$first': '$StationName'},
            'Modality': {'$first': '$Modality'},
            'InstitutionAddress': {'$first': '$InstitutionAddress'}}}])

    external_imaging, internal_imaging = [], []
    for i in instances_cursor:
        acquisition_state = get_acquisition_state(i, arrival_time_at_hospital)
        i["_AcquisitionState"] = acquisition_state
        if acquisition_state == "External":
            external_imaging.append(i)
        elif acquisition_state == "Internal":
            internal_imaging.append(i)
        else:
            raise ValueError

    sort_order = ["_StudyTimeExact", "_SeriesTimeExact", "_AcquisitionTimeExact"]

    df_internal = pd.DataFrame(internal_imaging)
    df_internal = df_internal if df_internal.empty else \
        df_internal.sort_values(sort_order)
    df_internal["_AcquisitionNumber"] = range(1, df_internal.shape[0] + 1)

    df_external = pd.DataFrame(external_imaging)
    df_external = df_external if df_external.empty else \
        df_external.sort_values(sort_order)
    df_external["_AcquisitionNumber"] = range(1, df_external.shape[0] + 1)

    studies_updated = pd.concat([df_external, df_internal]).to_dict(orient="records")
    for study in studies_updated:
        logging.info(f"Setting {study['AccessionNumber']} as {study['_AcquisitionState']} "
                     f"imaging {study['_AcquisitionNumber']}.")
        studies_collection.update_one(
            {"_SSRID": ssr_id, "StudyInstanceUID": study["StudyInstanceUID"]},
            {"$set": {"_AcquisitionState": study["_AcquisitionState"],
                      "_AcquisitionNumber": study["_AcquisitionNumber"]}})
