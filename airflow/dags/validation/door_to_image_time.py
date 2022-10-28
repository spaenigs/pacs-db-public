from airflow.decorators import task

import pandas as pd

import logging
import json

from utils.misc import mongo_get_collection, strptime


@task
def test_door_to_image_time(ssr_id, **kwargs):
    """Integration test to compare manual and automatic door to image time assignment."""

    config = {"user": kwargs["MONGODB_USER"], "password": kwargs["MONGODB_PASSWORD"],
              "url": kwargs["DEPLOYMENT_URL"], "port": kwargs["MONGODB_PORT"],
              "db": kwargs["MONGODB_DATABASE_NAME"]}

    ssr_collection = mongo_get_collection("swiss_stroke_registry", **config)
    studies_collection = mongo_get_collection("studies", **config)
    instances_collection = mongo_get_collection("instances", **config)

    s_cursor = ssr_collection.find_one({"_SSRID": ssr_id})
    s = pd.Series(s_cursor)

    s["_FirstInternalImagingTime"] = strptime(
        str(s['First Internal Imaging Date']) + str(s['First Internal Imaging Time']), "%d.%m.%Y%H:%M")

    patient_id = s.PatientID
    db_query = {"PatientID": patient_id, "_SSRID": ssr_id}
    study_cursor = studies_collection.find(db_query)
    df_studies = pd.DataFrame(study_cursor)

    ref_first_int_img = dict()
    try:
        sid = df_studies.loc[
            (df_studies["_AcquisitionState"] == "Internal") &
            (df_studies["_AcquisitionNumber"] == 1),
            "StudyInstanceUID"].to_list()[0]
        for tag in ["_AcquisitionTimeExact", "_SeriesTimeExact", "_StudyTimeExact"]:
            instances = list(instances_collection
                             .find({**db_query, **{"StudyInstanceUID": sid, tag: {"$exists": True}}})
                             .sort(tag, 1))
            if len(instances) > 0:
                ref_first_int_img = instances[0]
                break
    except (KeyError, IndexError, StopIteration):
        pass

    door_to_img_min_ex = float(s.Doortoimagemin)
    
    if not ref_first_int_img:
        match = False
        db = {
            "_AcquisitionTimeExact": None,
            "arrival_time_at_hospital": s.arrival_time_at_hospital.isoformat(),
            "door_to_image_min": None}
    else:
        stime = ref_first_int_img.get("_AcquisitionTimeExact",
                    ref_first_int_img.get("_SeriesTimeExact",
                         ref_first_int_img.get("_StudyTimeExact", None)))
        door_to_img_min_db = \
            (stime - s.arrival_time_at_hospital).total_seconds() / 60
        match = abs(door_to_img_min_db - door_to_img_min_ex) <= 2
        db = {
            "_AcquisitionTimeExact": stime.isoformat(),
            "arrival_time_at_hospital": s.arrival_time_at_hospital.isoformat(),
            "door_to_image_min": door_to_img_min_db}

    if pd.isna(s["_FirstInternalImagingTime"]):
        first_internal_time_str = None
    else:
        first_internal_time_str = s["_FirstInternalImagingTime"].isoformat()

    res = {
        "_SSRID": ssr_id,
        "door_to_image_time": {
            "match": match,
            "db": db,
            "excel": {
                "_FirstInternalImagingTime": first_internal_time_str,
                "arrival_time_at_hospital": s.arrival_time_at_hospital.isoformat(),
                "door_to_image_min": door_to_img_min_ex}}}

    logging.info(f"Door to image time: {match}")

    return json.dumps(res)
