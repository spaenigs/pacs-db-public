from airflow.decorators import task

import pandas as pd

import logging
import json

from utils.misc import mongo_get_collection, strptime


@task
def test_second_internal_imaging(ssr_id, **kwargs):
    """Integration test to compare manual and automatic assignment of second internal imaging AccessionNumber(s)."""

    config = {"user": kwargs["MONGODB_USER"], "password": kwargs["MONGODB_PASSWORD"],
              "url": kwargs["DEPLOYMENT_URL"], "port": kwargs["MONGODB_PORT"],
              "db": kwargs["MONGODB_DATABASE_NAME"]}

    ssr_collection = mongo_get_collection("swiss_stroke_registry", **config)
    studies_collection = mongo_get_collection("studies", **config)
    instances_collection = mongo_get_collection("instances", **config)

    s_cursor = ssr_collection.find_one({"_SSRID": ssr_id})
    s = pd.Series(s_cursor)

    s["_SecondInternalImagingTime"] = strptime(
        str(s['Second Internal Second Date']) + str(s['Second Internal Imaging Time']), "%d.%m.%Y%H:%M")

    second_internal_imaging_scanner = s['Second Internal Imaging Scanner']
    _second_internal_imaging_time = s["_SecondInternalImagingTime"]
    second_internal_imaging_accession_number = s[' Second Internal Imaging Accession Number']

    patient_id = s.PatientID
    db_query = {"PatientID": patient_id, "_SSRID": ssr_id}
    study_cursor = studies_collection.find(db_query)
    df_studies = pd.DataFrame(study_cursor)

    ref_second_int_img = dict()
    try:
        sid2, stime2 = df_studies \
                           .loc[(df_studies["_AcquisitionState"] == "Internal") &
                                (df_studies["_AcquisitionNumber"] == 2), :] \
                           .loc[:, ["StudyInstanceUID", "_StudyTimeExact"]].iloc[0]
        for tag in ["_AcquisitionTimeExact", "_SeriesTimeExact", "_StudyTimeExact"]:
            instances = list(instances_collection
                             .find({**db_query, **{"StudyInstanceUID": sid2, tag: {"$exists": True}}})
                             .sort(tag, 1))
            if len(instances) > 0:
                ref_second_int_img = instances[0]
                break
    except (KeyError, IndexError, StopIteration):
        pass

    if not ref_second_int_img:
        second_int_img_scanner, second_int_img_acc_nr = "##", None
    else:
        second_int_img_scanner, second_int_img_acc_nr = \
            ref_second_int_img["InstitutionName"], ref_second_int_img.get("AccessionNumber", None)

    time = ref_second_int_img.get("_AcquisitionTimeExact",
             ref_second_int_img.get("_SeriesTimeExact",
               ref_second_int_img.get("_StudyTimeExact", None)))

    second_internal_imaging_accession_number = \
        None if second_internal_imaging_accession_number in ["nan", "0"] else \
        second_internal_imaging_accession_number

    res = {
        "_SSRID": ssr_id,
        "internal_imaging_2": {
            "match": second_int_img_acc_nr == second_internal_imaging_accession_number,
            "db": {
                "_AcquisitionTimeExact": time if time is None else str(time),
                "AccessionNumber": second_int_img_acc_nr, 
                "Scanner": ref_second_int_img.get("InstitutionName", None)},
            "excel": {
                "_AcquisitionTimeExact":
                    None if _second_internal_imaging_time == "nan" else
                    None if pd.isna(_second_internal_imaging_time) else
                    str(_second_internal_imaging_time),
                "AccessionNumber": second_internal_imaging_accession_number,
                "Scanner":
                    None if second_internal_imaging_scanner in [None, "0", "nan"] else
                    second_internal_imaging_scanner}}}
    
    logging.info(
        f"Second internal imaging: {second_int_img_acc_nr} == {second_internal_imaging_accession_number}")

    return json.dumps(res)
