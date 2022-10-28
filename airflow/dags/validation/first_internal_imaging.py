from airflow.decorators import task

import pandas as pd

import logging
import json

from utils.misc import mongo_get_collection, strptime


@task
def test_first_internal_imaging(ssr_id, **kwargs):
    """Integration test to compare manual and automatic assignment of first internal imaging AccessionNumber(s)."""

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

    first_internal_imaging_scanner = s['First Internal Imaging Scanner']
    _first_internal_imaging_time = s["_FirstInternalImagingTime"]
    first_internal_imaging_accession_number = s[' First Internal Imaging Accession Number']

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

    if not ref_first_int_img:
        first_int_img_scanner, first_int_img_acc_nr = "##", None
    else:
        first_int_img_scanner, first_int_img_acc_nr = \
            ref_first_int_img["InstitutionName"], ref_first_int_img.get("AccessionNumber", None)

    stime = ref_first_int_img.get("_AcquisitionTimeExact",
              ref_first_int_img.get("_SeriesTimeExact",
                ref_first_int_img.get("_StudyTimeExact", None)))

    first_internal_imaging_accession_number = \
        None if first_internal_imaging_accession_number in ["nan", "0"] else \
        first_internal_imaging_accession_number

    res = {
        "_SSRID": ssr_id,
        "internal_imaging_1": {
            "match": first_int_img_acc_nr == first_internal_imaging_accession_number,
            "db": {
                "_AcquisitionTimeExact": stime if stime is None else str(stime),
                "AccessionNumber": first_int_img_acc_nr, 
                "Scanner": ref_first_int_img.get("InstitutionName", None)},
            "excel": {
                "_AcquisitionTimeExact":
                    None if _first_internal_imaging_time == "nan" else
                    None if pd.isna(_first_internal_imaging_time) else
                    str(_first_internal_imaging_time),
                "AccessionNumber": first_internal_imaging_accession_number,
                "Scanner":
                    None if first_internal_imaging_scanner in [None, "0", "nan"] else
                    first_internal_imaging_scanner}}}
    
    logging.info(
        f"First internal imaging: {first_int_img_acc_nr} == {first_internal_imaging_accession_number}")

    return json.dumps(res)
