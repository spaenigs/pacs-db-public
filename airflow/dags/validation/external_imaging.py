from airflow.decorators import task

import pandas as pd

import logging
import json

from utils.misc import mongo_get_collection, strptime


@task
def test_external_imaging(ssr_id, **kwargs):
    """Integration test to compare manual and automatic assignment of external imaging AccessionNumber(s)."""

    config = {"user": kwargs["MONGODB_USER"], "password": kwargs["MONGODB_PASSWORD"],
              "url": kwargs["DEPLOYMENT_URL"], "port": kwargs["MONGODB_PORT"],
              "db": kwargs["MONGODB_DATABASE_NAME"]}

    ssr_collection = mongo_get_collection("swiss_stroke_registry", **config)
    studies_collection = mongo_get_collection("studies", **config)
    instances_collection = mongo_get_collection("instances", **config)

    s_cursor = ssr_collection.find_one({"_SSRID": ssr_id})
    s = pd.Series(s_cursor)

    s["_ExternalImagingTime"] = strptime(
        str(s['External Imaging Date']) + str(s['External Imaging Time']), "%d.%m.%Y%H:%M")

    _external_imaging_time = s['_ExternalImagingTime']
    external_imaging_accession_number = s['External Imaging Accession Number']

    patient_id = s.PatientID
    db_query = {"PatientID": patient_id, "_SSRID": ssr_id}
    study_cursor = studies_collection.find(db_query)
    df_studies = pd.DataFrame(study_cursor)

    ref_ext_img = dict()
    try:
        # use external imaging before admission as reference image, e.g., 2 if external 1 and 2 exist
        sid = df_studies \
                  .loc[df_studies["_AcquisitionState"] == "External", :] \
                  .sort_values("_AcquisitionNumber", ascending=False) \
                  .loc[df_studies["_StudyTimeExact"] < s["arrival_time_at_hospital"]] \
                  .loc[:, "StudyInstanceUID"].to_list()[0]
        for tag in ["_AcquisitionTimeExact", "_SeriesTimeExact", "_StudyTimeExact"]:
            instances = list(instances_collection
                             .find({**db_query, **{"StudyInstanceUID": sid, tag: {"$exists": True}}})
                             .sort(tag, 1))
            if len(instances) > 0:
                ref_ext_img = instances[0]
                break
        raise StopIteration()
    except (KeyError, IndexError, StopIteration):
        pass

    if not ref_ext_img:
        ext_img_avail, ext_img_acc_nr = False, None
    else:
        ext_img_avail, ext_img_acc_nr = True, ref_ext_img.get("AccessionNumber", None)

    time = ref_ext_img.get("_AcquisitionTimeExact",
               ref_ext_img.get("_SeriesTimeExact",
                   ref_ext_img.get("_StudyTimeExact", None)))

    external_imaging_accession_number = \
        None if external_imaging_accession_number in ["nan", "0"] else \
        external_imaging_accession_number

    res = {
        "_SSRID": ssr_id,
        "external_imaging": {
            "match": ext_img_acc_nr == external_imaging_accession_number,
            "db": {
                "_AcquisitionTimeExact": time if time is None else str(time),
                "AccessionNumber": ext_img_acc_nr, 
                "Scanner": ref_ext_img.get("InstitutionName", None)},
            "excel": {
                "_AcquisitionTimeExact":
                    None if _external_imaging_time == "nan" else
                    None if pd.isna(_external_imaging_time) else
                    str(_external_imaging_time),
                "AccessionNumber": external_imaging_accession_number,
                "Scanner": None}}}

    logging.info(
        f"External imaging: {ext_img_acc_nr} == {external_imaging_accession_number}")

    return json.dumps(res)
