from airflow.decorators import task
from pymongo import ASCENDING

from utils.misc import mongo_get_collection

import pandas as pd

import logging

_individual = []
_individual = [sorted(e) for e in _individual]

_group_1 = []
_group_1 = [sorted(e) for e in _group_1]

_group_2 = []
_group_2 = [sorted(e) for e in _group_2]

_special_case_1 = []
_special_case_1 = [sorted(e) for e in _special_case_1]


def fuse_studies(*study_descriptions):
    """Check based on study description which studies will be fused."""
    study_descriptions = sorted(study_descriptions)
    if study_descriptions in _individual:
        return False
    elif study_descriptions in _group_1:
        logging.info(f"Fusing studies: {study_descriptions}.")
        return True
    elif study_descriptions in _group_2:
        logging.info(f"Fusing studies: {study_descriptions}.")
        return True
    elif study_descriptions in _special_case_1:
        logging.info(f"Fusing studies: {study_descriptions[1:]}. "
                     f"Keeping {study_descriptions[0]} separate.")
        return False
    else:
        logging.warning(f"Keep studies separated based on missing {study_descriptions} "
                        f"in individual/group_1/group_2.")
        return False


@task
def sanitize_studies(ssr_id, **kwargs):
    """Cleaning of dumped studies, i.e., grouping of cohesive studies and deletion of empty ones"""

    studies_collection = mongo_get_collection(
        "studies",
        user=kwargs["MONGODB_USER"], password=kwargs["MONGODB_PASSWORD"],
        url=kwargs["DEPLOYMENT_URL"], port=kwargs["MONGODB_PORT"])

    studies_cursor = studies_collection.find(
        {"_SSRID": ssr_id,
         "_AcquisitionState": {"$exists": True},
         "_AcquisitionNumber": {"$exists": True}},
        {"_id": 0}) \
        .sort([("_AcquisitionNumber", ASCENDING)])
    df_studies = pd.DataFrame(studies_cursor)
    df_studies = df_studies.sort_values(["_AcquisitionState", "_AcquisitionNumber"])

    res = []
    for state in df_studies["_AcquisitionState"].unique():
        nr_of_fusions = 0
        df_state = df_studies.loc[df_studies["_AcquisitionState"] == state]
        for n, df_to_fuse in df_state.groupby(["_StudyTimeExact", "_AcquisitionState"]):
            if df_to_fuse.shape[0] > 1:
                study_descriptions = df_to_fuse.StudyDescription
                if fuse_studies(*study_descriptions):
                    df_to_fuse["_AcquisitionNumber"] = df_to_fuse["_AcquisitionNumber"].values[0] - nr_of_fusions
                    nr_of_fusions += 1
                elif sorted(study_descriptions) in _special_case_1:
                    # Explicitly handle special case 1:
                    df_to_fuse = df_to_fuse.sort_values("StudyDescription").reset_index(drop=True)
                    df_to_fuse.loc[df_to_fuse.index[0], "_AcquisitionNumber"] = \
                        df_to_fuse["_AcquisitionNumber"].values.min()
                    df_to_fuse.loc[df_to_fuse.index[1], "_AcquisitionNumber"] = \
                        df_to_fuse.loc[df_to_fuse.index[0], "_AcquisitionNumber"] + 1
                    df_to_fuse.loc[df_to_fuse.index[2], "_AcquisitionNumber"] = \
                        df_to_fuse.loc[df_to_fuse.index[0], "_AcquisitionNumber"] + 1
                    nr_of_fusions += 1
                else:
                    print(ssr_id)
            else:
                df_to_fuse["_AcquisitionNumber"] -= nr_of_fusions
            res.append(df_to_fuse)

    df_res = pd.concat(res)

    # delete studies for ssr_id (this will also remove empty studies
    del_res = studies_collection.delete_many({"_SSRID": ssr_id})
    deleted_studies = del_res.deleted_count
    logging.info(f"Deleted {deleted_studies - df_res.shape[0]} studies")
    logging.info(f"Updated {df_res.shape[0]} studies.")

    # push updated/merged studies
    studies_collection.insert_many(df_res.to_dict(orient="records"))
