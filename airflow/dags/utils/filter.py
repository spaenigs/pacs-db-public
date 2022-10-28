from airflow.decorators import task

from pydicom import Dataset

from utils.misc import filter_examined_body_part

import logging


@task
def filter_studies(study_datasets):
    """Filter study based on StudyDescription."""
    res = []
    for study_dataset_str in study_datasets:
        study_dataset = Dataset.from_json(study_dataset_str)
        desc = study_dataset.StudyDescription
        if filter_examined_body_part(desc):
            logging.info(f"Including study based on study description: {desc}")
            res.append(study_dataset_str)
        else:
            logging.warning(f"Skipping study based on study description: {desc}")
    return res


@task
def filter_series(series_datasets):
    """Filter series based on Modality and SeriesDescription."""
    res = []
    for series_dataset_str in series_datasets:
        series_dataset = Dataset.from_json(series_dataset_str)
        moda = series_dataset.get("Modality", None)
        desc = series_dataset.get("SeriesDescription", None)
        if moda in ["MR", "CT", "XA", None] and filter_examined_body_part(desc):
            logging.info(f"Including series based on modality ({moda}) "
                        f"and series description: {desc}.")
            res.append(series_dataset_str)
        else:
            logging.warning(f"Skipping series based on modality ({moda}) "
                            f"and series description: {desc}.")
    return res


@task
def filter_images(image_datasets):
    """Filter series based on Modality, SeriesDescription, RequestedProcedureDescription, and BodyPartExamined."""
    res = []
    for image_dataset_str in image_datasets:
        if image_dataset_str == "{}":
            continue
        image_dataset = Dataset.from_json(image_dataset_str, bulk_data_uri_handler=lambda _: None)
        req_prod_desc = image_dataset.get("RequestedProcedureDescription", None)
        bod_part_exam = image_dataset.get("BodyPartExamined", None)
        desc = image_dataset.get("SeriesDescription", None)
        modality = image_dataset.get("Modality", None)
        filter_1 = modality in ["MR", "CT", "XA"]
        filter_2 = all([filter_examined_body_part(d) for d in [req_prod_desc, bod_part_exam, desc]])
        if filter_1 and filter_2:
            logging.info(f"Including image based on modality ({modality}), "
                         f"requested procedure description ({req_prod_desc}), "
                         f"body part examined ({bod_part_exam}) and "
                         f"series description: {desc}.")
            res.append(image_dataset_str)
        else:
            logging.warning(f"Skipping image based on modality ({modality}), "
                            f"requested procedure description ({req_prod_desc}), "
                            f"body part examined ({bod_part_exam}), or "
                            f"series description: {desc}.")
    return res
