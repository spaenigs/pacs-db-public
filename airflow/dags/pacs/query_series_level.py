from pydicom import Dataset
from pynetdicom import AE, QueryRetrievePresentationContexts
from pynetdicom.sop_class import \
    StudyRootQueryRetrieveInformationModelMove, \
    PatientRootQueryRetrieveInformationModelMove, \
    StudyRootQueryRetrieveInformationModelFind

from airflow.decorators import task

import logging


@task
def query_series_level(study_dataset, **kwargs):
    """Get information about series from the PACS."""

    query_dataset = Dataset.from_json(study_dataset)
    query_dataset.QueryRetrieveLevel = "SERIES"
    query_dataset.SeriesInstanceUID = ""
    query_dataset.SeriesDate = ""
    query_dataset.SeriesTime = ""
    query_dataset.SeriesDescription = ""
    query_dataset.TimezoneOffsetFromUTC = ""

    ae = AE(ae_title=kwargs["PACS_LOCAL_AE_TITLE"])
    assoc = ae.associate(
        kwargs["PACS_REMOTE_URL"], int(kwargs["PACS_REMOTE_PORT"]),
        ae_title=kwargs["PACS_REMOTE_AE_TITLE"],
        contexts=QueryRetrievePresentationContexts)
    if assoc.is_established:
        responses = assoc.send_c_find(
            query_dataset, query_model=StudyRootQueryRetrieveInformationModelFind)
        series_datasets = []
        for (status, response_dataset) in responses:
            if response_dataset is not None:
                series_datasets.append(response_dataset.to_json())
        assoc.release()
        logging.info(f"# of series = {len(series_datasets)}")
        return series_datasets