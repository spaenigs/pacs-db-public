from pydicom import Dataset
from pynetdicom import AE, QueryRetrievePresentationContexts
from pynetdicom.sop_class import \
    StudyRootQueryRetrieveInformationModelMove, \
    PatientRootQueryRetrieveInformationModelMove, \
    StudyRootQueryRetrieveInformationModelFind

from airflow.decorators import task

from utils.misc import rearrange_datasets


@task
def query_instance_level(series_dataset, **kwargs):
    """Get information about images from the PACS."""

    ds = Dataset.from_json(series_dataset)

    query_dataset = Dataset()
    query_dataset.StudyInstanceUID = ds.StudyInstanceUID
    query_dataset.SeriesInstanceUID = ds.SeriesInstanceUID
    query_dataset.QueryRetrieveLevel = "IMAGE"
    query_dataset.SOPInstanceUID = ""
    query_dataset.InstanceNumber = ""

    ae = AE(ae_title=kwargs["PACS_LOCAL_AE_TITLE"])
    assoc = ae.associate(
        kwargs["PACS_REMOTE_URL"], int(kwargs["PACS_REMOTE_PORT"]),
        ae_title=kwargs["PACS_REMOTE_AE_TITLE"],
        contexts=QueryRetrievePresentationContexts)
    if assoc.is_established:
        responses = assoc.send_c_find(
            query_dataset, query_model=StudyRootQueryRetrieveInformationModelFind)
        instance_datasets = []
        for (status, response_dataset) in responses:
            if response_dataset is not None:
                instance_datasets.append(response_dataset)
        assoc.release()
        try:
            return rearrange_datasets(instance_datasets)[0].to_json()
        except IndexError:
            return query_dataset.to_json()
