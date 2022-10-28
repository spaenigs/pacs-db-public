from pydicom import Dataset
from pynetdicom import AE, QueryRetrievePresentationContexts
from pynetdicom.sop_class import \
    StudyRootQueryRetrieveInformationModelMove, \
    PatientRootQueryRetrieveInformationModelMove, \
    StudyRootQueryRetrieveInformationModelFind

from airflow.decorators import task


@task
def query_study_level(PatientID, StudyInstanceUID="", StudyDate="", AccessionNumber="", **kwargs):
    """Get information about studies from the PACS."""

    query_dataset = Dataset()
    query_dataset.QueryRetrieveLevel = "STUDY"
    query_dataset.PatientID = PatientID
    query_dataset.PatientName = ""
    query_dataset.StudyInstanceUID = StudyInstanceUID
    query_dataset.StudyDescription = ""
    query_dataset.StudyDate = StudyDate
    query_dataset.StudyTime = ""
    query_dataset.NumberOfStudyRelatedSeries = ""
    query_dataset.NumberOfStudyRelatedInstances = ""
    query_dataset.ModalitiesInStudy = ""
    query_dataset.AccessionNumber = AccessionNumber

    ae = AE(ae_title=kwargs["PACS_LOCAL_AE_TITLE"])
    assoc = ae.associate(
        kwargs["PACS_REMOTE_URL"], int(kwargs["PACS_REMOTE_PORT"]),
        ae_title=kwargs["PACS_REMOTE_AE_TITLE"],
        contexts=QueryRetrievePresentationContexts)
    if assoc.is_established:
        responses = assoc.send_c_find(
            query_dataset, query_model=StudyRootQueryRetrieveInformationModelFind)
        study_datasets = []
        for (status, response_dataset) in responses:
            if response_dataset is not None:
                study_datasets.append(response_dataset.to_json())
        assoc.release()
        return study_datasets
