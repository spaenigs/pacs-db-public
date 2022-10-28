from typing import List

from pydicom import Dataset
from pynetdicom import AE, QueryRetrievePresentationContexts
from pynetdicom.sop_class import \
    StudyRootQueryRetrieveInformationModelMove, \
    PatientRootQueryRetrieveInformationModelMove, \
    PatientRootQueryRetrieveInformationModelFind

from airflow.decorators import task


@task
def query_patient_level(PatientName, PatientBirthDate, PatientID="", **kwargs) -> List[str]:
    """Get information about patients from the PACS."""

    query_dataset = Dataset()
    query_dataset.QueryRetrieveLevel = "PATIENT"
    query_dataset.PatientID = PatientID
    query_dataset.PatientName = PatientName
    query_dataset.PatientBirthDate = PatientBirthDate
    query_dataset.SpecificCharacterSet = ""

    ae = AE(ae_title=kwargs["PACS_LOCAL_AE_TITLE"])
    assoc = ae.associate(
        kwargs["PACS_REMOTE_URL"], int(kwargs["PACS_REMOTE_PORT"]),
        ae_title=kwargs["PACS_REMOTE_AE_TITLE"],
        contexts=QueryRetrievePresentationContexts)
    if assoc.is_established:
        responses = assoc.send_c_find(
            query_dataset, query_model=PatientRootQueryRetrieveInformationModelFind)
        patient_datasets = []
        for (status, response_dataset) in responses:
            if response_dataset is not None:
                patient_datasets.append(response_dataset.to_json())
        assoc.release()
        return patient_datasets
