from pynetdicom import AE, evt, StoragePresentationContexts, QueryRetrievePresentationContexts
from pynetdicom.sop_class import \
    StudyRootQueryRetrieveInformationModelMove, \
    PatientRootQueryRetrieveInformationModelMove, \
    StudyRootQueryRetrieveInformationModelFind

from pydicom import Dataset
from pathlib import Path
from pymongo import MongoClient

import os

config = {k: v for k, v in os.environ.items()}


def mongo_get_collection(collection_name):
    """Returns the reference to a collection in the database DB_NAME."""
    mongo_db = MongoClient(config["DB_CONN_STRING"])
    db = mongo_db[config["DB_NAME"]]
    return db[collection_name]


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
        config["PACS_REMOTE_URL"], config["PACS_REMOTE_PORT"], ae_title=config["PACS_REMOTE_PORT"],
        contexts=QueryRetrievePresentationContexts)
    if assoc.is_established:
        responses = assoc.send_c_find(query_dataset, query_model=StudyRootQueryRetrieveInformationModelFind)
        study_datasets = []
        for (status, response_dataset) in responses:
            if response_dataset is not None:
                study_datasets.append(response_dataset.to_json())
        assoc.release()
        return study_datasets


def store_image(instance_dataset, storage_path, delete_pixel_data=True, **kwargs):
    """Store image at storage_path. Delete PixelData attribute if required."""

    def handle_store(event, *args):
        """Handle a C-STORE service request"""
        ds = event.dataset
        ds.is_little_endian = True
        ds.is_implicit_VR = True
        if delete_pixel_data:
            del ds.PixelData
        path = f"{storage_path}{ds.Modality}/{ds.AccessionNumber}/" + \
               f"{ds.SeriesInstanceUID}/"
        Path(path).mkdir(parents=True, exist_ok=True)
        print("Storing " + path + f"{ds.SOPInstanceUID}.dcm")
        ds.save_as(path + f"{ds.SOPInstanceUID}.dcm")
        return 0x0000

    handlers = [(evt.EVT_C_STORE, handle_store)]

    ae = AE()
    ae.add_requested_context(StudyRootQueryRetrieveInformationModelMove)
    ae.supported_contexts = StoragePresentationContexts
    ae.ae_title = kwargs["PACS_LOCAL_AE_TITLE"]
    scp = ae.start_server(("0.0.0.0", kwargs["PACS_LOCAL_PORT"]), block=False, evt_handlers=handlers)

    query_dataset = Dataset.from_json(instance_dataset)

    ae = AE(ae_title=kwargs["PACS_LOCAL_AE_TITLE"])
    assoc = ae.associate(
        config["PACS_REMOTE_URL"], config["PACS_REMOTE_PORT"], ae_title=config["PACS_REMOTE_AE_TITLE"],
        contexts=QueryRetrievePresentationContexts)
    if assoc.is_established:
        responses = assoc.send_c_move(
            query_dataset, kwargs["PACS_LOCAL_AE_TITLE"],
            query_model=StudyRootQueryRetrieveInformationModelMove)
        for (status, response_dataset) in responses:
            print(status, response_dataset)
        print("--- Finished c-move ---")
        assoc.release()

    scp.shutdown()
