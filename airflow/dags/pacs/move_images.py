import struct

from pynetdicom import \
    AE, evt, \
    StoragePresentationContexts, \
    QueryRetrievePresentationContexts

from pynetdicom.sop_class import \
    StudyRootQueryRetrieveInformationModelMove, \
    PatientRootQueryRetrieveInformationModelMove, \
    StudyRootQueryRetrieveInformationModelFind

from airflow.decorators import task

from pydicom import Dataset
from pydicom.datadict import keyword_for_tag
from pydicom.errors import BytesLengthException

from utils.misc import rearrange_datasets

import logging


@task
def failed_images(images):
    """Find failed images downloads based on missing SOPInstanceUID."""
    res = []
    for image_str in images:
        ds = Dataset.from_json(image_str)
        if "SOPClassUID" not in ds:
            res.append(image_str)
    # until fix of https://github.com/apache/airflow/issues/24338
    return res if len(res) > 0 else [Dataset().to_json()]


@task
def successful_images(images):
    """Find successfully downloaded images based on present SOPInstanceUID."""
    res = []
    for image_str in images:
        ds = Dataset.from_json(image_str)
        if "SOPClassUID" in ds:
            res.append(image_str)
    return res


def validate_entries(dataset):
    """Remove invalid, i.e., un-parsable, entries from dicom."""
    ds = Dataset()
    for k, v in dataset.items():
        tag_name = keyword_for_tag(k)
        try:
            _ = dataset[(hex(k.group), hex(k.elem))]
            Dataset.from_json(Dataset({k: v}).to_json())
            ds[k] = v
        except ValueError as e:
            logging.warning(f"Skipped tag '{tag_name}'  due to: {e}!")
        except struct.error as e:
            logging.warning(f"Skipped tag '{tag_name}' due to: {e}!")
        except BytesLengthException as e:
            logging.warning(f"Skipped tag '{tag_name}'  due to: {e}!")
    return ds


@task
def move_image(instance_dataset, **kwargs):
    """Move (download) reference image. Requires present SOPInstanceUID."""

    def handle_store(event, *args):
        """Handle a C-STORE service request"""
        ds = event.dataset
        try:
            # we are only interested in the meta data
            del ds.PixelData
        except AttributeError:
            pass
        args[0].append(ds)
        return 0x0000

    image = []
    args = [image]
    handlers = [(evt.EVT_C_STORE, handle_store, args)]

    ae = AE()
    ae.add_requested_context(StudyRootQueryRetrieveInformationModelMove)
    ae.supported_contexts = StoragePresentationContexts
    scp = ae.start_server(
        ("0.0.0.0", int(kwargs["PACS_LOCAL_PORT"])), block=False, evt_handlers=handlers)

    query_dataset = Dataset.from_json(instance_dataset)
    query_dataset.QueryRetrieveLevel = "IMAGE"

    ae = AE(ae_title=kwargs["PACS_LOCAL_AE_TITLE"])
    assoc = ae.associate(
        kwargs["PACS_REMOTE_URL"], int(kwargs["PACS_REMOTE_PORT"]),
        ae_title=kwargs["PACS_REMOTE_AE_TITLE"],
        contexts=QueryRetrievePresentationContexts)
    if assoc.is_established:
        responses = assoc.send_c_move(
            query_dataset, kwargs["PACS_LOCAL_AE_TITLE"],
            query_model=StudyRootQueryRetrieveInformationModelMove)
        for (status, response_dataset) in responses:
            print(status, response_dataset)
        assoc.release()

    scp.shutdown()

    try:
        ds = validate_entries(image[0])
        return ds.to_json(
            bulk_data_threshold=0,
            bulk_data_element_handler=lambda _: "removed")
    except IndexError:
        return query_dataset.to_json()


@task
def move_series(series_dataset, **kwargs):
    """Move (download) complete series and return reference image."""

    def handle_store(event, *args):
        """Handle a C-STORE service request"""
        args[0].append(event.dataset)
        return 0x0000

    if "ti" in kwargs and kwargs["ti"].map_index == 0:
        return "{}"
    elif series_dataset == Dataset().to_json():
        return "{}"

    images = []
    args = [images]
    handlers = [(evt.EVT_C_STORE, handle_store, args)]

    ae = AE()
    ae.add_requested_context(StudyRootQueryRetrieveInformationModelMove)
    ae.supported_contexts = StoragePresentationContexts
    scp = ae.start_server(
        ("0.0.0.0", int(kwargs["PACS_LOCAL_PORT"])), block=False, evt_handlers=handlers)

    ds = Dataset.from_json(series_dataset)

    query_dataset = Dataset()
    query_dataset.StudyInstanceUID = ds.StudyInstanceUID
    query_dataset.SeriesInstanceUID = ds.SeriesInstanceUID
    query_dataset.QueryRetrieveLevel = "SERIES"

    ae = AE(ae_title=kwargs["PACS_LOCAL_AE_TITLE"])
    assoc = ae.associate(
        kwargs["PACS_REMOTE_URL"], int(kwargs["PACS_REMOTE_PORT"]),
        ae_title=kwargs["PACS_REMOTE_AE_TITLE"],
        contexts=QueryRetrievePresentationContexts)

    if assoc.is_established:
        responses = assoc.send_c_move(
            query_dataset, kwargs["PACS_LOCAL_AE_TITLE"],
            query_model=StudyRootQueryRetrieveInformationModelMove)
        cnt_pending = 0
        for (status, response_dataset) in responses:
            print(status, response_dataset)
            # in case the server is stuck on state "PENDING", exit and close association
            cnt_pending += 1 if status.get("Status", -1) == 65280 else 0
            if cnt_pending == 70:
                break
        assoc.release()

    # in case no images have been downloaded
    if len(images) == 0:
        args[0].append(query_dataset)

    scp.shutdown()

    return rearrange_datasets(images)[0].to_json(
        bulk_data_threshold=0,
        bulk_data_element_handler=lambda _: "removed")


@task
def store_image(instance_dataset, storage_path, **kwargs):
    """Store image at storage_path."""

    def handle_store(event, *args):
        """Handle a C-STORE service request"""
        logging.info("Downloaded image")
        ds = event.dataset
        ds.is_little_endian = True
        ds.is_implicit_VR = True
        ds.save_as(f"{storage_path}{ds.SOPInstanceUID}")
        return 0x0000

    handlers = [(evt.EVT_C_STORE, handle_store)]

    ae = AE()
    ae.add_requested_context(StudyRootQueryRetrieveInformationModelMove)
    ae.supported_contexts = StoragePresentationContexts
    ae.ae_title = kwargs["PACS_LOCAL_AE_TITLE"]
    scp = ae.start_server(
        ("0.0.0.0", int(kwargs["PACS_LOCAL_PORT"])), block=False, evt_handlers=handlers)

    query_dataset = Dataset.from_json(instance_dataset)
    query_dataset.QueryRetrieveLevel = "IMAGE"

    ae = AE(ae_title=kwargs["PACS_LOCAL_AE_TITLE"])
    assoc = ae.associate(
        kwargs["PACS_REMOTE_URL"], int(kwargs["PACS_REMOTE_PORT"]),
        ae_title=kwargs["PACS_REMOTE_AE_TITLE"],
        contexts=QueryRetrievePresentationContexts)
    if assoc.is_established:
        responses = assoc.send_c_move(
            query_dataset, kwargs["PACS_LOCAL_AE_TITLE"],
            query_model=StudyRootQueryRetrieveInformationModelMove)
        for (status, response_dataset) in responses:
            print(status, response_dataset)
        assoc.release()

    scp.shutdown()
