from typing import Dict, Any
from pymongo import MongoClient
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime

import logging
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

import pandas as pd

import orjson
import re
import math
from dotenv import dotenv_values


config = dotenv_values()


class ORJSONResponse(JSONResponse):
    media_type = "application/json"
    def render(self, content):
        return orjson.dumps(content, default=str)


app = FastAPI(default_response_class=ORJSONResponse)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Get more detailed error messages."""
    exc_str = f'{exc}'.replace('\n', ' ').replace('   ', ' ')
    logging.info(request)
    logging.error(f"{request}: {exc_str}")
    content = {'status_code': 10422, 'message': exc_str, 'data': None}
    return JSONResponse(content=content, status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)


origins = [
    f"http://localhost:{config['FRONTEND_PORT']}",
    f"http://{config['FRONTEND_URL']}:{config['FRONTEND_PORT']}"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"])


def mongo_get_collection(collection_name):
    """Get reference to MongoDB collection."""
    mongo_db = MongoClient(config["DB_CONN_STRING"])
    db = mongo_db[config["DB_NAME"]]
    return db[collection_name]


PROJECTION = {
    "_id": 0, "SpecificCharacterSet": 0, "TimezoneOffsetFromUTC": 0, 
    "QueryRetrieveLevel": 0, "RetrieveAETitle": 0, "InstanceAvailability": 0,
    "StudyID": 0}


@app.get("/tests")
async def root():
    """Get all tests results from database."""
    tests_collection = mongo_get_collection("tests")
    tests_cursor = tests_collection.find({}, PROJECTION)
    return [i for i in tests_cursor]


@app.get("/ssr_ids")
async def root():
    """Get all SSR IDs from database."""
    ssr_collection = mongo_get_collection("swiss_stroke_registry")
    ssr_ids_cursor = ssr_collection.find({}, {"_id": 0, "_SSRID": 1})
    return [i["_SSRID"] for i in ssr_ids_cursor]


@app.get("/ssr_ids_db")
async def root():
    """Get all SSR IDs with complete data from database."""
    instances_collection = mongo_get_collection("instances")
    ssr_ids_cursor = instances_collection.find({}, {"_id": 0, "_SSRID": 1}).distinct("_SSRID")
    return [i for i in ssr_ids_cursor]


class Query(BaseModel):
    query: Dict[Any, Any]
    start: int
    end: int


def parse_dates_for_mongo(query: Query):
    """Parses date string at path as datetime object. Required for MongoDB to compare dates."""
    hits = re.findall("'path': '(.*?)'\}", str(query))
    for hit in hits:
        logical_op, idx, field, op = hit.split(":")
        query[logical_op][int(idx)][field][op] = \
            datetime.fromisoformat(query[logical_op][int(idx)][field][op]["date"])
    return query


def calc_last_page(total_documents, to):
    """Calculates the last page for pagination."""
    return 1 if to == 0 else math.ceil(total_documents / to)


def get_pagination(last_page, data):
    """Get pagination data."""
    return {
        "last_page": last_page,
        "data": data}


def get_skip_value(from_, to, last_page):
    """Translates a page range to the MongoDB skip value."""
    if from_ == 1:
        return 0
    elif from_ == last_page:
        return (last_page - 1) * to
    else:
        return (from_ - 1) * to


@app.post("/studies")
async def get_studies(query: Query):
    """
    Get studies for query from database. Returns pagination object, i.e., not complete list of studies is returned.
    """
    q = parse_dates_for_mongo(query.query)
    print(q)
    studies_collection = mongo_get_collection("studies")
    cnt = studies_collection.count_documents(q)
    last_page = calc_last_page(cnt, query.end)
    skip = get_skip_value(
        from_=query.start, to=query.end, last_page=last_page)
    c = studies_collection\
        .find(q, PROJECTION)\
        .skip(skip)\
        .limit(query.end)
    return get_pagination(last_page, data=[i for i in c])


@app.post("/series")
async def get_series(query: Query):
    """
    Get series for query from database. Returns pagination object, i.e., not complete list of series is returned.
    """
    q = parse_dates_for_mongo(query.query)
    print(q)
    series_collection = mongo_get_collection("series")
    cnt = series_collection.count_documents(q)
    last_page = calc_last_page(cnt, query.end)
    skip = get_skip_value(
        from_=query.start, to=query.end, last_page=last_page)
    c = series_collection\
        .find(q, PROJECTION)\
        .skip(skip)\
        .limit(query.end)
    return get_pagination(last_page, [i for i in c])


@app.post("/instances")
async def get_instances(query: Query):
    """
    Get instances, i.e., reference images, for query from database. Returns pagination object, i.e., not complete list
    of instances is returned.
    """
    q = parse_dates_for_mongo(query.query)
    print(q)
    instances_collection = mongo_get_collection("instances")
    # only return the following entries
    instances_fields = [
        "AccessionNumber", "BodyPartExamined", "ImageType", "InstanceNumber", "InstitutionAddress",
        "InstitutionName", "Manufacturer", "ManufacturerModelName", "Modality", "PatientAge",
        "PatientBirthDate", "PatientID", "PatientName", "PatientPosition", "PatientSex", "PixelSpacing",
        "ProtocolName", "SliceLocation", "SliceThickness", "SmallestImagePixelValue", "SoftwareVersions",
        "StationName", "WindowCenter", "_AcquisitionTimeExact", "_SSRID", "_SequenceType", "_SeriesTimeExact",
        "_StudyTimeExact", "SequenceName"]
    cnt = instances_collection.count_documents(q)
    last_page = calc_last_page(cnt, query.end)
    skip = get_skip_value(
        from_=query.start, to=query.end, last_page=last_page)
    proj = {**{"_id": 0}, **{k:1 for k in instances_fields}}
    c = instances_collection\
        .find(q, proj)\
        .skip(skip)\
        .limit(query.end)
    return get_pagination(last_page, [i for i in c])


@app.post("/swiss_stroke_registry")
async def get_ssr(query: Query):
    """
    Get SSR entry for query from database. Returns pagination object, i.e., not complete list of cases is returned.
    """
    q = parse_dates_for_mongo(query.query)
    print(q)
    ssr_collection = mongo_get_collection("swiss_stroke_registry")
    # only return the following entries
    ssr_fields = []
    cnt = ssr_collection.count_documents(q)
    last_page = calc_last_page(cnt, query.end)
    skip = get_skip_value(
        from_=query.start, to=query.end, last_page=last_page)
    proj = {**{"_id": 0}, **{k: 1 for k in ssr_fields}}
    c = ssr_collection\
        .find(q, proj)\
        .skip(skip)\
        .limit(query.end)
    return get_pagination(last_page, [i for i in c])


class Join(BaseModel):
    left_collection: str
    right_collection: str
    left_query: Query
    right_query: Query
    on: str
    start: int
    end: int


def get_uids(collection):
    """Get UIDs for collection. Required for table joins."""
    if collection == "series":
        return ["SeriesInstanceUID", "SeriesInstanceUID_series"]
    elif collection == "instances":
        return ["SOPInstanceUID"]
    else:
        return ["StudyInstanceUID", "StudyInstanceUID_studies"]


@app.post("/join")
async def get_join(join: Join):
    """Join two tables. Returns pagination object, i.e., not complete list of join is returned."""
    left_collection = mongo_get_collection(join.left_collection)
    right_collection = mongo_get_collection(join.right_collection)
    df_left = pd.DataFrame(
        left_collection.find(parse_dates_for_mongo(join.left_query.query), PROJECTION))
    df_right = pd.DataFrame(
        right_collection.find(parse_dates_for_mongo(join.right_query.query), PROJECTION))
    df_res = df_left.merge(
        df_right, on=join.on, how="left", 
        suffixes=[f"_{join.left_collection}", f"_{join.right_collection}"])
    for uid in get_uids(join.right_collection):
        try:
            filter_ = ~pd.isna(df_res[uid])
            break
        except KeyError:
            pass
    df_res = df_res.loc[filter_, :]
    last_page = calc_last_page(df_res.shape[0], join.end)
    skip = get_skip_value(
        from_=join.start, to=join.end, last_page=last_page)
    if join.end > 0:
        end = skip+join.end
    else:
        end = None  # return complete data
    res = df_res.iloc[skip:end, :].to_dict(orient="records")
    return get_pagination(last_page, res)

