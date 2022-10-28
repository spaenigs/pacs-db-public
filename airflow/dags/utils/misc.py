from pymongo import MongoClient
from itertools import zip_longest
from datetime import datetime, timedelta
from typing import List, Tuple
from more_itertools import partition

import numpy as np

import re


def mongo_get_collection(collection_name, user, password, url, port, db="PACS_DB"):
    """Get reference to MongoDB collection from the database db."""
    mongo_db = MongoClient(f"mongodb://{user}:{password}@{url}:{port}")
    db = mongo_db[db]
    return db[collection_name]


def rearrange_datasets(datasets):
    """
    Put image from the middle of a series to the first position, since images in the beginning of series are often not
    usable or contain corrupted meta data.
    """
    # if instance number is not set, skip instance and move to end
    datasets_, datasets_none = \
        partition(lambda ds: ds.get("InstanceNumber", None) is None, datasets)
    datasets = list(datasets_)
    datasets = sorted(datasets, key=lambda ds: ds.InstanceNumber)
    if len(datasets) is not 0:
        mid_id_idx = int(len(datasets) / 2)
        new_first_item = datasets.pop(mid_id_idx)
        return [new_first_item] + datasets + list(datasets_none)
    return datasets + list(datasets_none)


def get_time_frame(arrival_time: datetime, dates: List[datetime]):
    """
    Get time frame for follow-up imaging based on the arrival_time at hospital and all visiting dates of a patient.
    The time frame is 12 hours before admission and one year after admission. In case another  event (visiting dates)
    happens in this time, only the follow-up studies until this date are returned.
    """
    delta_12h = timedelta(hours=12)
    delta_one_year = timedelta(weeks=52)
    for start, end in zip_longest(dates, dates[1:]):
        if end is None:
            end = start + delta_one_year
        if end - start > delta_one_year:
            end = start + delta_one_year
        if start == arrival_time:
            return start - delta_12h, end


def filter_examined_body_part(case_description=None) -> bool:
    """Filter examined body part based on a white- and blacklist of keywords."""

    # in case no data is available include tfor now
    if case_description is None or case_description == "":
        return True

    variations = []
    included_keywords = \
        re.compile("|".join([f"{v}" for v in variations]))

    excluded_keywords = \
        []
    pattern_excluded_keywords = \
        re.compile("|".join([f"\\b{v.lower()}\\b" for v in excluded_keywords]))

    excluded_treatments = \
        []
    pattern_excluded_treatments = \
        re.compile("|".join([f"\\b{v.lower()}\\b" for v in excluded_treatments]))

    desc = case_description.lower()
    if re.search(included_keywords, desc):
        return True
    elif re.search(pattern_excluded_keywords, desc):
        return False
    elif re.search(pattern_excluded_treatments, desc):
        return False
    else:
        return True


def strptime(date_string, format):
    """Tries to parse date_string using format. Returns NA if parsing fails."""
    try:
        return datetime.strptime(date_string, format)
    except (ValueError, TypeError) as e:
        return np.nan
