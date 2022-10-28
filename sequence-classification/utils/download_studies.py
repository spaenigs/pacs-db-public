import time

from pydicom import Dataset
from glob import glob

import pandas as pd

from utils import mongo_get_collection, query_study_level, store_image

"""Download studies from PACS for which we have manual classified data."""

ssr_collection = mongo_get_collection("swiss_stroke_registry")

query_1 = {"$or": [
    {"$and": [
        {" First Internal Imaging Accession Number": {"$ne": "0"}},
        {" First Internal Imaging Accession Number": {"$ne": "nan"}}]},
    {"$and": [
        {"External Imaging Accession Number": {"$ne": "0"}}, 
        {"External Imaging Accession Number": {"$ne": "nan"}}]},
    {"$and": [{" Second Internal Imaging Accession Number": {"$ne": "0"}},
        {" Second Internal Imaging Accession Number": {"$ne": "nan"}}]}
]}

keys = [
    "MR ADC (4mm) Series Instance UID", "MR DWI (4mm, optional b0) Series Instance UID",
    "MR DWI (4mm, optional b1000) Series Instance UID",
    "MR FLAIR (4mm) Series Instance UID", "MR Perfusion (1800) (4mm) Series Instance UID",
    "MR SWI Series Instance UID", "MR TOF (0_6mm) Series Instance UID", "CT Nativ (1mm) Series Instance UID",
    "CT Angio (0_6 mm) Series Instance UID", "CT Post-Contrast (1mm) Series Instance UID",
    "CT Perfusion (960) Series Instance UID"
]

or_query = []
for k in keys:
    and_query = {"$and": [{k: {"$ne": "0"}}, {k: {"$ne": "nan"}}]}
    or_query.append(and_query)
query_2 = {"$or": or_query}

predicate = {
    "MR ADC (4mm) Series Instance UID": 1, 
    "MR DWI (4mm, optional b0) Series Instance UID" :1, 
    "MR DWI (4mm, optional b1000) Series Instance UID" : 1,
    "MR FLAIR (4mm) Series Instance UID": 1,
    "MR Perfusion (1800) (4mm) Series Instance UID": 1,
    "MR SWI Series Instance UID": 1,
    "MR TOF (0_6mm) Series Instance UID": 1,
    "CT Nativ (1mm) Series Instance UID": 1,
    "CT Angio (0_6 mm) Series Instance UID": 1,
    "CT Post-Contrast (1mm) Series Instance UID": 1,
    "CT Perfusion (960) Series Instance UID": 1, 
    " First Internal Imaging Accession Number": 1, 
    "External Imaging Accession Number": 1, 
    " Second Internal Imaging Accession Number": 1
}

# get all entries for which we have either an AccessionNumber and/or
# the SeriesInstanceUID of the classified series
ssr_cursor = ssr_collection.find(
    {**query_1, **query_2}, {**predicate, "_id": 0, "_SSRID": 1})
df_ssr = pd.DataFrame(ssr_cursor)
df_ssr.index = df_ssr["_SSRID"]

res = []
# store AccessionNumber along with the SeriesInstanceUID of the classified series
# i.e., each line is one Study and the respective SeriesInstanceUID's
# however, at this point we don't know whether a Series actually belongs to a study
for n, s in df_ssr.iterrows():
    ids = []
    for id_ in s[1:3]:
        ids.extend(id_.split("+"))
    for i in ids:
        if i == "0" or i == "nan":
            pass
        else:
            i = i.replace("(", "").replace(")", "").strip()
            res.append([s[0], i] + s[4:].tolist())

df_res = pd.DataFrame(res, columns=["_SSRID", "AccessionNumber"] + list(df_ssr.columns[4:]))
df_res = df_res.drop_duplicates(["AccessionNumber"])

# get AccessioNumber-SeriesInstanceUID mappings
df_res = pd.melt(df_res, id_vars=['AccessionNumber'], value_vars=keys)\
    .sort_values("AccessionNumber")\
    .reset_index(drop=True)
df_res.columns = ["AccessionNumber", "class", "SeriesInstanceUID"]

df_res["class"] = df_res["class"].apply(
    lambda i: i.replace("Series Instance UID", "").strip())

df_res["SeriesInstanceUID"] = df_res["SeriesInstanceUID"]\
    .apply(lambda i: "" if i == "0" else "" if i == "nan" else "" if type(i) == float else i)

df_res.to_csv("data/accession_numbers.csv")

# download each study
for idx, acc in enumerate(df_res.AccessionNumber.unique()):
    if acc in ["4726280"]:
        continue
    ds = Dataset()
    ds.AccessionNumber = acc
    ds.StudyInstanceUID = ""
    target_dir = "/str/data/images/ssr_complete/"
    if len(glob(f"{target_dir}*/{acc}/")) >= 1:
        print("==>", idx, acc)
        continue
    else:
        print(idx, acc)
        try:
            study = query_study_level(PatientID="", AccessionNumber=acc)[0]
            store_image(study, target_dir)
            time.sleep(7*60)
            # break
        except IndexError:
            pass
        except TypeError:
            pass
            

