from glob import glob

import pandas as pd

"""The script stores the paths to the images as well as AccessionNumber and SeriesInstanceUID"""

df = pd.read_csv("data/accession_numbers.csv", index_col=0)

target_dir = "/str/data/images/ssr_complete/"

# get all image paths and parse AccessionNumber and SeriesInstanceUID and keep path
res = []
for path in glob(f"{target_dir}*/*/*/*"):
    if "MR" in path or "CT" in path:
        splitted = path.split("/")
        acc_nr, serid = splitted[6], splitted[7]
        res.append([f"{acc_nr}_{serid}", acc_nr, serid, path])

df_res = pd.DataFrame(res, columns=["id", "AccessionNumber", "SeriesInstanceUID", "path"])
df_res.to_csv("data/paths.csv")
