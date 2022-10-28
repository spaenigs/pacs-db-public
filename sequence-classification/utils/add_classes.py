from more_itertools import distribute
import pandas as pd


"""A script to get the reference image of series and to add the respective class, i.e., sequence type."""


def get_ref_img(df):
    group_1, group_2 = distribute(2, df.sort_values("path").index)
    idx = list(group_1)[-1]
    return df.loc[idx, :]


df_accs = pd.read_csv("data/accession_numbers.csv", index_col=0, keep_default_na=False)
df_accs["id"] = df_accs.apply(lambda row: row["AccessionNumber"] + "_" + row["SeriesInstanceUID"], axis=1)

# for all series get reference image in the middle of the series
df_paths = pd.read_csv("data/paths.csv", index_col=0)
df_paths = df_paths.groupby(["AccessionNumber", "SeriesInstanceUID"])\
    .apply(lambda df: get_ref_img(df))\
    .reset_index(drop=True)

# left join reference image paths with assigned classes
df_res = df_paths.merge(df_accs, how="left", on="id")
df_res = df_res[["AccessionNumber_x", "SeriesInstanceUID_x", "path", "class"]]
df_res.columns = ["AccessionNumber", "SeriesInstanceUID", "path", "y"]

# assign class 'Other' to unclassified series
df_res["y"] = df_res["y"].apply(lambda c: "Other" if type(c) == float else c)

df_res.AccessionNumber = [str(i) for i in df_res.AccessionNumber]

# remove studies for which no class has been assigned
# i.e., studies, which only contain class 'Other'
df_res = df_res.groupby("AccessionNumber")\
    .apply(lambda df: pd.DataFrame() if df["y"].unique()[0] == "Other" and len(df["y"].unique()) == 1 else df)\
    .reset_index(drop=True)

df_res["mod"] = ["MR" if "MR" in path else "CT" for path in df_res["path"]]

df_res = df_res[["AccessionNumber", "SeriesInstanceUID", "path", "mod", "y"]]

for mod in ["MR", "CT"]:
    df_res.loc[df_res["mod"] == mod].to_csv(f"data/{mod}/paths_and_classes.csv")
