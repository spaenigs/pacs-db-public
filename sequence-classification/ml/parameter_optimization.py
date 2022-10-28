import warnings

warnings.filterwarnings("ignore", category=UserWarning)

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.multiclass import OneVsRestClassifier

import pandas as pd
import altair as alt

import logging


def run(df, mod, tag, param_grid, directory, cpu_cores=-1):
    """Runs a nested cross-validation on a given grid of hyper-parameters to find the best configuration."""
    X, y = df.iloc[:, :-1].values, df.y.values
    iclf = RandomForestClassifier(n_jobs=cpu_cores)
    clf = OneVsRestClassifier(iclf, n_jobs=cpu_cores)
    grid = GridSearchCV(
        clf, param_grid, return_train_score=True,
        scoring="matthews_corrcoef", error_score="raise")
    grid.fit(X, y)
    grid.cv_results_["mod"] = [mod] * len(grid.cv_results_["mean_fit_time"])
    grid.cv_results_["tag"] = [tag] * len(grid.cv_results_["mean_fit_time"])
    grid.cv_results_["params"] = [{
        k.replace("estimator__", ""): v for k, v in d.items()} 
        for d in grid.cv_results_["params"]]
    df_res = pd.DataFrame(grid.cv_results_)
    df_res.to_csv(directory + "nested_cv.csv")
    return df_res


def plot(df_cv, directory):
    """Plot the results of the hyper-parameter optimization."""
    rows = []
    for _, c in df_cv.loc[(df_cv.rank_test_score == 1), :] \
            .sort_values("mean_test_score") \
            .transpose().iteritems():
        res = []
        for n, s in c.iteritems():
            if "split" in n:
                _, cv_type, _ = n.split("_")
                res.append([cv_type, s, c["mod"], c["tag"], c["params"]])
        rows.extend(res)
    df_res = pd.DataFrame(rows, columns=["cv_type", "mcc", "modality", "tag", "params"])
    alt.Chart(
        df_res,
        title="Nested CV: inner vs. outer"
    ).mark_boxplot(extent='min-max').encode(
        x='cv_type:N',
        y='mcc:Q',
        row="modality:N",
        column="tag:N"
    ).properties(
        height=100, width=100
    ).save(directory + "nested_cv.html")
