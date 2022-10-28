from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import precision_recall_curve, average_precision_score
from sklearn.model_selection import ShuffleSplit
from sklearn.multiclass import OneVsRestClassifier
from sklearn.preprocessing import label_binarize

import pandas as pd
import numpy as np


def run(df, mod, tag, directory=None, cpu_cores=-1, **optimal_params):
    """Finds the optimal threshold based on the harmonic mean of the precision and recall (F1-score)."""

    X, y = df.iloc[:, :-1].values, df.y.values

    n_classes = len(set(y))
    names = list(set(y))

    y = label_binarize(y, classes=list(set(y)))

    train_index, test_index = \
        next(ShuffleSplit(n_splits=1, test_size=0.5, random_state=0).split(X, y))

    X_train, X_test, y_train, y_test = \
        X[train_index], X[test_index], y[train_index], y[test_index]

    iclf = RandomForestClassifier(
        random_state=0, n_jobs=cpu_cores, **optimal_params)
    classifier = OneVsRestClassifier(iclf, n_jobs=cpu_cores)

    y_score = classifier.fit(X_train, y_train).predict_proba(X_test)

    precision = dict()
    recall = dict()
    thresholds = dict()
    average_precision = dict()
    for i in range(n_classes):
        precision[i], recall[i], thresholds[i] = precision_recall_curve(y_test[:, i], y_score[:, i])
        average_precision[i] = average_precision_score(y_test[:, i], y_score[:, i])

    points = []
    thresholds_final = dict()
    for i in range(n_classes):
        prec, rec = precision[i], recall[i]
        fscore = (2 * prec * rec) / (prec + rec)
        ix = np.nanargmax(fscore)
        thresholds_final[i] = thresholds[i][ix]
        points.append([rec[ix], prec[ix]])

    d = {names[k]: v for k, v in thresholds_final.items()}

    path = f"data/{mod}/{tag}/" if directory is None else directory

    ds = pd.Series(d)
    ds.to_csv(path + f"thresholds.csv")

    return ds
