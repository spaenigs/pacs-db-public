import warnings

warnings.filterwarnings("ignore", category=UserWarning)

from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix, matthews_corrcoef
from sklearn.model_selection import StratifiedKFold
from sklearn.multiclass import OneVsRestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import label_binarize
from sklearn.metrics import precision_recall_curve
from sklearn.metrics import average_precision_score

import pandas as pd
import numpy as np
import altair as alt

import logging


def run_cv(df, mod, tag, cpu_cores=-1, **optimal_params):
    """Runs a 10-fold stratified cross-validation and returns the result as a confusion matrix."""

    X, y = df.iloc[:, :-1].values, df.y.values

    n_splits = 10
    y_test_all, y_pred_all = [], []
    for idx, (train_index, test_index) in enumerate(
            StratifiedKFold(n_splits=n_splits, shuffle=True).split(X, y), start=1):
        logging.debug(f"Training split {idx}/{n_splits} --- {optimal_params}")
        X_train, X_test = X[train_index], X[test_index]
        y_train, y_test = y[train_index], y[test_index]
        iclf = RandomForestClassifier(
            random_state=0, n_jobs=cpu_cores, **optimal_params)
        clf = OneVsRestClassifier(
            iclf, n_jobs=cpu_cores).fit(X_train, y_train)
        y_pred = clf.predict(X_test)
        y_test_all.extend(y_test)
        y_pred_all.extend(y_pred)

    names = sorted(list(set(y_test_all)))
    cm = confusion_matrix(y_test_all, y_pred_all, labels=names)
    x_mesh, y_mesh = np.meshgrid(names, names)
    source = pd.DataFrame({
        'x': x_mesh.ravel(), 'y': y_mesh.ravel(), 'z': cm.ravel(), "tag": tag, "modality": mod})

    hm = alt.Chart(
        source,
        title="Confusion matrix"
    ).mark_rect().encode(
        x=alt.X('x:O', title="Predicted label"),
        y=alt.Y('y:O', title="True label"),
        color=alt.Color('z:Q', title="#Images", scale=alt.Scale(scheme="Blues"))
    ).properties(
        height=300,
        width=300
    )

    text = alt.Chart(source).mark_text().encode(
        x='x:O',
        y='y:O',
        text='z:Q'
    ).properties(
        height=300,
        width=300
    )

    return hm + text


def compute_pr_curve(df, cpu_cores=-1, **optimal_params):
    """Compute the precision-recall curve."""

    X, y = df.iloc[:, :-1].values, df.y.values

    n_classes = len(set(y))
    names = list(set(y))

    y = label_binarize(y, classes=list(set(y)))

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.5, random_state=0)

    iclf = RandomForestClassifier(
        random_state=0, n_jobs=cpu_cores, **optimal_params)
    classifier = OneVsRestClassifier(iclf, n_jobs=cpu_cores)

    y_score = classifier.fit(X_train, y_train).predict_proba(X_test)

    precision = dict()
    recall = dict()
    thresholds = dict()
    average_precision = dict()
    for i in range(n_classes):
        precision[i], recall[i], thresholds[i] = \
            precision_recall_curve(y_test[:, i], y_score[:, i])
        average_precision[i] = \
            average_precision_score(y_test[:, i], y_score[:, i])

    points = []
    indices = dict()
    for i in range(n_classes):
        prec, rec = precision[i], recall[i]
        fscore = (2 * prec * rec) / (prec + rec)
        ix = np.nanargmax(fscore)
        indices[i] = ix
        points.append([rec[ix], prec[ix]])

    res = []
    for i in range(n_classes):
        y_pred_class = [1 if scores[i] > 0.5 else 0 for scores in y_score]
        mcc_before = matthews_corrcoef([e[i] for e in y_test], y_pred_class)
        threshold = thresholds[i][indices[i]]
        y_pred_class = [1 if scores[i] > threshold else 0 for scores in y_score]
        mcc_after = matthews_corrcoef([e[i] for e in y_test], y_pred_class)
        res.append([names[i], mcc_before, "before"])
        res.append([names[i], mcc_after, "after"])

    df_res = pd.DataFrame(res, columns=["class", "mcc", "thresholding"])

    source = pd.DataFrame()
    for i in range(n_classes):
        tmp = pd.DataFrame(
            {"x": recall[i], "y": precision[i],
             "class": [names[i]] * len(recall[i])})
        source = pd.concat([source, tmp])

    c1 = alt.Chart(
        source,
        title="Precision vs. recall"
    ).mark_line().encode(
        x=alt.X("x:Q", axis=alt.Axis(title="Recall")),
        y=alt.Y("y:Q", axis=alt.Axis(title="Precision")),
        color=alt.Color("class:N", scale=alt.Scale(
            range=['#7fc97f', '#beaed4', '#fdc086',
                   '#ffff99', '#386cb0', '#f0027f', '#bf5b17'],
            domain=names
        ))
    )

    c2 = alt.Chart(pd.DataFrame(points, columns=["x", "y"])).mark_point(
        color="black", size=50, filled=True, opacity=1.0
    ).encode(
        x=alt.X("x:Q"),
        y=alt.Y("y:Q"),
    )

    c3 = alt.Chart(
        df_res,
        title="Threshold optimization"
    ).mark_bar().encode(
        x=alt.X("thresholding:N", title=None, sort=alt.SortArray(["before", "after"])),
        y="mcc:Q",
        color="class:N",
        column=alt.Column(
            "class:N", title=None, header=alt.Header(labels=False), spacing=5)
    )

    return (c1 + c2) | c3


def run(df, mod, tag, directory=None, **optimal_params):
    """
    Runs a 10-fold stratified cross-validation. The result are visualized as a confusion matrix and precision-recall
    curve.
    """
    c1 = run_cv(df, mod, tag, **optimal_params)
    c2 = compute_pr_curve(df, **optimal_params)
    path = f"data/{mod}/{tag}/" if directory is None else directory
    alt.hconcat(
        c1, c2, title=f"{mod}, {tag}, {optimal_params}"
    ).save(path + "res.html")
