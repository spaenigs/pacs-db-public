from typing import Dict, List, Optional
from fastapi import FastAPI, Response
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from utils.encode import ParamEncoderMR, ParamEncoder, TextEncoder
from scipy import sparse
from sklearn.ensemble import RandomForestClassifier
from sklearn.multiclass import OneVsRestClassifier
from sklearn.preprocessing import label_binarize
from glob import glob
from os.path import isfile
from mimetypes import guess_type

import ml.parameter_optimization as parameter_optimization
import ml.cross_validation as cross_validation
import ml.optimal_threshold as optimal_threshold
import ml.tsne as tsne

import pandas as pd
import numpy as np

import joblib
import logging


logging.basicConfig(
    level=logging.DEBUG, # logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()])


default_param_grid = {
    'estimator__n_estimators': [50, 100, 150, 200],
    'estimator__max_features': ["sqrt", "log2", 100, 0.5],
    'estimator__max_depth':
      [int(x) for x in np.linspace(10, 110, num=5)] + [None],
}


class BaseQuery(BaseModel):
    tag: str
    modality: str


class Query(BaseQuery):
    dataset: List[dict]


class Training(Query):
    classes: List[str]
    param_grid: Optional[dict] = default_param_grid
    cpu_cores: Optional[int] = -1


class Prediction(Query):
    model_version: Optional[int] = -1
    return_features: Optional[bool] = False


class TrainingResponse(BaseModel):
    model_path: str
    thresholds_path: str
    vectorizer_path: str
    encoded_dataset_path: str
    encoded_dataset: List[dict]


class PredictionResponse(BaseModel):
    prediction_dataset: List[dict]


def get_current_version(working_dir) -> int:
    """Get current model version."""
    version_dirs = glob(working_dir + "v*/")
    if len(version_dirs) > 0:
        model_versions = [
            n.split("/")[-2].replace("v", "") for n in version_dirs]
        model_versions = sorted([int(mv) for mv in model_versions])
        return int(model_versions[-1])
    else:
        return 0


app = FastAPI()

tags = ["SeriesDescription", "ProtocolName", "Params"]


@app.post("/model_versions")
def get_model_versions(query: BaseQuery):
    """Get a list of available model versions."""
    working_dir = f"/server/data/{query.modality}/{query.tag}/"
    return list(range(1, get_current_version(working_dir) + 1))


@app.post("/train", response_model=TrainingResponse)
def train(query: Training):
    """
    Rest API endpoint to trigger model training. Training data should be specified in query.dataset either using
    the 'path' column pointing to DICOM images or a column denoted as query.tag which contains the extracted
    training data. Allows only one training at the same time.
    """

    df = pd.DataFrame(query.dataset)
    df["y"] = query.classes
    df = df.loc[df.y != "MR DWI (4mm, optional b0)"]  # too less training data, ignore for now

    base_dir = f"/server/data/{query.modality}/{query.tag}/"

    current_version = get_current_version(base_dir)
    new_version = current_version + 1

    working_dir = base_dir + f"v{new_version}/"

    logging.info("Encoding dataset...")

    if query.tag in tags[:2]:
        encoder = TextEncoder(
            df=df, modality=query.modality, tag=query.tag,
            directory=working_dir)
        encoder.create_corpus()
        encoder.encode().dump_vectorizer()
    else:
        _ParamEncoder = ParamEncoder if query.modality == "CT" else ParamEncoderMR
        encoder = _ParamEncoder(df, directory=working_dir)
        encoder.encode()

    encoder.dump_dataset()

    logging.info("Running dimensionality reduction (t-SNE)...")

    tsne.run(encoder.df_encoded, working_dir)

    logging.info("Running hyperparameter optimization...")

    df_cv = parameter_optimization.run(
        encoder.df_encoded, mod=query.modality, tag=query.tag,
        cpu_cores=query.cpu_cores, param_grid=query.param_grid,
        directory=working_dir)

    parameter_optimization.plot(df_cv, directory=working_dir)

    logging.info("Running cross-validation...")

    optimal_params = df_cv\
        .loc[(df_cv["mod"] == query.modality) & (df_cv["tag"] == query.tag) &
            (df_cv["rank_test_score"] == 1), "params"]\
        .to_list()[0]

    cross_validation.run(
        encoder.df_encoded, mod=query.modality, tag=query.tag,
        cpu_cores=query.cpu_cores, directory=working_dir,
        **optimal_params)

    logging.info("Optimizing thresholds...")

    optimal_threshold.run(
        encoder.df_encoded, mod=query.modality, tag=query.tag,
        cpu_cores=query.cpu_cores, directory=working_dir,
        **optimal_params)

    logging.info("Training final model...")

    iclf = RandomForestClassifier(
        random_state=0, n_jobs=query.cpu_cores, **optimal_params)
    classifier = OneVsRestClassifier(iclf, n_jobs=query.cpu_cores)

    X, y = encoder.X, encoder.y
    y = label_binarize(y, classes=sorted(set(y)))

    classifier = classifier.fit(X, y)

    file_name = working_dir + f"classifier.joblib"
    joblib.dump(classifier, file_name)

    logging.info("Done.")

    return {
        "model_path": file_name,
        "thresholds_path": working_dir + f"thresholds.csv",
        "vectorizer_path": working_dir + f"vectorizer.joblib",
        "encoded_dataset_path": working_dir + "dataset.csv",
        "encoded_dataset": encoder.df_encoded.to_dict(orient="records")}


@app.post("/predict", response_model=PredictionResponse)
async def predict(query: Prediction):
    """
    Rest API endpoint to run prediction using a specific model. Prediction data should be specified in the
    query.dataset using a column denoted as query.tag which contains the extracted training data.
    """

    df = pd.DataFrame(query.dataset)

    logging.info("Loading trained models...")

    base_dir = f"/server/data/{query.modality}/{query.tag}/"

    current_version = \
        get_current_version(base_dir) if query.model_version == -1 else \
        query.model_version

    working_dir = base_dir + f"v{current_version}/"

    df_thresholds = pd.read_csv(
        working_dir + f"thresholds.csv", index_col=0)
    df_thresholds.columns = ["cutoff"]

    classifier = joblib.load(working_dir + f"classifier.joblib")

    logging.info("Encoding dataset...")

    if query.tag in tags[:2]:
        vectorizer = joblib.load(working_dir + f"vectorizer.joblib")
        encoder = TextEncoder(
            df=df, modality=query.modality, tag=query.tag,
            directory=working_dir)
        encoder.create_corpus()
        col_names_out = list(vectorizer.get_feature_names_out())
        X_unknown = vectorizer.transform(encoder.corpus)
    else:
        _ParamEncoder = ParamEncoder if query.modality == "CT" else ParamEncoderMR
        encoder = _ParamEncoder(df, directory=working_dir)
        encoder.encode()
        col_names_out = encoder.cols
        X_unknown = sparse.csc_matrix(encoder.X)

    logging.info("Predicting unknown labels...")

    y_pred_classes = classifier.predict(X_unknown)
    names = sorted(set(df_thresholds.index.to_list()))

    y_pred = []
    for pred_cl in y_pred_classes:
        if sum(pred_cl) == 0 or sum(pred_cl) > 1:
            y_pred.append("Uncertain")
        else:
            y_pred.append(names[np.argmax(pred_cl)])

    y_pred_scores = classifier.predict_proba(X_unknown)

    y_prob = []
    for pred_cl in y_pred_scores:
        y_prob.append(np.max(pred_cl))

    if query.return_features:
        df_res = pd.DataFrame.sparse.from_spmatrix(X_unknown, columns=col_names_out)
        df_res.columns = col_names_out
        df_res["y"] = y_pred
        df_res["confidence"] = y_prob
    else:
        df_res = pd.DataFrame({"y": y_pred, "confidence": y_prob})

    logging.info("Done.")

    return {"prediction_dataset": df_res.to_dict(orient="records")}


@app.get("/show", response_class=HTMLResponse)
def show_results():
    """
    Rest API endpoint to show the results . Each visualization is an HTML file, embedded in an <iframe>. Simple
    <select>-fields allow the selection of the respective modality and tag.
    """
    data = """
<link rel="icon" href="data:,">
<html>
    <select id="mod" onchange="mod=this.value;" autocomplete="off">
      <option value="MR" selected>MR</option>
      <option value="CT">CT</option>
    </select>
     <select id="tag" onchange="tag=this.value;" autocomplete="off">
      <option value="SeriesDescription" selected>Series Description</option>
      <option value="ProtocolName">Protocol Name</option>
      <option value="Params">Params (image header)</option>
    </select>
    <button onclick="setURLs();">Show</button>
    <br>
    <br>
    <iframe id="ifr_nested" width="100%" height="50%" frameBorder="0"></iframe>
    <iframe id="ifr_res" width="100%" height="100%" frameBorder="0"></iframe>
</html>
<script>
    let mod = "MR"
    let tag = "SeriesDescription"
    function setURLs() {
        let url_res = `show_result?modality=${mod}&tag=${tag}&filename=res`
        document.getElementById('ifr_res').src = url_res
        let url_nested = `show_result?modality=${mod}&tag=${tag}&filename=nested_cv`
        document.getElementById('ifr_nested').src = url_nested
    }
    setURLs();
</script>
"""
    return Response(content=data, media_type="text/html")


@app.get("/show_result")
def show_results(modality: str, tag: str, filename: str):
    """Loads the result files and returns them as HTML."""
    base_path = f"/server/data/{modality}/{tag}/"
    current_version = get_current_version(base_path)
    working_dir = base_path + f"v{current_version}/"
    file_path = working_dir + f"{filename}.html"
    if not isfile(file_path):
        return Response(status_code=404)
    with open(file_path) as f:
        content = f.read()
    content_type, _ = guess_type(file_path)
    return Response(content, media_type=content_type)