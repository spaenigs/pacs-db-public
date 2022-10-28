import pandas as pd

import requests

"""Use this script to test the train and test Rest API."""

for mod in [
    "MR",
    "CT"
]:
    for tag in [
        "SeriesDescription",
        "ProtocolName",
        "Params"
    ]:
        print(mod, tag)

        df = pd.read_csv(f"./sequence-classification/data/{mod}/paths_and_classes.csv")

        df_train = df.iloc[:1500, :]
        classes = list(df_train.y)
        df_train = df_train.drop(["y"], axis=1)

        df_train["path"] = df_train["path"].apply(
            lambda p: p.replace("/str/data/images/ssr_complete", "/ref_images"))

        df = df.drop(["y"], axis=1)

        train_query = {
            "modality": mod,
            "tag": tag,
            "param_grid": {
                'estimator__n_estimators': [50, 100],
                'estimator__max_features': ["sqrt"]},
            "dataset": df_train.to_dict(orient="records"),
            "classes": classes}
        train_response = requests.post(
            'http://localhost:7777/train', json=train_query)
        train_response = train_response.json()

        df_predict = df_train.sample(100, random_state=42)

        predict_query = {
            "modality": mod,
            "tag": tag,
            "model_version": -1,  # use current version
            "dataset": df_predict.to_dict(orient="records")}
        predict_response = requests.post(
            'http://localhost:7777/predict', json=predict_query)
        predict_response = predict_response.json()
