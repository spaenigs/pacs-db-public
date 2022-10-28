from sklearn.manifold import TSNE

import pandas as pd
import altair as alt


def run(df, directory):
    X = df.iloc[:, :-1].values
    X_embedded = TSNE(
        n_components=2, learning_rate='auto',
        init='random').fit_transform(X)
    source = pd.DataFrame(X_embedded, columns=["tsne1", "tsne2"])
    source["type"] = df.y
    alt.Chart(source).mark_point(filled=True, opacity=1.0).encode(
        x="tsne1:Q",
        y="tsne2:Q",
        color="type:N"
    ).save(directory + "tsne.html")
