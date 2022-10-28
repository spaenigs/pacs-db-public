FROM apache/airflow:2.3.2
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install \
    pymongo==3.11 \
    more_itertools==8.12.0 \
    dicom-parser==1.2.3 \
    pydevd_pycharm==222.3048.9 \
    pydicom==2.3.0 \
    pynetdicom==2.0.2 \
    scikit-learn==1.0 \
    altair==4.1.0 \
    python-dotenv==0.21.0
