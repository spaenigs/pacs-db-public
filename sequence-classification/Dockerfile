FROM python:3.9

WORKDIR /server

COPY sequence-classification/requirements.txt .

RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY sequence-classification/* /server/

EXPOSE 7777
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "7777", "--reload"]
