#
FROM python:3.9

#
WORKDIR /server

#
COPY gui-backend/requirements.txt .

#
RUN pip install --no-cache-dir --upgrade -r requirements.txt

#
COPY gui-backend/* /server/

#
EXPOSE 8000
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
