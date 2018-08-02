FROM python:2.7-stretch

COPY requirements.txt /tmp/
RUN pip install -U pip
RUN pip install -r /tmp/requirements.txt
COPY ./bigquery_archiver.py bigquery_archiver.py

CMD ["python", "bigquery_archiver.py"]