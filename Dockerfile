FROM astrocrpublic.azurecr.io/runtime:3.1-9

COPY requirements.txt .

RUN pip install -r requirements.txt

ENV AIRFLOW__METRICS__STATSD_ON=true
ENV AIRFLOW__METRICS__STATSD_HOST=statsd-exporter
ENV AIRFLOW__METRICS__STATSD_PORT=9125
ENV AIRFLOW__METRICS__STATSD_PREFIX=airflow
ENV AIRFLOW__OPENLINEAGE__TRANSPORT='{"type": "http", "url": "http://marquez:5000", "endpoint": "api/v1/lineage"}'
ENV AIRFLOW__OPENLINEAGE__NAMESPACE=batch_pipeline
ENV AIRFLOW__OPENLINEAGE__DISABLED=false