FROM astrocrpublic.azurecr.io/runtime:3.1-9

COPY requirements.txt .

RUN pip install -r requirements.txt

ENV AIRFLOW__METRICS__STATSD_ON=True
ENV AIRFLOW__METRICS__STATSD_HOST=otel-collector
ENV AIRFLOW__METRICS__STATSD_PORT=8125
ENV AIRFLOW__METRICS__STATSD_PREFIX=airflow