FROM apache/airflow:2.7.3-python3.11

COPY requirements.txt /requirements.txt
RUN pip install --user --no-cache-dir -r /requirements.txt

USER root
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
USER airflow

ENTRYPOINT ["/entrypoint.sh"]