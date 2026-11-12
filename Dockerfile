FROM python:3.13-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PREFECT_SERVER_ANALYTICS_ENABLED=false DO_NOT_TRACK=1
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY pipeline pipeline
COPY prefect prefect
COPY dbt dbt
COPY great_expectations great_expectations
COPY data/sample data/sample
CMD ["python", "prefect/dags/etl_pipeline.py"]
