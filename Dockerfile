
# Airflow 3.3.2 with Conda, dbt and DuckDB
FROM apache/airflow:3.3.2

USER root

# Install Miniconda
ARG MINICONDA_VERSION=py313_25.9.1-1
RUN apt-get update \
    && apt-get install -y --no-install-recommends wget bzip2 ca-certificates \
    && wget -q https://repo.anaconda.com/miniconda/Miniconda3-${MINICONDA_VERSION}-Linux-x86_64.sh -O /tmp/miniconda.sh \
    && bash /tmp/miniconda.sh -b -p /opt/conda \
    && rm /tmp/miniconda.sh \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/conda/bin:$PATH"

# Create Conda environment
RUN conda create -y -n dbt_project python=3.13 \
    && conda clean -afy

# Copy requirements
COPY requirements.txt /tmp/requirements.txt

# Install dbt in its own Conda environment
RUN conda run -n dbt_project pip install --no-cache-dir -r /tmp/requirements.txt

# Install DuckDB in Airflow's Python environment for ingestion
USER airflow

RUN pip install --no-cache-dir duckdb==2.0.0

# Environment variables
ENV DBT_BIN=/opt/conda/envs/dbt_project/bin/dbt
ENV CONDA_ENV=dbt_project
