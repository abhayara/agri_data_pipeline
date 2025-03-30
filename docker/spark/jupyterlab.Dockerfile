FROM cluster-base

# -- Layer: JupyterLab

ARG spark_version=3.3.1
ARG jupyterlab_version=3.6.1

RUN apt-get update -y && \
    apt-get install -y python3-pip python3-venv python3-full && \
    python3 -m venv /opt/venv && \
    . /opt/venv/bin/activate && \
    pip install wget pyspark==${spark_version} jupyterlab==${jupyterlab_version}

# Add the virtual environment to PATH
ENV PATH="/opt/venv/bin:${PATH}"

# -- Runtime

EXPOSE 8888
WORKDIR ${SHARED_WORKSPACE}
CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=