FROM quay.io/jupyter/minimal-notebook:2024-02-26

ENV PATH=$PATH:$HOME/.cargo/bin

USER root

RUN apt-get update \
 && apt install -y clang postgresql-server-dev-14 cmake libgeos-dev libjson-c-dev libproj-dev libgsl-dev

USER jovyan

RUN curl https://sh.rustup.rs -sSf | sh -s -- --profile minimal -y

RUN cargo install evcxr_jupyter

RUN mkdir http

RUN evcxr_jupyter --install

# --

USER root

RUN curl -L -o simple-http-server https://github.com/TheWaWaR/simple-http-server/releases/download/v0.6.9/x86_64-unknown-linux-musl-simple-http-server \
 && chmod +x simple-http-server \
 && mv simple-http-server /usr/local/bin

RUN echo "simple-http-server -p 8889 --cors /home/jovyan/http &" > /usr/local/bin/before-notebook.d/20start-http-server.sh \
 && chmod +x /usr/local/bin/before-notebook.d/20start-http-server.sh

COPY mobdb.sh /usr/local/bin

RUN mobdb.sh /usr/bin /tmp

USER jovyan

RUN pip install git+https://github.com/jw3/example-jupyter-extension

RUN conda install -c conda-forge -y psycopg2 "pgspecial<2" jupyter-resource-usage

RUN pip install jupysql polars pyarrow pandas

ADD http://worldclockapi.com/api/json/utc/now /tmp/time

RUN pip install --upgrade --force-reinstall git+https://github.com/jw3/example-jupyter-extension
