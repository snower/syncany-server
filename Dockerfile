FROM python:3.10-slim

WORKDIR /config

RUN apt-get update && apt-get install -y ca-certificates git

COPY requirements.txt /root/
COPY examples/mall/ /config/

RUN cd /root && \
    sed -i '/Items below this point will not be included in the Docker Image/,$d' requirements.txt && \
    python -m pip install --upgrade pip && \
	pip install --no-cache-dir -r requirements.txt && \
    pip install git+https://github.com/snower/syncany.git#egg=syncany && \
    pip install git+https://github.com/snower/syncany-sql.git#egg=syncanysql && \
    pip install git+https://github.com/snower/syncany-server.git#egg=syncanyserver && \
    cd /config

CMD syncany-server