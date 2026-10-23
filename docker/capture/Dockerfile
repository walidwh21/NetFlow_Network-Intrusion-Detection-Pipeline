FROM ubuntu:22.04
RUN apt-get update && apt-get install -y --no-install-recommends \
        tcpdump nfdump inotify-tools python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*
RUN pip3 install --no-cache-dir kafka-python pandas
COPY docker/capture/flow_exporter.py /opt/flow_exporter.py
COPY docker/capture/capture.sh /opt/capture.sh
CMD ["/opt/capture.sh"]
