# we are doing a two-stage build to keep the size of
# the final image low.

# First stage build and cleanup
#FROM python:3.8-slim
FROM ubuntu:22.04
ARG BUILD_ID
LABEL stage=builder
LABEL build=$BUILD_ID
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt-get update && \
    apt-get install -y avahi-daemon avahi-utils gcc python3 python3-venv python3-pip python3-appdirs libmetis-dev curl git sudo orphan-sysvinit-scripts && \
    apt-get clean

COPY / /daliuge

RUN cd / && python3 -m venv dlg && cd /daliuge && \
    . /dlg/bin/activate && \
    pip install wheel numpy && \
    pip install .

# we don't clean this up, will be done in the derived images
