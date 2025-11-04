ARG VCS_TAG
# We need the base image we build with the other Dockerfile
FROM icrar/daliuge-common:${VCS_TAG:-latest}

COPY / /daliuge
RUN . /dlg/bin/activate && pip install wheel && cd /daliuge && \
    pip install . 

EXPOSE 9000
EXPOSE 5555
EXPOSE 6666
EXPOSE 8000
EXPOSE 8001
EXPOSE 8002

# enable the virtualenv path from daliuge-common
ENV VIRTUAL_ENV=/dlg
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
ENV DLG_ROOT="/tmp/dlg"
# We start the Node Manager daemon
CMD ["dlg", "daemon", "-vvv","--mm"]
