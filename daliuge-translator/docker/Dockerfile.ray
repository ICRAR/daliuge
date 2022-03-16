# we are doing a two-stage build to keep the size of
# the final image low.
ARG VCS_TAG
# First stage build based on daliuge-common and cleanup
FROM icrar/daliuge-common:${VCS_TAG:-latest}
ARG BUILD_ID
LABEL stage=builder
LABEL build=$BUILD_ID
RUN sudo apt-get update && sudo apt-get install -y python3-pip gcc && sudo apt-get clean

COPY / /daliuge

RUN . ~/dlg/bin/activate && \
    cd /daliuge && \
    pip install . 

# Second stage build taking what's required from first stage
FROM icrar/daliuge-common:${VCS_TAG:-latest}
COPY --from=0 /daliuge/. /daliuge/.
COPY --from=0 /home/ray/dlg /home/ray/dlg

# enable the virtualenv path from daliuge-common
ENV VIRTUAL_ENV=/home/ray/dlg
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

EXPOSE 8084
CMD [ "dlg", "lgweb", "-vv", "-d", "/daliuge/test/dropmake", "-t", "/tmp" ]
