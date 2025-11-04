# we are doing a two-stage build to keep the size of
# the final image low.

# First stage build based on daliuge-common and cleanup
ARG VCS_TAG
#ARG BUILD_ID
FROM icrar/daliuge-common:${VCS_TAG:-latest}
LABEL stage=builder
#LABEL build=$BUILD_ID

# All dependencies are already installed in daliuge-common

COPY / /daliuge

RUN . /dlg/bin/activate && \
    cd /daliuge && \
    pip3 install wheel && \
    pip3 install . 

# Second stage build taking what's required from first stage
FROM icrar/daliuge-common:${VCS_TAG:-latest}
COPY --from=0 /daliuge/. /daliuge/.
COPY --from=0 /dlg /dlg

# enable the virtualenv path from daliuge-common
ENV VIRTUAL_ENV=/dlg
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

EXPOSE 8084
CMD [ "dlg", "tm", "-vv", "-d", "/daliuge/test/dropmake", "-t", "/tmp" ]
