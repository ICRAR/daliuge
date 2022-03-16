# we are doing a two-stage build to keep the size of
# the final image low.

# First stage build based on daliuge-common and cleanup
ARG VCS_TAG
ARG BUILD_ID
FROM icrar/daliuge-common:${VCS_TAG}
LABEL stage=builder
LABEL build=$BUILD_ID
# all dependencies are already installed in daliuge-common
# RUN apt-get update && \
#     apt-get clean && \
#     apt install -y gcc python3-venv python3-distutils

COPY / /daliuge

RUN . /dlg/bin/activate && \
    cd /daliuge && \
    pip3 install wheel && \
    pip3 install . 

# Second stage build taking what's required from first stage
FROM icrar/daliuge-common:${VCS_TAG}
COPY --from=0 /daliuge/. /daliuge/.
COPY --from=0 /dlg /dlg
# RUN apt-get update && apt-get install -y libmetis-dev python3

# enable the virtualenv path from daliuge-common
ENV VIRTUAL_ENV=/dlg
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

EXPOSE 8084
CMD [ "dlg", "lgweb", "-vv", "-d", "/daliuge/test/dropmake", "-t", "/tmp" ]
