# we are doing a two-stage build to keep the size of
# the final image low.

# First stage build based on daliuge-common and cleanup
FROM icrar/daliuge-common:ray
ARG BUILD_ID
LABEL stage=builder
LABEL build=$BUILD_ID
RUN apt-get update && apt-get install -y gcc && apt-get clean

COPY / /daliuge

RUN . /dlg/bin/activate && \
    cd /daliuge && \
    pip install .

# Second stage build taking what's required from first stage
FROM icrar/daliuge-common:ray
COPY --from=0 /daliuge/. /daliuge/.
COPY --from=0 /dlg /dlg

# enable the virtualenv path from daliuge-common
ENV VIRTUAL_ENV=/dlg
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

EXPOSE 8084
CMD [ "dlg", "lgweb", "-d", "/daliuge/test/dropmake", "-t", "/tmp" ]
