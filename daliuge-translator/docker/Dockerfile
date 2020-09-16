# we are doing a two-stage build to keep the size of
# the final image low.

# First stage build based on daliuge-common and cleanup
FROM icrar/daliuge-common:latest
ARG BUILD_ID
LABEL stage=builder
LABEL build=$BUILD_ID
RUN apt-get update && apt-get install -y gcc && apt-get clean

COPY / /daliuge/translator

RUN cd /daliuge/translator && \
    pip install .

# Second stage build taking what's required from first stage
FROM icrar/daliuge-common:latest
COPY --from=0 /usr/local/lib/python3.8/site-packages/. /usr/local/lib/python3.8/site-packages/.
COPY --from=0 /usr/local/bin/. /usr/local/bin/.
COPY --from=0 /daliuge/. /daliuge/.

EXPOSE 8084
CMD [ "dlg", "lgweb", "-d", "/daliuge/translator/test/dropmake/logical_graphs", "-t", "/tmp" ]
