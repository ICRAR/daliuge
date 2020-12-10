# we are doing a two-stage build to keep the size of
# the final image low.

# First stage build and cleanup
#FROM python:3.8-slim
FROM rayproject/ray:latest-cpu
ARG BUILD_ID
LABEL stage=builder
LABEL build=$BUILD_ID
RUN apt-get update && apt-get install -y gcc
RUN pip install numpy

COPY / /daliuge

RUN cd /daliuge && \
    pip install .


# Second stage build taking what's required from first stage
# FROM alpine
# RUN apk add --update python3 db sqlite
# COPY VERSION /home/ngas/VERSION
# COPY startServer.sh /home/ngas/startServer.sh
# COPY --from=0 /home/ngas/. /home/ngas/.
# COPY --from=0 /usr/bin/. /usr/bin/.
# COPY --from=0 /usr/lib/python3.8/site-packages/. /usr/lib/python3.8/site-packages/.
# COPY --from=0 /NGAS/. /NGAS/.
# RUN sed -i 's/127.0.0.1/0.0.0.0/g' /NGAS/cfg/ngamsServer.conf
# Second stage build taking what's required from first stage
#FROM python:3.8-slim
FROM rayproject/ray:latest-cpu
RUN apt-get update && apt-get install -y git
# COPY --from=0 /usr/local/lib/python3.8/site-packages/. /usr/local/lib/python3.8/site-packages/.
COPY --from=0 /root/anaconda3/. /root/anaconda3/.
# COPY --from=0 /usr/local/bin/. /usr/local/bin/.
# COPY --from=0 /daliuge/. /daliuge/.
