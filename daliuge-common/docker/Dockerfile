# we are doing a two-stage build to keep the size of
# the final image low.

# First stage build and cleanup
#FROM python:3.8-slim
FROM rayproject/ray:latest-cpu
ARG BUILD_ID
LABEL stage=builder
LABEL build=$BUILD_ID
RUN apt-get update && apt-get install -y gcc

COPY / /daliuge

RUN python -m venv dlg && cd /daliuge && \
    . /dlg/bin/activate && \
    pip install numpy && \
    pip install . && \
    apt-get remove -y gcc


FROM rayproject/ray:latest-cpu
RUN apt-get update && apt-get install -y git
#COPY --from=0 /root/anaconda3/bin/dlg /root/anaconda3/bin/dlg 
#COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/dlg /root/anaconda3/lib/python3.7/site-packages/dlg
#COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/daliuge_common-1.0.0.dist-info \
#            /root/anaconda3/lib/python3.7/site-packages/daliuge_common-1.0.0.dist-info
COPY --from=0 /dlg /dlg
