# we are doing a two-stage build to keep the size of
# the final image low.

# First stage build and cleanup
#FROM python:3.8-slim
FROM rayproject/ray:1.3.0-py38
ARG BUILD_ID
LABEL stage=builder
LABEL build=$BUILD_ID
RUN sudo apt-get update && sudo apt-get install -y gcc

COPY / /daliuge

RUN cd && python -m venv dlg && cd /daliuge && \
    . /home/ray/dlg/bin/activate && \
    pip install numpy && \
    pip install . && \
    sudo apt-get remove -y gcc


FROM rayproject/ray:1.3.0-py38
RUN sudo apt-get update && sudo apt-get install -y git
#COPY --from=0 /root/anaconda3/bin/dlg /root/anaconda3/bin/dlg 
#COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/dlg /root/anaconda3/lib/python3.7/site-packages/dlg
#COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/daliuge_common-1.0.0.dist-info \
#            /root/anaconda3/lib/python3.7/site-packages/daliuge_common-1.0.0.dist-info
COPY --from=0 /home/ray/dlg /home/ray/dlg
RUN sudo chown -R ray:users /home/ray/dlg
