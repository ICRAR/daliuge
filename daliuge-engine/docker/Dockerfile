# We need the base image we build with the other Dockerfile
FROM icrar/daliuge-common:ray
RUN sudo apt-get update && sudo apt-get install -y gcc

RUN pip install --upgrade pip
# Get the DALiuGE sources and install them in the system
COPY / /daliuge
RUN . /home/ray/dlg/bin/activate && cd /daliuge && \
    pip install .

# Second stage build taking what's required from first stage
FROM icrar/daliuge-common:ray

# copy sources and virtualenv
COPY --from=0 /daliuge/. /daliuge/.
COPY --from=0 /home/ray/dlg /home/ray/dlg
RUN sudo mkdir -p /var/dlg_home/workspace && sudo chown -R ray:users /var/dlg_home

EXPOSE 5555
EXPOSE 6666
EXPOSE 8000
EXPOSE 8001
EXPOSE 8002
EXPOSE 9000

# enable the virtualenv path from daliuge-common
ENV VIRTUAL_ENV=/home/ray/dlg
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
ENV DLG_ROOT="/var/dlg_home"
RUN sudo apt-get update && sudo apt-get install -y curl

CMD ["dlg", "daemon", "-vv"]