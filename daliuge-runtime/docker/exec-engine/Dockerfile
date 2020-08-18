# We need the base image we build with the other Dockerfile
FROM icrar/daliuge-base:latest

# Get the DFMS sources and install them in the system
RUN git clone https://github.com/ICRAR/daliuge ~/daliuge && \
    cd ~/daliuge && \
    pip install .

# Second stage build taking what's required from first stage
FROM icrar/daliuge-base:latest
COPY --from=0 /usr/local/lib/python3.8/site-packages/. /usr/local/lib/python3.8/site-packages/.
COPY --from=0 /usr/local/bin/. /usr/local/bin/.
COPY --from=0 /daliuge/. /daliuge/.

EXPOSE 8000
EXPOSE 9000

CMD ["dlg", "daemon", "-vv", "--no-nm"]