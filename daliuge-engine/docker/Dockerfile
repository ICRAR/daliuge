# We need the base image we build with the other Dockerfile
FROM icrar/daliuge-common:ray
RUN apt-get update && apt-get install -y gcc
RUN pip install --upgrade pip
# Get the DALiuGE sources and install them in the system
COPY / /daliuge
RUN cd /daliuge && \
    pip install .

# Second stage build taking what's required from first stage
FROM icrar/daliuge-common:ray
# COPY --from=0 /usr/local/lib/python3.8/site-packages/. /usr/local/lib/python3.8/site-packages/.
#COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/. /root/anaconda3/lib/python3.7/site-packages/.
COPY --from=0 /root/anaconda3/bin/. /root/anaconda3/bin/.
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/_version.py /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/bottle-0.12.19.dist-info /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/bottle.py /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/configobj-5.0.6.dist-info /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/configobj.py /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/crc32c-2.2.dist-info /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/crc32c.cpython-37m-x86_64-linux-gnu.so /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/daemon /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/daliuge_common-1.0.0.dist-info /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/daliuge_engine-1.0.0.dist-info /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/dill /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/dill-0.3.3.dist-info /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/dlg /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/docker /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/docker-4.4.0.dist-info /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/gevent /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/gevent-20.9.0.dist-info /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/greenlet-0.4.17.dist-info /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/greenlet.cpython-37m-x86_64-linux-gnu.so /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/ifaddr /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/ifaddr-0.1.7.dist-info /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/lib64_dist.pth /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/lockfile /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/lockfile-0.12.2.dist-info /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/netifaces-0.10.9.dist-info /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/netifaces.cpython-37m-x86_64-linux-gnu.so /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/python_daemon-2.2.4.dist-info /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/pyzmq-20.0.0.dist-info /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/pyzmq.libs /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/scp-0.13.3.dist-info /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/scp.py /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/validate.py /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/websocket /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/websocket_client-0.57.0.dist-info /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/zeroconf /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/zeroconf-0.28.6.dist-info /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/zerorpc /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/zerorpc-0.6.3.dist-info /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/zmq /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/zope /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/zope.event-4.5.0-py3.6-nspkg.pth /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/zope.event-4.5.0.dist-info /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/zope.interface-5.2.0-py3.7-nspkg.pth /anaconda3/lib/python3.7/site-packages/
# COPY --from=0 /root/anaconda3/lib/python3.7/site-packages/zope.interface-5.2.0.dist-info /anaconda3/lib/python3.7/site-packages/

#COPY --from=0 /daliuge/. /daliuge/.

EXPOSE 5555
EXPOSE 6666
EXPOSE 8000
EXPOSE 8001
EXPOSE 8002
EXPOSE 9000


CMD ["dlg", "daemon", "-vv", "--no-nm"]