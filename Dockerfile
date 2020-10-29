# Build a docker image 
# Merging the ray engine (https://docs.ray.io/en/master/installation.html)
# and DALiuGE
FROM rayproject/ray:latest-cpu
RUN apt update && apt install -y gcc && test -e daliuge || git clone --branch ray_test https://github.com/ICRAR/daliuge.git
RUN cd daliuge/daliuge-common && pip install . \
    && cd ../daliuge-engine && pip install . \
    && rm -rf /root/anaconda3/lib/python3.7/site-packages/azure \
    && apt-get remove cmake gcc -y \
    && apt-get clean 