# Build a docker image 
# Merging the ray engine (https://docs.ray.io/en/master/installation.html)
# and DALiuGE
FROM rayproject/ray:latest-cpu
RUN sudo apt update && sudo apt install -y gcc && sudo test -e daliuge || sudo git clone https://github.com/pritchardn/daliuge.git
RUN cd daliuge/daliuge-common && pip install . \
    && cd ../daliuge-engine && pip install . \
    && sudo rm -rf /root/anaconda3/lib/python3.7/site-packages/azure \
    && sudo apt-get remove cmake gcc -y \
    && sudo apt-get clean

CMD ["dlg", "daemon", "-vv", "--no-nm"]