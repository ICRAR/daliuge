# We need the base image we build with the other Dockerfile
FROM dfms/centos7:base

# Get the DFMS sources and install them in the system
RUN git clone https://github.com/SKA-ScienceDataProcessor/dfms ~/dfms && \
    cd ~/dfms && git checkout origin/spead2 && \
    python setup.py install
