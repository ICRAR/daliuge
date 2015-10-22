# We need centos 7 if we want gcc 4.8
FROM centos:7

# Install all required packages
RUN yum -y update && yum -y install \
    boost-devel \
    boost-python \
    boost-system \
    gcc-c++ \
    git \
    openssh-server \
    openssh-clients \
    python-devel && \
    curl "https://bootstrap.pypa.io/get-pip.py" -o "get-pip.py" && \
    python get-pip.py

# Add the dfms user and create its private/public key pair
# Also setup the SSH host keys
RUN ssh-keygen -A && \
    adduser --uid 1000 dfms

COPY dfms_docker.pem.pub /home/dfms/.ssh/authorized_keys
RUN chmod 755 /home/dfms/.ssh && \
    chown -R dfms.dfms /home/dfms/.ssh
