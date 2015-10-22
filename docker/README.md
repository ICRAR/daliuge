# Docker containers

We currently support two images:
 * *dfms/centos7:base* includes a CentOS 7 system with a 'dfms' user and all the requirements to install the dfms framework installed.
 * *dfms/centos7:latest* is built on top of the :base image and includes the installation of the dfms framework

This way we try to separate the pre-requirements of dfms from the framework installation, which is more dynamic. The idea is then to rebuild only the :latest image as needed when new versions of the framework need to be deployed, and not build it from scratch each time.

Most of the dependencies included in :base do not belong to the dfms framework itself, but rather to its requirements (mainly to the spead2 communication protocol). Once we move out the spead2 application from this repository (and therefore the dependency of dfms on spead2) we'll re-organize these Dockerfiles to have a base installation of the dfms framework, and then build further images on top of that base image containing specific applications with their own system installation requirements.
