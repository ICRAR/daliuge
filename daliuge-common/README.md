# daliuge-base Docker Image

The DALiuGE base image contains common code for both the translator and also the DALiuGE execution engine. It is build and distributed as an image called *icrar/daliuge-base:latest*.

## Building the image

Pre-requisite for building and running the docker image is to have docker running on the host. The translator image is dependent on the icrar/daliuge-base image. If that is not available locally it will be installed from the ICRAR dockerHub repository.

To build the base image run the script

```bash
./build_common.sh
```

## Using the daliuge-base image

The daliuge-base image is not meant to be used stand-alone.
