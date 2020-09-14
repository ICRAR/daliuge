# daliuge-translator Docker Image

We currently build the DALiuGE translator in a single stand-alone image called *icrar/daliuge-translator:latest*.

## Building the image

Pre-requisite for building and running the docker image is to have docker running on the host. The translator image is dependent on the icrar/daliuge-base image. If that is not available locally it will be installed from the ICRAR dockerHub repository.

To build the translator image run the script

```bash
./build_translator.sh
```

## Starting the DALiuGE Translator Daemon

The *icrar/daliuge-translator:latest* image can be started using the

```bash
./run_translator.sh
```

script. This will start the image in interactive mode, means that the logs from the DALiuGE translator are displayed on the screen.

## Using the Translator

The translator is a RESTful service, which is not meant to be used directly, but it is possible to use *curl* to submit a logical graph to be translated.
