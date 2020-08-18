# Translator Docker Container

We currently build the DALiuGE translator in a single stand-alone image called *icrar/daliuge-translator:latest*.

## Building the image
Pre-requisite for building and running the docker image is to have docker running on the host.
To build the image run the script 
```./build_translator.sh```

## Starting the DALiuGE Translator Daemon
The *icrar/daliuge-translator:latest* image can be started using the 
```./run_translator.sh``` 
script. This will start the image in interactive mode, means that the logs from the DALiuGE translator are displayed on the screen.

## Using the Translator
The translator is a RESTful service, which is not meant to be used directly, but it is possible to use *curl* to submit a logical graph to be translated.
