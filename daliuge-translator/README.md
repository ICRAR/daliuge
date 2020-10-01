# Docker containers

We currently build the translator in a single, separate image in order to enable a deployment on machines completely seperate from the EAGLE editor and the runtime environment.

## Building the translator image

Just execute the shell script:
```
./build_translator.sh
```

## Starting the DALiuGE Translator Daemon
The *icrar/daliuge-translator:latest* image can be started using 

```
./run_translator.sh
````

 This will start the image in interactive mode, means that the logs from the daemon are displayed on the screen, which is good for debugging sessions. The RESTful interface is mapped to http://localhost:8084 by default and that address can be configured in the EAGLE editor.

## Usage
### Stand alone
The DALiuGE translator is not meant to be used stand-alone, but since it is exposing a RESTful interface it can be called using e.g. curl.

### EAGLE
The DALiuGE translator is integrated with the EAGLE (https://github.com/ICRAR/EAGLE) graphical workflow editor. EAGLE has an interface to the DALiuGE translator and users can submit logical graphs to the translator and retrieve the resulting physical graph templates back.
