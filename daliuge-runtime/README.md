# Docker containers

We currently build the system in two images:
 * *icrar/daliuge-base:latest* includes a CentOS 7 system with a 'dfms' user and all the requirements to install the dfms framework installed.
 * *icrar/daliuge-engine:latest* is built on top of the :base image and includes the installation of the DALiuGE framework.

This way we try to separate the pre-requirements of the daliuge engine from the framework installation, which is more dynamic. The idea is then to rebuild only the daliuge-engine image as needed when new versions of the framework need to be deployed, and not build it from scratch each time.

Most of the dependencies included in :base do not belong to the DALiuGE framework itself, but rather to its requirements (mainly to the spead2 communication protocol). Once we move out the spead2 application from this repository (and therefore the dependency of dfms on spead2) we'll re-organize these Dockerfiles to have a base installation of the dfms framework, and then build further images on top of that base image containing specific applications with their own system installation requirements.

The *daliuge-engine* image by default runs a generic daemon, which allows to then start the Master Manager, Node Manager or DataIsland Manager. This approach allows to change the actual manager deployment configuration in a more dynamic way and adjusted to the actual requirements of the environment.

## Building the docker images
If starting from scratch you need to build both the base and the runtime image. This can be achieved by using the shell scripts provided in the same directory as this README file:
```
./build_base.sh
````
followed by
```
./build_engine.sh
```

## Starting the DALiuGE Engine Daemon
The *icrar/daliuge-engine:latest* image can be started using the *run_engine.sh* script:
```
./run_engine.sh
```
This will start the image in interactive mode, means that the logs from the DALiuGE daemon are displayed on the screen.

## Starting managers
In a typical real-world scenario DALiuGE runs manager services on multiple machines. Each machine participating in a DALiuGE deployment will run at least one of these services. Each worker machine will need to run a Node Manager and, if more than one node participates in a deployment, in addition there must be a Master Manager running as well. The Master Manager can run on a seperate machine, or on one of the worker machine in addition to the Node Manager. For scalability reasons DALiuGE also introduces the concept of Data Islands, in order to keep the load on the Master under control for very big workflow runs. Data Islands are only really helpful when trying to deploy extremely large physical graphs with 10s of millions of nodes, they are not required when just a large number of machines are involved. Thus starting a DataIsland Manager is optional and could be started on a worker node, or a seperate machine.

Here are examples of the commands used to start the managers on localhost, assuming that the docker image icrar/daliuge-engine:latest is running on localhost.

### Node manager
```
docker exec -ti daliuge-engine dlg nm -v --no-dlm -H 0.0.0.0
```
### Master manager
```
docker exec -ti daliuge-engine dlg mm -v -H 0.0.0.0
```
### Starting managers using the RESTful interface
This would be required if the docker-engine is running on a remote host.
```curl -X POST http://localhost:9000/managers/master
curl -X POST http://localhost:9000/managers/node
curl -d '{"nodes": ["0.0.0.0"]}' -H "Content-Type: application/json" -X POST http://localhost:9000/managers/dataisland
```
## Accessing the run-time web interface
To access the session interface open a browser and point to https://localhost:8000. This allows to monitor the status of deployment sessions.

## Usage
### Stand alone
The DALiuGE runtime and the DALiuGE translator are exposing a command line interface> However, with docker containers using that functionality, although possible is not practical. In many cases the user does not even have access to the host machine running the docker engine. For completeness here is an example on how to call the command line interface:
```
docker exec -ti daliuge-engine dlg
```
It is also possible to interact with both of them directly using the RESTful API, but the easiest way is to use EAGLE.

### EAGLE
The DALiuGE run-time is integrated with the EAGLE (https://github.com/ICRAR/EAGLE) graphical workflow editor. EAGLE has an interface to the DALiuGE translator and through that users can also submit physical graph templates graphs for execution. Please refer to the EAGLE documentation for more details.
