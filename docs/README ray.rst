Notes of Merge between Data Activated 流 Graph Engine and Ray
=============================================================
The objective of this activity was to investigate a feasible solution for the flexible and simple deployment of DALiuGE on various platforms. In particular the deployment of DAliuGE on AWS in an autoscaling environment is of interest to us.

Ray (https://docs.ray.io/en/master/) is a pretty complete execution engine all by itself, targeting DL and ML applications and integrating a number of the major ML software packages. What we are in particular interested in is the Ray core software, which states the folloing mission:
 
  - Providing simple primitives for building and running distributed applications.

  - Enabling end users to parallelize single machine code, with little to zero code changes.

  - Including a large ecosystem of applications, libraries, and tools on top of the core Ray to enable complex applications.

Internally Ray is using a number of technologies we are also using or evaluating within DALiuGE and/or the SKA. The way Ray is managing and distributing computing is done very well and essentially covers a number of our target platforms including AWS, SLURM, Kubernetes, Azure and GC.

The idea thus was to use Ray to distribute DALiuGE on those platforms and on AWS to start with.

Setup
-----
Ray uses a YAML file to configure a deployment and allows to run additional setup commands on both the head and the worker nodes. In general Ray is running inside docker containers on the target hosts and the initial setup thus is to get the Ray docker image from dockerhub. Getting DALiuGE runnning inside that container is pretty straight forward, but requires installation of gcc and that is quite an overhead. Thus we have created a daliuge-ray docker image, which is now available on the icrar dockerhub repo and is donwloaded instead of the standard Ray image. 

The rest is then straight forward and just requires to configure a few AWS autoscale specific settings, which includes AWS region, type of head node and type and (maximum and minimum) number of worker nodes as well as whether this is using the Spot market or not. In addition it is required to specify the virtual machine AMI ID, which is a pain to get and different for the various AWS regions. 

To get DALiuGE up and running in addition to Ray requires just two additional lines for the HEAD and the worker nodes in the YAML file, but there are some caveats:

Issues
------
The basic idea is to start up a Data Island Manager (and possibly also a Node Manager) on the Ray Head node and a Node Manager on each of the worker nodes. Starting them is trivial, but the issue is to correctly register the NMs with the DIM. DALiuGE is usually doing this the other way around, by telling the DIM at startup which NMs it is responsible for. This is not possible in a autoscaling setup, since the NMs don't exist yet. 
As a workaround DALiuGE does provide a REST API call to register a NM with the DIM, but that currently has a bug, registering the node twice.
Another issue is the fact that the NMs are running inside the Ray docker container and are thus exposed to a docker network interface with a specfic IP address. Ray currently does not expose that IP address through an API call and thus this is a bit tricky to get. It is possible to see that IP address in the Ray monitoring console, which is exposed on port 8625 by default (see below).
In addition both DALiuGE and Ray require a number of ports to be exposed in order to monitor and connect the various parts. In order to achieve that it is best to attach to a virtual terminal on the Head node and specify all the ports at that point as well::

   ray attach -p 8265 -p 8001 -p 8000 -p 5555 -p 6666 daliuge-ray.yaml

Registering the NMs can then be done using curl::

    curl -X POST -H "Content-Type: application/json"  http://localhost:8001/api/nodes/$RAY_WORKER_NODE_IP

Note that the RAY_WORKER_NODE_IP needs to be the IP address of the docker container on the nodes running NMs, which is displayed on the Ray console (http://localhost:8625). The other option is to use the *public* IP addresses of the AWS VMs, mixing the two will not work.

Restarting the Ray cluster does not work because of a bug in Ray.

Submitting a second graph to already running NMs does not work either, because of a bug in DALiuGE. 

To stop and start a node manager use the following two commands, replacing the SSH key file with the one created when creating the cluster and the IP address with the public IP address of the AWS node where the NM should be restarted::

    ssh -tt -o IdentitiesOnly=yes -i /Users/awicenec/.ssh/ray-autoscaler_ap-southeast-2.pem ubuntu@54.253.243.145 docker exec -it ray_container dlg nm -s
    ssh -tt -o IdentitiesOnly=yes -i /Users/awicenec/.ssh/ray-autoscaler_ap-southeast-2.pem ubuntu@54.253.243.145 docker exec -it ray_container dlg nm -v -H 0.0.0.0 -d

Submitting and executing a Graph
--------------------------------
It is possible to use a translator deployed somewhere else, but when submitting the PG from the translator the IP address to be entered there is the *public* IP address of the DIM (Ray head node). After submitting the DALiuGE monitoring page will pop up and show the progress bar.





