Notes of the merge between |daliuge| and Ray
=============================================

The objective of this activity was to investigate a feasible solution for the flexible and simple deployment of DALiuGE on various platforms. In particular the deployment of DAliuGE on AWS in an autoscaling environment is of interest to us.

Ray (https://docs.ray.io/en/master/) is a pretty complete execution engine all by itself, targeting DL and ML applications and integrating a number of the major ML software packages. What we are in particular interested in is the Ray core software, which states the folloing mission:
 
  - Providing simple primitives for building and running distributed applications.

  - Enabling end users to parallelize single machine code, with little to zero code changes.

  - Including a large ecosystem of applications, libraries, and tools on top of the core Ray to enable complex applications.

Internally Ray is using a number of technologies we are also using or evaluating within DALiuGE and/or the SKA. The way Ray is managing and distributing computing is done very well and essentially covers a number of our target platforms including AWS, SLURM, Kubernetes, Azure and GC.

The idea thus was to use Ray to distribute DALiuGE on those platforms and on AWS to start with, but leave the rest of the two systems essentially independent.

Setup
-----

Pre-requisites
^^^^^^^^^^^^^^

First you need to install Ray into your local python virtualenv::

    pip install ray

Ray uses a YAML file to configure a deployment and allows to run additional setup commands on both the head and the worker nodes. In general Ray is running inside docker containers on the target hosts and the initial setup thus is to get the Ray docker image from dockerhub. Getting DALiuGE runnning inside that container is pretty straight forward, but requires installation of gcc and that is quite an overhead. Thus we have created a daliuge-ray docker image, which is now available on the icrar dockerhub repo and is donwloaded instead of the standard Ray image. 

The rest is then straight forward and just requires to configure a few AWS autoscale specific settings, which includes AWS region, type of head node and type and (maximum and minimum) number of worker nodes as well as whether this is using the Spot market or not. In addition it is required to specify the virtual machine AMI ID, which is a pain to get and different for the various AWS regions. 

Starting the DALiuGE Ray cluster
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To get DALiuGE up and running in addition to Ray requires just two additional lines for the HEAD and the worker nodes in the YAML file, but there are some caveats as outlined below. With the provided ray configuration YAML file starting a cluster running DALiuGE on AWS is super easy (provided you have your AWS environment set up in place)::

    cd <path_to_daliuge_git_clone>
    ray up daliuge-ray.yaml

Stopping the cluster is equally simple::

    ray down daliuge-ray.yaml

More for convenience both DALiuGE and Ray require a number of ports to be exposed in order to monitor and connect the various parts. In order to achieve that it is best to attach to a virtual terminal on the Head node and specify all the ports at that point as well::

   ray attach -p 8265 -p 8001 -p 8000 -p 5555 -p 6666 daliuge-ray.yaml

More specifically the command above actually opens a shell inside to the docker container running on the head node AWS instance. 

Issues
^^^^^^
The basic idea is to start up a Data Island Manager (and possibly also a Node Manager) on the Ray Head node and a Node Manager on each of the worker nodes. Starting them is trivial, but the issue is to correctly register the NMs with the DIM. DALiuGE is usually doing this the other way around, by telling the DIM at startup which NMs it is responsible for. This is not possible in a autoscaling setup, since the NMs don't exist yet. 
As a workaround DALiuGE does provide a REST API call to register a NM with the DIM, but that currently has a bug, registering the node twice.

Bringing the cluster down by default only stops the instances and thus the next startup is quite a bit faster. There is just one 'small' issue: Ray v1.0 has a bug, which prevents the second start to work! That is why the current default setting in daliuge-ray.yaml is to terminate the instances::

    cache_stopped_nodes: False

To stop and start a node manager use the following two commands, replacing the SSH key file with the one created when creating the cluster and the IP address with the public IP address of the AWS node where the NM should be restarted::

    ssh -tt -o IdentitiesOnly=yes -i /Users/awicenec/.ssh/ray-autoscaler_ap-southeast-2.pem ubuntu@54.253.243.145 docker exec -it ray_container dlg nm -s
    ssh -tt -o IdentitiesOnly=yes -i /Users/awicenec/.ssh/ray-autoscaler_ap-southeast-2.pem ubuntu@54.253.243.145 docker exec -it ray_container dlg nm -v -H 0.0.0.0 -d

The commands above also show how to connect to a shell inside the docker container on a worker node. Unfortunately this is not exposed as easily as the connection to the head node in Ray.

Submitting and executing a Graph
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
This configuration only deploys the DALiuGE engine. EAGLE and a translator need to be deployed somewhere else. When submitting the PG from a translator web interface, the IP address to be entered there is the *public* IP address of the DIM (Ray AWS head instance). After submitting, the DALiuGE monitoring page will pop up and show the progress bar. It is then also possible to click your way through to the sub-graphs running on the worker nodes.

Future thoughts
---------------
This implementation is the start of an integration between Ray and DALiuGE. Ray (like the AWS autoscaling) is a *reactive* execution framework and as such it uses the autoscaling feature just in time, when the load exceeds a certain threshold. DALiuGE on the other hand is a *proactive* execution framework and pre-allocates the resources required to execute a whole workflow. Both approaches have pros and cons. In particular in an environment where resources are charged by the second it is desireable to allocate them as dynamically as possible. On the other hand dynamic allocation comes with the overhead of provisioning additional resources during run-time and is thus non-deterministic in terms of completion time. This is even more obvious when using the spot market on AWS. Fully dynamic allocation also does not fit well with bigger workflows, which require lots of resources already at the beginning. The optimal solution very likely is somewhere in the middle between fully dynamic and fully static resource provisioning. 

Dynamic workflow allocation
^^^^^^^^^^^^^^^^^^^^^^^^^^^
The first step in that direction is to connect the DALiuGE translator with the ray deployment. After the translator has performed the workflow partitioning the resource requirements are fixed and could be used in turn to startup the Ray cluster with the required number of worker nodes. Essentially This would also completely isolate one workflow from another. The next step could be to add workflow fragmentation to the DALiuGE translator and scale the Ray cluster according to the requirements of each of the fragments, rather than the whole workflow. It has to be seen how to trigger the scaling of the Ray cluster just enough ahead of time to be available for the previous workflow fragment to continue without delays.





