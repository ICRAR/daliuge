# daliuge-k8s
Code for the implementation of running DALiuGE in k8s.

# When building kubernetes
When building the kubernetes envrionment from https://github.com/ska-teleskoe/ska-cicd-deploy-minikube I used the following make command. NOTE: memory is limited on current system

	make MEM=6144 RUNTIME=docker all

When building the DALiuGE docker containers run the following command first so that kubernetes docker is used

	eval $(minikube docker-env)


# Information
To be able to connect to service in minikube

	minikube tunnel --cleanup

To be able to share local directory with pod in minikube

	minikube mount /local_daliuge_dir/dlg:/dlg


Once minikube is setup run the following to start a DALiuGE daemon container

	

Mkae sure minikube is started

	minikube start

Now run the tunnel and mount /dlg (see above)

Note: The files dv01.yaml and dvc01.yaml are for local storage but need to use 'minikube mount ....' do map local directory from OS

Now apply the ConfigMap, Service and Deployment for local minikube storage
	kubectl apply -f daliuge-daemon-configmap.yaml
	kubectl apply -f daliuge-daemon-service.yaml
	kubectl apply -f daliuge-daemon-depl-store-minikube.yaml

Get the pod and service information

	kubectl get pod -o wide
	kubectl get svc

Start the Data Island Manager, remember to ensure the tunnel is running above!

	curl -d '{"nodes": ["localhost"]}' -H "Content-Type: application/json" -X POST http://<IP of Service from above>:9000/managers/island/start

Useful commands

	kubectl describe pod <pod name>
	kubectl describe service
	kubectl get pv
	kubectl get pvc
	kubectl get svc

To connect to a pod

	kubectl exec --stdin --tty <pod name> -- /bin/bash

To shutdown everything

	kubectl delete deployment daliuge-daemon-deployment
	kubectl delete service daliuge-daemon-service
	kubectl delete configmap daliuge-daemon-configmap
	minikube stop


