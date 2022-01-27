NOTE: there are two deployment versions, one for minikube and one for a cluster, copy the correct file into templates.

The DALiuGE root directory needs to be visible inside the cluster. In the case of minikube this means you may need to run

	minikube mount <local path>/dlg:/dlg

Finally, on minikube you may need to run the follwoing

	minikube tunnel --cleanup

NOTE: On MacOS you can run with --clenaup and will start it and cleanup after. Not sure if this is the dame for all platforms.


# Install/Setup
From mychart directory

helm install daliuge-daemon .
kubectl get svc -o wide
curl -d '{"nodes": ["localhost"]}' -H "Content-Type: application/json" -X POST http://<IP address from above>:9000/managers/island/start
helm uninstall daliuge-daemon
