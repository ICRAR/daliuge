# Script starts a node manager and a data island manager on the local node. Useful mainly for testing.

docker exec daliuge-engine bash -c 'dlg nm -vvd --no-dlm -H 0.0.0.0 -w ${HOME}'
docker exec -ti daliuge-engine bash -c 'dlg dim -N localhost -vvd -H 0.0.0.0 -w ${HOME}'