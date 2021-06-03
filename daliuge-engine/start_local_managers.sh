# Script starts a node manager and a data island manager on the local node. Useful mainly for testing.

# for some reason the -d flag does no work with the dim always leaves a defunct process
# docker exec daliuge-engine bash -c 'dlg nm -vvd --no-dlm -H 0.0.0.0 --dlg-path=/var/dlg_home/code -w /var/dlg_home/workspace'
docker exec -ti daliuge-engine bash -c 'dlg dim -N localhost -vv -H 0.0.0.0 -w /var/dlg_home/workspace > /tmp/dim.log 2&>1 &'
