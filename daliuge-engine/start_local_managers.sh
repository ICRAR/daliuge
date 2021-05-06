# Script starts a data island manager on the local node. Useful mainly for testing.

docker exec daliuge-engine bash -c 'source /home/ray/dlg/bin/activate && curl -sd '\''{"nodes": ["localhost"]}'\'' -H "Content-Type: application/json" -X POST http://localhost:9000/managers/dataisland'