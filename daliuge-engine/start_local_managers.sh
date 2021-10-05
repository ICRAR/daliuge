# Script starts a node manager and a data island manager on the local node. Useful mainly for testing.
# docker exec -ti daliuge-engine /daliuge/
curl -d '{"nodes": ["localhost"]}' -H "Content-Type: application/json" -X POST http://localhost:9000/managers/island/start
