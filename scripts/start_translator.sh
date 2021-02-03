echo "Starting LG2PGT container"
docker run --rm -td --name=dlg-lg2pgt -p 8084:8084 icrar/daliuge-translator:ray > /dev/null
