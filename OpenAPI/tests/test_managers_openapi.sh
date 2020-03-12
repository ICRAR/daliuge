#!/bin/bash

try() {
	"$@"
	status=$?
	if [ $status -ne 0 ]; then
		echo "Command exited with status $status, aborting now: $@" 1>&2
		exit 1;
	fi
}

docker run --rm hello-world &> /dev/null
if [ $? -ne 0 ]; then
	echo "Docker is not available, skipping tests" 1>&2
	exit 0
fi

openapi_cli="docker run --rm -v ${PWD}/..:/local openapitools/openapi-generator-cli"
for f in node_manager composite_manager; do
	try $openapi_cli validate -i /local/$f.yaml
	echo "{\"packageName\": \"${f}_client\"}" > config.properties
	try $openapi_cli generate -i /local/$f.yaml -g python -o /local/tests/${f}_client -c /local/tests/config.properties
	try pip install ./${f}_client
done
rm config.properties

# Start node and data island managers
dlg nm -w . -v --no-dlm -l . &
nm_pid=$!
dlg dim -v -N 127.0.0.1 -l . &
dim_pid=$!
echo "Started NM/DIM with PIDs: $nm_pid/$dim_pid"

shutdown_dlg() {
	try kill $dim_pid
	try wait $dim_pid
	try kill $nm_pid
	try wait $nm_pid
}

trap shutdown_dlg EXIT

# Run the test client, which will perform a number of operations
# in the servers, all of which should basically work
sleep 1
try python managers_test_client.py
