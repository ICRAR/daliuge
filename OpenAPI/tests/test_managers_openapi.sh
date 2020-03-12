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
for f in node_manager composite_manager translator; do
	try $openapi_cli validate -i /local/$f.yaml
	echo "{\"packageName\": \"${f}_client\"}" > config.properties
	try $openapi_cli generate -i /local/$f.yaml -g python -o /local/tests/${f}_client -c /local/tests/config.properties
	try pip install ./${f}_client
done
rm config.properties

# Start node and data island managers
tmpdir=`mktemp -d`
dlg nm -w . -v --no-dlm -l $tmpdir -w $tmpdir &
nm_pid=$!
dlg dim -v -N 127.0.0.1 -l $tmpdir -w $tmpdir &
dim_pid=$!
dlg lgweb -d $tmpdir -t $tmpdir -v &
lgweb_pid=$!
echo "Started nm/dim/lgweb under $tmpdir with PIDs: $nm_pid/$dim_pid/$lgweb_pid"

shutdown_dlg() {
	try kill $lgweb_pid
	try wait $lgweb_pid
	try kill $dim_pid
	try wait $dim_pid
	try kill $nm_pid
	try wait $nm_pid
	rm -rf $tmpdir
}

trap shutdown_dlg EXIT

# Run the test client for managers only, which will perform a number of
# operations in the servers, all of which should basically work
sleep 1
try python managers_test_client.py

# Now run the test client for the translator, which should culminate
# on a physical graph running on the node manager
try python translator_test_client.py <(sed "s|%PWD%|$PWD|" test.graph)

# The graph should have produced a copy of the test.graph file
# in the session directory under $tmpdir
found=no
for i in {1..10}; do
	session_dir="`find $tmpdir -mindepth 1 -type d`"
	if [ -n "$session_dir" ]; then
		copy="`find $session_dir -type f`"
		if [ -n "$copy" ] && [ -z "`diff -Naur $copy test.graph`" ]; then
			found=yes
			break
		fi
	fi
	sleep 0.5
done
test $found = yes || { echo "graph didn't run correctly" && exit 1; }
echo "Graph ran successfully, tests finished"
