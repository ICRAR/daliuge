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

test -d editable_copies || mkdir editable_copies
openapi_cli="docker run --rm -v ${PWD}/..:/local openapitools/openapi-generator-cli"
for f in node_manager composite_manager translator; do
	try $openapi_cli validate -i /local/$f.yaml
	echo "{\"packageName\": \"${f}_client\"}" > config.properties
	try $openapi_cli generate -i /local/$f.yaml -g python -o /local/tests/${f}_client -c /local/tests/config.properties
	try cp -r ${f}_client editable_copies/${f}_client
	try pip install editable_copies/${f}_client
done
rm config.properties

# Start node and data island managers
tmpdir=`mktemp -d`
dlg nm -w . -v --no-dlm -l $tmpdir -w $tmpdir &
nm_pid=$!
dlg dim -v -N localhost -l $tmpdir -w $tmpdir &
dim_pid=$!
dlg tm -d $tmpdir -t $tmpdir -v &
tm_pid=$!
echo "Started nm/dim/tm under $tmpdir with PIDs: $nm_pid/$dim_pid/$tm_pid"

shutdown_dlg() {
	try kill $tm_pid
	try wait $tm_pid
	try kill $dim_pid
	try wait $dim_pid
	try kill $nm_pid
	try wait $nm_pid
	rm -rf $tmpdir
}

trap shutdown_dlg EXIT

# Run the test client for managers only, which will perform a number of
# operations in the servers, all of which should basically work
sleep 2
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
