#!/bin/bash
#
# Script to create a virtual environment suitable for
# developing/installing NGAS, or to use the fabric scripts
#
# ICRAR - International Centre for Radio Astronomy Research
# (c) UWA - The University of Western Australia, 2016
# Copyright by UWA (in the framework of the ICRAR)
# All rights reserved
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston,
# MA 02111-1307  USA
#

#
# This script creates a virtual environment.
# This new venv can be then used both to install NGAS on it
# (either normally, or in development mode)
# or to locally support the fabric-based remote installation procedure.
#

error() {
	echo "ERROR: $1" 1>&2
	exit 1
}

warning() {
	echo "WARNING: $1" 1>&2
}

function print_usage {
	echo "$0 [-h | -?] [-f] [-p <python_exec>] [-3 | -2] <virtualenv_dir>"
	echo
	echo "-h, -?: Show this help"
	echo "-f: Install Fabric into the virtual environment"
	echo "-p <python_exec>: Use <python_exec> as the python interpreted for this virtualenv"
	echo "-3, -2: Choose between python 3 or 2.7. Defaults to 2.7"
}

# Command-line option parsing
FABRIC_READY=
PYTHON_EXEC=
PYTHON_VERSION=

while getopts "fp:32h?" opt
do
	case "$opt" in
		f)
			FABRIC_READY=yes
			;;
		p)
			PYTHON_EXEC="$OPTARG"
			;;
		3)
			if [ "${PYTHON_VERSION}" = "2" ]
			then
				echo "Error: Cannot specify -2 and -3 together" 1>&2
				print_usage 1>&2
				exit 1
			fi
			PYTHON_VERSION=3
			;;
		2)
			if [ "${PYTHON_VERSION}" = "3" ]
			then
				echo "Error: Cannot specify -2 and -3 together" 1>&2
				print_usage 1>&2
				exit 1
			fi
			PYTHON_VERSION=2
			;;
		[h?])
			print_usage
			exit 0
			;;
		:)
			print_usage 1>&2
			exit 1
	esac
done

if [ $(($# - $OPTIND)) -lt 0 ]
then
	print_usage 1>&2
	exit 1
fi

veDir=${@:$OPTIND:1}
if [[ -d "$veDir" ]]
then
	error "$veDir already exists"
fi

# Still default to python 2
PYTHON_VERSION=${PYTHON_VERSION:-2}

# First things first, check that we have python installed
# We default to whatever is in the path if not specified
if [ -z "${PYTHON_EXEC}" ]
then
	if [ ${PYTHON_VERSION} = "2" ]
	then
		PYTHON_EXEC=python
	else
		PYTHON_EXEC=python3
	fi
fi
if [[ -z "$(command -v ${PYTHON_EXEC} 2> /dev/null)" ]]
then
	error "No Python found in this system, install Python 2.7 or above"
fi

# Check that the python version is correct
pythonVersion=$(${PYTHON_EXEC} -V 2>&1)
if [[ "${PYTHON_VERSION}" == "2" && ! "$pythonVersion" == *" 2.7"* ]]
then
	error "Python 2.7 needed, found: $pythonVersion"
elif [[ "${PYTHON_VERSION}" == "3" && ! "$pythonVersion" == *" 3."* ]]
then
	error "Python 3 needed, found: $pythonVersion"
fi

# Check if we already have virtualenv
# If not download one and untar it
# Python 3 comes with virtualenv though
sourceCommand="source -- $veDir/bin/activate"
if [ ${PYTHON_VERSION} = "3" ]
then
	veCommand="${PYTHON_EXEC} -mvenv"
else
	veCommand="virtualenv -p $PYTHON_EXEC"
	if [[ -z "$(command -v virtualenv 2> /dev/null)" ]]
	then
		VIRTUALENV_URL='https://pypi.python.org/packages/8b/2c/c0d3e47709d0458816167002e1aa3d64d03bdeb2a9d57c5bd18448fd24cd/virtualenv-15.0.3.tar.gz#md5=a5a061ad8a37d973d27eb197d05d99bf'
		if [[ ! -z "$(command -v wget 2> /dev/null)" ]]
		then
			wget "$VIRTUALENV_URL" || error "Failed to download virtualenv"
		elif [[ ! -z "$(command -v curl 2> /dev/null)" ]]
		then
			curl "$VIRTUALENV_URL" -o virtualenv-15.0.3.tar.gz || error "Failed to download virtualenv"
		else
			error "Can't find a download tool (tried wget and curl), cannot download virtualenv"
		fi

		tar xf virtualenv-15.0.3.tar.gz || error "Failed to untar virtualenv"
		veCommand="$PYTHON_EXEC virtualenv-15.0.3/virtualenv.py -p $PYTHON_EXEC"
		REMOVE_VE="yes"
	fi
fi

# Create a virtual environment for the NGAMS installation procedure to begin
# and source it
$veCommand -- "$veDir" || error "Failed to create virtualenv"
if [[ "$REMOVE_VE" == "yes" ]]
then
	rm -rf virtualenv-15.0.3 virtualenv-15.0.3.tar.gz || warning "Failed to remove temporary copy of virtualenv"
fi

# Install initial packages into the new venv
# Fabric is needed to allow using the fab scripts in the first place.
# pycrypto is needed by the SSH pubkey-related bits in the fab scripts.
# boto is needed to support the aws-related fab tasks.
if [[ "$FABRIC_READY" == "yes" ]]
then
	$sourceCommand || error "Failed to source virtualenv"
	FABRIC=fabric
	if [ ${PYTHON_VERSION} = "3" ]
	then
		FABRIC=fabric3
	fi
	pip install boto ${FABRIC} pycrypto || error "Failed to install fabric packages in virtualenv"
fi

echo
echo
echo "----------------------------------------------------------------------------"
echo "Virtual Environment successfully created!"
echo "Now run the following command on your shell to load the virtual environment:"
echo
echo "$sourceCommand"
echo
echo "You can now use this virtual environment to either locally install NGAS"
echo "(normally or in development mode, see ./build.sh -h), or to run the remote"
echo "installation procedures via fabric scripts (run fab -l for more information)"
echo "----------------------------------------------------------------------------"
echo
