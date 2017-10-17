#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2015
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#
'''
A module containing utility code for running remote commands over SSH.
'''

import logging
import os
import time

from paramiko.client import SSHClient, AutoAddPolicy
from paramiko.rsakey import RSAKey
import scp


logger = logging.getLogger(__name__)

def execRemoteWithClient(client, command, timeout=None, bufsize=-1):
    """
    Executes `command` using the given SSH `client`.
    """
    chan = client.get_transport().open_session()
    chan.settimeout(timeout)
    chan.exec_command('/bin/bash -l -c "%s"' % (command.replace('"','\"')))
    # Otherwise do something like this:
    #chan.get_pty(width=80, height=24)
    #chan.invoke_shell()
    #chan.sendall(command + "\n")

    # Wait until the command has finished execution and return the full contents
    # of the stdout and stderr
    while True:
        if chan.exit_status_ready():
            break
        try:
            time.sleep(0.01)
        except KeyboardInterrupt:
            chan.send('\x03')

    exitStatus = chan.recv_exit_status()
    with chan.makefile('r', bufsize) as f:
        stdout = f.read()
    with chan.makefile_stderr('r', bufsize) as f:
        stderr = f.read()
    return stdout, stderr, exitStatus

def execRemote(host, command, username=None, timeout=None, bufsize=-1):
    """
    Executes `command` on `host`. If `username` is provided, the command will
    be run as `username`; otherwise the current user will be used to run the
    command.
    """
    with createClient(host, username) as client:
        return execRemoteWithClient(client, command, timeout, bufsize)

def createClient(host, username=None, pkeyPath=None):
    """
    Creates an SSH client object that can be used to perform SSH-related
    operations
    """
    client = SSHClient()
    client.set_missing_host_key_policy(AutoAddPolicy())

    pkey = RSAKey.from_private_key_file(os.path.expanduser(pkeyPath)) if pkeyPath else None
    client.connect(host, username=username, pkey=pkey)
    return client

def __scpProgress(filename, size, sent):
    if size == sent:
        logger.debug("Finished scp-ing %s (%d bytes)", filename, sent)

def copyFrom(host, remotePath, localPath='.', recursive=False, username=None, pkeyPath=None, timeout=None):
    """
    Copies the files located at `host`:`remotePath` to `localPath` connecting
    to `host` as `username`. A `recursive` flag can be specified, as well as a
    private key (via `pkeyPath`) to be used when creating the connection.
    """
    if timeout is None:
        timeout = 5.0
    client = createClient(host, username=username, pkeyPath=pkeyPath)
    with scp.SCPClient(client.get_transport(), progress=__scpProgress, socket_timeout=timeout) as scpClient:
        scpClient.get(remote_path=remotePath, local_path=localPath, recursive=recursive)
    client.close()

def copyTo(host, localFiles, remotePath='.', recursive=False, username=None, pkeyPath=None, timeout=None):
    """
    Copies the files located at `localPath` to `host`:`remotePath` connecting
    to `host` as `username`. A `recursive` flag can be specified, as well as a
    private key (via `pkeyPath`) to be used when creating the connection.
    """
    if timeout is None:
        timeout = 5.0
    client = createClient(host, username=username, pkeyPath=pkeyPath)
    with scp.SCPClient(client.get_transport(), progress=__scpProgress, socket_timeout=timeout) as scpClient:
        scpClient.put(localFiles, remote_path=remotePath, recursive=recursive)
    client.close()