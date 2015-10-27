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
Module containing docker-related applications and functions
'''

import logging
import os
import threading
import time

from docker.client import AutoVersionClient

from dfms import utils
from dfms.data_object import BarrierAppDataObject, FileDataObject, \
    DirectoryContainer


logger = logging.getLogger(__name__)

DFMS_ROOT = '/dfms_root'

class ContainerIpWaiter(object):
    """
    A class that remembers the target DO's uid and containerIp properties
    when its internal event has been set, and returns them when waitForIp is
    called, which previously waits for the event to be set.
    """

    def __init__(self, do):
        self._evt = threading.Event()
        self._uid = do.uid
        do.subscribe(self.containerIpChanged, 'containerIp')

    def containerIpChanged(self, do):
        self._containerIp = do.containerIp
        self._evt.set()

    def waitForIp(self, timeout=None):
        self._evt.wait(timeout)
        return self._uid, self._containerIp

class DockerApp(BarrierAppDataObject):
    """
    A BarrierAppDataObject that represents a process running in a container
    hosted by a local docker daemon. Depending on the host system, the docker
    daemon might be automatically activated when a client tries to connect to
    it via its unix socket (like with systemd) or it needs to be brought up
    previous to any client operation (upstart). In any case, if the daemon is
    not present, this class will raise exceptions whenever it tries to connect
    to the server to perform some operation.

    Docker containers are built from docker images, which are pulled to the host
    where the docker daemon runs either explicitly (via `docker pull`) or less
    visibly (e.g., when running `docker run` using an image that has not been
    fetched yet). This DockerApp application will explicitly pull the image at
    `initialize` time, meaning that the docker images will become available at
    the time the physical graph (which this application is part of) is deployed.
    Docker containers also need a command to be run in them, which should be
    an available program inside the image.

    Input and output
    ----------------

    The inputs and outputs used by the dockerized application are made available
    by mapping host directories and files as "data volumes". Since non-existing
    files will appear as directories in the container filesystem, this class
    create any output FileDataObject first in the host filesystem with size 0,
    so it will also appear as a file in the container filesystem.

    Processes running inside docker container run as root by default. Before
    running the command that is meant to be container-ized, this application
    appends some commands to make sure that: a) a user exists in the container
    namespace with the same UID of the user running this application, and b)
    that the command is executed by such user. This way the output produced by
    the containerized application will belong to the host user.

    Since data volumes are a file-specific feature, this DockerApp application
    supports file-system based input/output DataObjects only, namely the
    FileDataObject and the DirectoryContainer types.

    Since the command to be run in the container receives most probably as
    arguments the paths of its inputs and outputs, and since these might not be
    known precisely until runtime, users should use placeholders for them in the
    command-line specification. Placeholders for input locations take the form
    of "%iX", where X starts from 0 and refers to the X-th input. Likewise,
    output locations are specified as "%oX".

    Communication between containers
    --------------------------------

    Although some containerized applications might run on their own, there are
    cases where applications need to talk to each other in order to advance
    (like in the case of client-server applications, or in the case of MPI
    applications). All containers started in the same host (and therefore, all
    applications running in them) belong by default to the same network, and
    therefore are already visible.

    Applications needing to communicate with other applications should be able
    to specify the target's IP in their command-line. Since the IP is not known
    until containers are created, this specification is done using the
    %containerIp[oid]% placeholder, with 'oid' being the OID of the target
    DockerApp.

    This need to know other DockerApp's IP imposes a sequential order on the
    startup of the containers, since one needs to be started in order to learn
    its IP, which is used to start the second. This is handled gracefully by
    the DockerApp code, with the condition that `self.handleInterest` is invoked
    where necessary. See `self.handleInterest` for more information about this
    mechanism.

    TODO:
    -----
    Processes in containers might not always exit by themselves, and the
    containers might need to be manually stopped. This the case for example of
    an set of MPI processes, where the master container will run the MPI
    program and the slave containers will run an SSH daemon, where the SSH
    daemon will not quit automatically once the master process has ended.

    Still, we probably will need to differentiate between a forced quit because
    of a timeout, and a good quit, and therefore we might impose that processes
    running in a container must quit themselves after successfully performing
    their task.
    """

    def initialize(self, **kwargs):
        BarrierAppDataObject.initialize(self, **kwargs)

        self._image = self._getArg(kwargs, 'image', None)
        if not self._image:
            raise Exception('No docker image specified, cannot create DockerApp')

        if ":" not in self._image:
            logger.warn("%r: Image %s is too generic since it doesn't specify a tag" % (self, self._image))

        self._command = self._getArg(kwargs, 'command', None)
        if not self._command:
            raise Exception("No command specified, cannot create DockerApp")

        if logger.isEnabledFor(logging.INFO):
            logger.info("%r: DockerApp with image '%s' and command '%s' created" % (self, self._image, self._command))

        # Check if we have the image; otherwise pull it.
        c = AutoVersionClient()
        found = reduce(lambda a,b: a or self._image in b['RepoTags'], c.images(), False)

        if not found:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Image '%s' not found, pulling it" % (self._image))
            start = time.time()
            c.pull(self._image)
            end = time.time()
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Took %.2f [s] to pull image '%s'" % ((end-start), self._image))
        else:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Image '%s' found, no need to pull it" % (self._image))

        self._containerIp = None
        self._containerId = None
        self._waiters = []

    @property
    def containerIp(self):
        return self._containerIp

    @containerIp.setter
    def containerIp(self, containerIp):
        self._containerIp = containerIp
        self._fire('containerIp', containerIp=containerIp)

    @property
    def containerId(self):
        return self._containerId

    def handleInterest(self, do):

        # The only interest we currently have is the containerIp of other
        # DockerApps, and only if our command actually uses this IP
        if isinstance(do, DockerApp):
            if '%containerIp[{0}]%'.format(do.uid) in self._command:
                self._waiters.append(ContainerIpWaiter(do))
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug('%r: Added ContainerIpWaiter for %r' % (self, do))

    def run(self):

        # Check inputs/outputs are of a valid type
        for i in self.inputs + self.outputs:
            if not isinstance(i, (FileDataObject, DirectoryContainer)):
                raise Exception("%r is not supported by the DockerApp" % (i))

        # We mount the inputs and outputs inside the docker under the
        # DFMS_ROOT directory, maintaining the rest of their original paths
        # The inputs are also mounted as RO to avoid any accidental write
        dockerInputs  = [DFMS_ROOT + i.path for i in self.inputs]
        dockerOutputs = [DFMS_ROOT + o.path for o in self.outputs]
        vols = dockerInputs + dockerOutputs
        binds  = [i.path + ":" + dockerInputs[x] + ":ro" for x,i in enumerate(self.inputs)]
        binds += [o.path + ":" + dockerOutputs[x]        for x,o in enumerate(self.outputs)]
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Volume bindings: %r" % (binds))

        # Replace any input/output placeholders that might be found in the
        # command line by the real path of the inputs and outputs
        cmd = self._command
        for x,i in enumerate(dockerInputs):
            cmd = cmd.replace("%%i%d" % (x), i)
        for x,o in enumerate(dockerOutputs):
            cmd = cmd.replace("%%o%d" % (x), o)

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Command after placeholder replacement is: %s" % (cmd))

        # Wait until the DockerApps this application runtime depends on have
        # started, and replace their IP placeholders by the real IPs
        for waiter in self._waiters:
            uid, ip = waiter.waitForIp();
            cmd = cmd.replace("%containerIp[{0}]%".format(uid), ip)

        # We want to run the container-ized program not as root, but as a user
        # with the same UID of the current user. This way the output produced
        # by the container user will be also owned by the host user, who won't
        # have any problem managing it.
        # We achieve this by creating a user with the same UID if one doesn't
        # exist, and running the command as the correct user via "su".
        uid = os.getuid()
        cmd = "id -u {0} &> /dev/null || adduser --uid {0} r; cd; su - $(getent passwd {0} | cut -f1 -d:) -c /bin/bash -c '{1}'".format(uid, utils.escapeQuotes(cmd, doubleQuotes=False))

        # Embed the command in bash
        cmd = '/bin/bash -c "%s"' % (utils.escapeQuotes(cmd, singleQuotes=False))

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Command after user creation and wrapping is: %s" % (cmd))

        # If we're mapping files that don't exist yet (i.e., FileDataObjects
        # in the outputs) we need to touch them first; otherwise the mapping
        # will result in the container end being treated as a directory
        for f in filter(lambda do: isinstance(do, FileDataObject), self.outputs):
            f.ensureExists()

        c = AutoVersionClient()

        # Create container
        container = c.create_container(self._image, cmd, volumes=vols, host_config=c.create_host_config(binds=binds))
        self._containerId = cId = container['Id']
        if logger.isEnabledFor(logging.INFO):
            logger.info("Created container %s for %r" % (cId, self))

        # Start it
        start = time.time()
        c.start(container)
        if logger.isEnabledFor(logging.INFO):
            logger.info("Started container %s" % (cId))

        # Figure out the container's IP and save it
        # Setting self.containerIp will trigger an event being sent to the
        # registered listeners
        inspection = c.inspect_container(container)
        self.containerIp = inspection['NetworkSettings']['IPAddress']

        # Wait until it finishes
        self._exitCode = c.wait(container)
        end = time.time()
        if logger.isEnabledFor(logging.INFO):
            logger.info("Container %s finished in %.2f [s] with exit code %d" % (cId, (end-start), self._exitCode))

        if self._exitCode == 0 and logger.isEnabledFor(logging.DEBUG):
            msg = "Container %s finished successfully" % (cId,)
            stdout = c.logs(container, stdout=True, stderr=False)
            stderr = c.logs(container, stdout=False, stderr=True)
            logger.debug(msg + ", output follows.\n==STDOUT==%s\n==STDERR==\n%s", stdout, stderr)
        elif self._exitCode != 0:
            stdout = c.logs(container, stdout=True, stderr=False)
            stderr = c.logs(container, stdout=False, stderr=True)
            msg = "Container %s didn't finish successfully (exit code %d)" % (cId, self._exitCode)
            logger.error(msg + ", output follows.\n==STDOUT==%s\n==STDERR==\n%s", stdout, stderr)
            raise Exception(msg)