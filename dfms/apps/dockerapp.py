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
import os
'''
Module containing docker-related applications and functions
'''

import logging
import time

from docker.client import AutoVersionClient

from dfms.data_object import BarrierAppDataObject, FileDataObject, \
    DirectoryContainer


logger = logging.getLogger(__name__)

DFMS_ROOT = '/dfms_root'

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

    Linking containers
    ------------------

    Although some containerized applications might run on their own, there are
    cases where applications need to talk to each other in order to advance
    (like in the case of client-server applications, or in the case of MPI
    applications). All containers started in the same host (and therefore, all
    applications running in them) belong by default to the same network, and
    therefore are already visible. The problem lies on getting them to know the
    hosts they should contact. See the TODO below.

    TODO:
    -----
    I manually tried to run an MPI program across more than one docker
    container and succeeded on it. In one container A I started simply an ssh
    server (the slave node), while on a second one I started the main mpiexec
    process (the master node). Thanks to this exercise a few things that need to
    be taken into account became clear:

    a) We need to have a placeholder mechanism to be able to refer to the IP of
    some other docker container, probably using the uid of the DockerApp we're
    referring to, something like:

    a = DockerApp('A', image='slave_image', cmd='sshd')
    b = DockerApp('B', image='master_image', cmd='mpiexec -H localhost,%ip{A} something')

    My current understanding is that there is no way to set a given IP on a
    newly created container, so manually assigning IPs to containers is not an
    option currently.

    b) This leads to a dependency problem on the startup process of the
    containers. In the example above, B needs to know A's IP address, and
    therefore A needs to be created first (although not necessarily run!), and
    in more complex examples there could be more complex dependencies involved.
    The framework would thus need to handle this scenario gracefully, starting
    the applications in the correct order, and letting them know information
    about each other (in this case the IPs of the containers they are starting).
    For this, we need to expose this start-up dependency explicitly at the
    AppDataObject level. In a dospec form, it could look something like:

    [
    {uid:'A', type='app', app='DockerApp', image='slave_image', cmd='sshd -D'},
    {uid:'B', type='app', app='DockerApp', image='master_image', cmd='mpiexec -H localhost,%ip{A} something', run_depends_on=['A']}
    ]

    c) Processes in containers might not always exit by themselves, and the
    containers will need to be manually stopped. This is the case for example of
    the slave containers, where the sshd daemon needs to be running before the
    master starts, but will not quit automatically once the master process has
    ended. Maybe the same "run_depends_on" attribute shown above can be reused
    for the purpose of automatically stopping containers, but on the other hand
    the 'starting A depends on B' and the 'B needs to be stopped after A'
    situations do not always correspond one to one, and thus one could think on
    a separate attribute to denote this relationship.
    """

    def initialize(self, **kwargs):
        BarrierAppDataObject.initialize(self, **kwargs)

        self._image = self._getArg(kwargs, 'image', None)
        if not self._image:
            raise Exception('No docker image specified, cannot create DockerApp')

        if ":" not in self._image:
            logger.warn("Image %s is too generic since it doesn't specify a tag" % (self._image))

        self._command = self._getArg(kwargs, 'command', None)
        if not self._command:
            raise Exception("No command specified, cannot create DockerApp")

        if logger.isEnabledFor(logging.INFO):
            logger.info("DockerApp with image '%s' and command '%s' created" % (self._image, self._command))

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

    def run(self):

        # Check inputs/outputs are of a valid type
        for i in self.inputs + self.outputs:
            if not isinstance(i, (FileDataObject, DirectoryContainer)):
                raise Exception("DataObject %r is not supported by the DockerApp" % (i))

        # We mount the inputs and outputs inside the docker under the
        # DFMS_ROOT directory, maintaining the rest of their original paths
        # The inputs are also mounted as RO to avoid any accidental write
        dockerInputs  = [DFMS_ROOT + i.path for i in self.inputs]
        dockerOutputs = [DFMS_ROOT + o.path for o in self.outputs]
        vols = dockerInputs + dockerOutputs
        binds  = [i.path + ":" + dockerInputs[x] + ":ro" for x,i in enumerate(self.inputs)]
        binds += [o.path + ":" + dockerOutputs[x]        for x,o in enumerate(self.outputs)]

        # Replace any placeholders that might be found in the command line
        # by the real path of the inputs and outputs
        cmd = self._command
        for x,i in enumerate(dockerInputs):
            cmd = cmd.replace("%%i%d" % (x), i)
        for x,o in enumerate(dockerOutputs):
            cmd = cmd.replace("%%o%d" % (x), o)

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Command after placeholder replacement is: '%s'" % (cmd))

        # We want to run the container-ized program not as root, but as a user
        # with the same UID of the current user. This way the output produced
        # by the container user will be also owned by the host user, who won't
        # have any problem managing it.
        # We achieve this by creating a user with the same UID if one doesn't
        # exist, and running the command that user via "su"
        uid = os.getuid()
        cmd = "id -u {0} &> /dev/null || adduser --uid {0} r; cd; su - r -c /bin/bash -c '{1}'".format(uid, cmd.replace("'","\\'"))

        # Embed the command in bash
        cmd = '/bin/bash -c "%s"' % (cmd.replace('"','\\"'))

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Command after user creation and wrapping is: '%s'" % (cmd))

        # If we're mapping files that don't exist yet (i.e., FileDataObjects
        # in the outputs) we need to touch them first; otherwise the mapping
        # will result in the container end being treated as a directory
        for f in filter(lambda do: isinstance(do, FileDataObject), self.outputs):
            f.ensureExists()

        c = AutoVersionClient()
        container_id = c.create_container(self._image, cmd, volumes=vols, host_config=c.create_host_config(binds=binds))

        if logger.isEnabledFor(logging.INFO):
            logger.info("Starting DockerApp with image '%s' and command '%s'" % (self._image, self._command))

        c.start(container_id)
        self._exitCode = c.wait(container_id)

        if logger.isEnabledFor(logging.INFO):
            logger.info("DockerApp with image '%s' and command '%s' finished with exit code %d" % (self._image, cmd, self._exitCode))