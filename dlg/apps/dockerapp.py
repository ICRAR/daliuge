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

import collections
import logging
import os
import threading
import time

from configobj import ConfigObj
import docker

from .. import utils, droputils
from ..drop import BarrierAppDROP
from ..exceptions import InvalidDropException


logger = logging.getLogger(__name__)

DLG_ROOT = '/dlg_root'

DockerPath = collections.namedtuple('DockerPath', 'path')


class ContainerIpWaiter(object):
    """
    A class that remembers the target DROP's uid and containerIp properties
    when its internal event has been set, and returns them when waitForIp is
    called, which previously waits for the event to be set.
    """

    def __init__(self, drop):
        self._evt = threading.Event()
        self._uid = drop.uid
        drop.subscribe(self, 'containerIp')

    def handleEvent(self, e):
        self._containerIp = e.containerIp
        self._evt.set()

    def waitForIp(self, timeout=None):
        self._evt.wait(timeout)
        return self._uid, self._containerIp

class DockerApp(BarrierAppDROP):
    """
    A BarrierAppDROP that represents a process running in a container
    hosted by a local docker daemon. Depending on the host system, the docker
    daemon might be automatically activated when a client tries to connect to
    it via its unix socket (like with systemd) or it needs to be brought up
    prior to any client operation (upstart). In any case, if the daemon is
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

    **Input and output**

    The inputs and outputs used by the dockerized application are made available
    by mapping host directories and files as "data volumes". Inputs are bound
    using their full path, but outputs are bound only up to their dirnames,
    because otherwise they would be created at container creation time by
    Docker. For example, the output /a/b/c will produce a binding to /dlg/a/b
    inside the docker container, where c will have to be written by the process
    running in the container.

    Since the command to be run in the container receives most probably as
    arguments the paths of its inputs and outputs, and since these might not be
    known precisely until runtime, users should use placeholders for them in the
    command-line specification. Placeholders for input locations take the form
    of "%iX", where X starts from 0 and refers to the X-th filesystem-related
    input. Likewise, output locations are specified as "%oX". Alternatively,
    inputs and outputs can be referred to by their UIDs, in which case the
    placeholders will look like "%i[X]" and "%o[X]" respectively, where X is the
    UID of the input/output being referenced.

    Data volumes are a file-specific feature. For this reason, volumes are setup
    for file-system based input/output DROPs only, namely the FileDROP and the
    DirectoryContainer types. Other DROP types can instead pass down their
    dataURL property via the command-line by using placeholders. Placeholders
    for input DROP dataURLs take the form of "%iDataURLX", where X starts from 0
    and refers to the X-th non-filesystem related input. Likewise, output
    dataURLs are specified as "%oDataURLX". Alternatively users can refer to the
    dataURL of a specific input or output as "%iDataURL[X]" and "%oDataURL[X]"
    respectively, where X is the UID of the input/output being referenced.

    Additional volume bindings can be specified via the keyword arguments when
    creating the DockerApp. The host file/directories must exist at the moment
    of creating the DockerApp; otherwise it will fail to initialize.

    **Users**

    A docker container usually runs as root by default. One of the major
    drawbacks of this is that the output generated by the containerized
    application will belong also to the root user of the host system, and not to
    the user running the DALiuGE framework. This DockerApp avoids to run containers
    as the root user because of this reason. Two parameters, given at
    construction time, control this behavior:

    * `user`
              If given indicates the user used to run the container. It is
              assumed that if a user is indicated, the user already exists in
              the docker image; otherwise the container will actually fail to
              start. Its default value is `None`, meaning that the container
              will run as the root user.
    * `ensureUserAndSwitch`
              If the container is run as the root user, this
              option indicates whether a non-root user with the same UID of the
              user running this process should be: a) searched for, b) created
              if it doesn't exist, and c) used to run the command inside the
              container. This is achieved by prepending some shell commands to
              the initial user-specified command, which will run as root first,
              but that finally perform the switch within the container process.
              Its default value is `True` if `user` is `None`; `False`
              otherwise.

    Using these two options one can thus control the user that will run the
    command inside the container.

    **Communication between containers**

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

    **TODO**

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
        BarrierAppDROP.initialize(self, **kwargs)

        self._image = self._getArg(kwargs, 'image', None)
        if not self._image:
            raise InvalidDropException(self, 'No docker image specified, cannot create DockerApp')

        if ":" not in self._image:
            logger.warning("%r: Image %s is too generic since it doesn't specify a tag", self, self._image)

        self._command = self._getArg(kwargs, 'command', None)
        if not self._command:
            raise InvalidDropException(self, "No command specified, cannot create DockerApp")

        # The user used to run the process in the docker container
        # By default docker containers run as root, but we don't want to run
        # a process using a different user because otherwise anything that that
        # process writes to the filesystem
        self._user = self._getArg(kwargs, 'user', None)

        # In some cases we want to make sure the command in the container runs
        # as a certain user, so we wrap up the command line in a small script
        # that will create the user if missing and switch to it
        self._ensureUserAndSwitch = self._getArg(kwargs, 'ensureUserAndSwitch', self._user is None)

        # By default containers are removed from the filesystem, but people
        # might want to preserve them.
        # TODO: This might be something that the data lifecycle manager could
        # handle, but for the time being we do it here
        self._removeContainer = self._getArg(kwargs, 'removeContainer', True)

        # Additional volume bindings can be specified for existing files/dirs
        # on the host system.
        self._additionalBindings = {}
        for binding in self._getArg(kwargs, 'additionalBindings', []):
            if binding.find(':') == -1:
                host_path = container_path = binding
            else:
                host_path, container_path = binding.split(':')
            if not os.path.exists(host_path):
                raise InvalidDropException(self, "'Path %s doesn't exist, cannot use as additional volume binding" % (host_path,))
            self._additionalBindings[host_path] = container_path

        logger.info("%r with image '%s' and command '%s' created", self, self._image, self._command)

        # Check if we have the image; otherwise pull it.
        c = DockerApp._get_client()
        found = any([self._image in im.tags for im in c.images.list()])

        if not found:
            logger.debug("Image '%s' not found, pulling it", self._image)
            start = time.time()
            c.images.pull(self._image)
            end = time.time()
            logger.debug("Took %.2f [s] to pull image '%s'", (end-start), self._image)
        else:
            logger.debug("Image '%s' found, no need to pull it", self._image)
        c.api.close()

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

    def handleInterest(self, drop):

        # The only interest we currently have is the containerIp of other
        # DockerApps, and only if our command actually uses this IP
        if isinstance(drop, DockerApp):
            if '%containerIp[{0}]%'.format(drop.uid) in self._command:
                self._waiters.append(ContainerIpWaiter(drop))
                logger.debug('%r: Added ContainerIpWaiter for %r', self, drop)

    def run(self):

        # Replace any placeholder in the commandline with the proper path or
        # dataURL, depending on the type of input/output it is
        # In the case of fs-based i/o we replace the command-line with the path
        # that the Drop will receive *inside* the docker container (see below)

        iitems = self._inputs.items()
        oitems = self._outputs.items()
        fsInputs  = {uid: i for uid,i in iitems if droputils.has_path(i)}
        fsOutputs = {uid: o for uid,o in oitems if droputils.has_path(o)}
        dockerInputs  = {uid: DockerPath(DLG_ROOT + i.path) for uid,i in fsInputs.items()}
        dockerOutputs = {uid: DockerPath(DLG_ROOT + o.path) for uid,o in fsOutputs.items()}
        dataURLInputs  = {uid: i for uid,i in iitems if not droputils.has_path(i)}
        dataURLOutputs = {uid: o for uid,o in oitems if not droputils.has_path(o)}

        cmd = droputils.replace_path_placeholders(self._command, dockerInputs, dockerOutputs)
        cmd = droputils.replace_dataurl_placeholders(cmd, dataURLInputs, dataURLOutputs)

        # We bind the inputs and outputs inside the docker under the DLG_ROOT
        # directory, maintaining the rest of their original paths.
        # Outputs are bound only up to their dirname (see class doc for details)
        # Volume bindings are setup for FileDROPs and DirectoryContainers only
        binds  = [                i.path  + ":" +                  dockerInputs[uid].path  for uid,i in fsInputs.items()]
        binds += [os.path.dirname(o.path) + ":" + os.path.dirname(dockerOutputs[uid].path) for uid,o in fsOutputs.items()]
        binds += [host_path + ":" + container_path  for host_path, container_path in self._additionalBindings.items()]
        logger.debug("Volume bindings: %r", binds)

        # Wait until the DockerApps this application runtime depends on have
        # started, and replace their IP placeholders by the real IPs
        for waiter in self._waiters:
            uid, ip = waiter.waitForIp()
            cmd = cmd.replace("%containerIp[{0}]%".format(uid), ip)
            logger.debug("Command after IP replacement is: %s", cmd)

        # If a user has been given, we run the container as that user. It is
        # useful to make sure that the USER environment variable is set in those
        # cases (e.g., casapy requires this to correctly operate)
        user = self._user
        env  = {}
        if user is not None:
            env = {'USER':user}

        if self._ensureUserAndSwitch is True:
            # Append commands that will make sure a user is present with the
            # same UID of the current user, and that the command that was
            # supplied for this container runs as that user.
            # Also make sure that the output will belong to that user
            uid = os.getuid()
            createUserAndGo = "id -u {0} &> /dev/null || adduser --uid {0} r; ".format(uid)
            for dirname in set([os.path.dirname(x.path) for x in dockerOutputs.values()]):
                createUserAndGo += 'chown -R {0}.{0} "{1}"; '.format(uid, dirname)
            createUserAndGo += "cd; su -l $(getent passwd {0} | cut -f1 -d:) -c /bin/bash -c '{1}'".format(uid, utils.escapeQuotes(cmd, doubleQuotes=False))

            cmd = createUserAndGo

        # Wrap everything inside bash
        cmd = '/bin/bash -c "%s"' % (utils.escapeQuotes(cmd, singleQuotes=False))

        logger.debug("Command after user creation and wrapping is: %s", cmd)

        c = DockerApp._get_client()

        # Remove the container unless it's specified that we should keep it
        # (used below)
        def rm(container):
            if self._removeContainer:
                container.remove()

        # Create container
        container = c.containers.create(
                self._image,
                cmd,
                volumes=binds,
                user=user,
                environment=env,
        )
        self._containerId = cId = container.id
        logger.info("Created container %s for %r", cId, self)

        # Start it
        start = time.time()
        container.start()
        logger.info("Started container %s", cId)

        # Figure out the container's IP and save it
        # Setting self.containerIp will trigger an event being sent to the
        # registered listeners
        inspection = c.api.inspect_container(cId)
        logger.debug("Docker inspection: %r", inspection)
        self.containerIp = inspection['NetworkSettings']['IPAddress']

        # Wait until it finishes
        # In docker-py < 3 the .wait() method returns the exit code directly
        # In docker-py >= 3 the .wait() method returns a dictionary with the API response
        x = container.wait()
        if isinstance(x, dict) and 'StatusCode' in x:
            self._exitCode = x['StatusCode']
        else:
            self._exitCode = x

        end = time.time()
        logger.info("Container %s finished in %.2f [s] with exit code %d", cId, (end-start), self._exitCode)

        if self._exitCode == 0 and logger.isEnabledFor(logging.DEBUG):
            stdout = container.logs(stream=False, stdout=True, stderr=False)
            stderr = container.logs(stream=False, stdout=False, stderr=True)
            logger.debug("Container %s finished successfully, output follows.\n==STDOUT==\n%s==STDERR==\n%s", cId, stdout, stderr)
        elif self._exitCode != 0:
            stdout = container.logs(stream=False, stdout=True, stderr=False)
            stderr = container.logs(stream=False, stdout=False, stderr=True)
            msg = "Container %s didn't finish successfully (exit code %d)" % (cId, self._exitCode)
            logger.error(msg + ", output follows.\n==STDOUT==\n%s==STDERR==\n%s", stdout, stderr)
            rm(container)
            raise Exception(msg)

        rm(container)
        c.api.close()

    @classmethod
    def _get_client(cls):
        return docker.from_env(version='auto', **cls._kwargs_from_env())

    @classmethod
    def _kwargs_from_env(cls, ssl_version=None, assert_hostname=False):
        """
        Look for parameters to make Docker work under OS X
        :param ssl_version:     which SSL version
        :param assert_hostname: perform hostname checking
        :return:
        """
        config_file_name = os.path.join(utils.getDlgDir(), 'dlg.settings')
        if os.path.exists(config_file_name):
            return ConfigObj(config_file_name)
        return {}