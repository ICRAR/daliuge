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
"""
Module containing docker-related applications and functions
"""

import collections
import logging
import multiprocessing
import multiprocessing.synchronize
import os
import pwd
import threading
import time
import json
from typing import Optional
from overrides import overrides

from configobj import ConfigObj
import docker
from docker.models.containers import Container

from dlg import utils, droputils
from dlg.drop import track_current_drop
from dlg.named_port_utils import replace_named_ports
from dlg.apps.app_base import BarrierAppDROP
from dlg.exceptions import DaliugeException, InvalidDropException


logger = logging.getLogger(f"dlg.{__name__}")

DLG_ROOT = utils.getDlgDir()

DockerPath = collections.namedtuple("DockerPath", "path")


class ContainerIpWaiter(object):
    """
    A class that remembers the target DROP's uid and containerIp properties
    when its internal event has been set, and returns them when waitForIp is
    called, which previously waits for the event to be set.
    """

    def __init__(self, drop):
        self._evt = threading.Event()
        self._uid = drop.uid
        drop.subscribe(self, "containerIp")

    def handleEvent(self, e):
        self._containerIp = e.containerIp
        self._evt.set()

    def waitForIp(self, timeout=None):
        self._evt.wait(timeout)
        return self._uid, self._containerIp


##
# @brief Docker
# @details A component wrapping docker based applications.
# @par EAGLE_START
# @param category Docker
# @param tag template
# @param image /String/ComponentParameter/NoPort/ReadWrite//False/False/The name of the docker image to be used for this application
# @param docker_tag 1.0/String/ComponentParameter/NoPort/ReadWrite//False/False/The tag of the docker image to be used for this application
# @param docker_digest /String/ComponentParameter/NoPort/ReadWrite//False/False/The hexadecimal hash (long version) of the docker image to be used for this application
# @param command /String/ComponentParameter/NoPort/ReadWrite//False/False/The command line to run within the docker instance. Leave empty to execute the default command specified as the entrypoint of the image.
# @param input_redirection /String/ComponentParameter/NoPort/ReadWrite//False/False/The command line argument that specifies the input into this application
# @param output_redirection /String/ComponentParameter/NoPort/ReadWrite//False/False/The command line argument that specifies the output from this application
# @param command_line_arguments /String/ComponentParameter/NoPort/ReadWrite//False/False/Additional command line arguments to be added to the command line to be executed
# @param entrypoint /String/ComponentParameter/NoPort/ReadWrite//False/False/Alternate entrypoint
# @param paramValueSeparator " "/String/ComponentParameter/NoPort/ReadWrite//False/False/Separator character(s) between parameters and their respective values on the command line
# @param argumentPrefix "--"/String/ComponentParameter/NoPort/ReadWrite//False/False/Prefix to each keyed argument on the command line
# @param log_level "NOTSET"/Select/ComponentParameter/NoPort/ReadWrite/NOTSET,DEBUG,INFO,WARNING,ERROR,CRITICAL/False/False/Set the log level for this drop
# @param dropclass dlg.apps.dockerapp.DockerApp/String/ComponentParameter/NoPort/ReadWrite//False/False/Drop class
# @param base_name dockerapp/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @param execution_time 5/Float/ConstraintParameter/NoPort/ReadOnly//False/False/Estimated execution time
# @param num_cpus 1/Integer/ConstraintParameter/NoPort/ReadOnly//False/False/Number of cores used
# @param group_start False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the start of a group?
# @param input_error_threshold 0/Integer/ComponentParameter/NoPort/ReadWrite//False/False/the allowed failure rate of the inputs (in percent), before this component goes to ERROR state and is not executed
# @param n_tries 1/Integer/ComponentParameter/NoPort/ReadWrite//False/False/Specifies the number of times the 'run' method will be executed before finally giving up
# @param user /String/ComponentParameter/NoPort/ReadWrite//False/False/Username of the user who will run the application within the docker image
# @param ensureUserAndSwitch False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Make sure the user specified in the User parameter exists and then run the docker container as that user
# @param removeContainer True/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Instruct Docker engine to delete the container after execution is complete
# @param additionalBindings /String/ComponentParameter/NoPort/ReadWrite//False/False/Directories which will be visible inside the container during run-time. Format is srcdir_on_host:trgtdir_on_container. Multiple entries can be separated by commas.
# @param portMappings /String/ComponentParameter/NoPort/ReadWrite//False/False/Port mappings on the host machine
# @par EAGLE_END
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
    an available program inside the image. Optionally, users can provide a
    working directory (in the container) under which the command will run
    via the `workingDir` parameter.

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

    _container: Optional[Container] = None

    # signals for stopping this drop must first wait
    # for the container to become available
    # TODO: This provides basic multiprocessing safety, alternative approach may
    # be to use a stopcontainer member variable flag. As soon as the container is
    # created the running process checks to see if it should stop. Use lock for
    # atomicity with _container and _stopflag.
    _containerLock = multiprocessing.synchronize.Lock

    @property
    def container(self) -> Optional[Container]:
        return self._container

    def initialize(self, **kwargs):
        self._containerLock = multiprocessing.Lock()
        super().initialize(**kwargs)

        self._image = self._popArg(kwargs, "image", None)
        self._env = self._popArg(kwargs, "env", None)
        self._inputRedirect = self._popArg(kwargs, "input_redirection", "")
        self._outputRedirect = self._popArg(kwargs, "output_redirection", "")
        self._cmdLineArgs = self._popArg(kwargs, "command_line_arguments", "")
        self._applicationArgs = self._popArg(kwargs, "applicationArgs", {})
        self._argumentPrefix = self._popArg(kwargs, "argumentPrefix", "--")
        self._paramValueSeparator = self._popArg(kwargs, "paramValueSeparator", " ")
        self._entryPoint = self._popArg(kwargs, "entrypoint", None)
        if not self._image:
            raise InvalidDropException(
                self, "No docker image specified, cannot create DockerApp"
            )

        if ":" not in self._image:
            logger.warning(
                "%r: Image %s is too generic since it doesn't specify a tag",
                self,
                self._image,
            )

        self._command = self._popArg(kwargs, "command", None)

        self._noBash = False
        if not self._command or self._command.strip()[:2] == "%%":
            logger.warning("Assume a default command is executed in the container")
            self._command = self._command.strip().strip()[2:] if self._command else ""
            self._noBash = True
            # This makes sure that we can retain any command defined in the image, but still be
            # able to add any arguments straight after. This requires to use the placeholder string
            # "%%" at the start of the command, else it is interpreted as a normal command.

        # The user used to run the process in the docker container is now always the user
        # who originally started the DALiuGE process as well. The information is passed through
        # from the host to the engine container (if run as docker) and then further to any
        # container running as a component.

        pw = pwd.getpwuid(os.getuid())
        self._user = pw.pw_name  # use current user by default
        self._userid = pw.pw_uid
        self._groupid = pw.pw_gid
        logger.debug(
            "User for docker container: %s %s:%s",
            self._user, self._userid,self._groupid
        )

        # By default containers are removed from the filesystem, but people
        # might want to preserve them.
        # TODO: This might be something that the data lifecycle manager could
        # handle, but for the time being we do it here
        self._removeContainer = self._popArg(kwargs, "removeContainer", True)

        # Ports - a comma seperated list of the host port mappings of form:
        # "hostport1:containerport1, hostport2:containerport2"
        self._portMappings = self._popArg(kwargs, "portMappings", "")
        logger.info("portMappings: %s", self._portMappings)

        self._shmSize = self._popArg(kwargs, "shmSize", "")
        logger.info("shmSize: %s", self._shmSize)

        # Additional volume bindings can be specified for existing files/dirs
        # on the host system. They are given either as a list or as a
        # comma-separated string
        self._additionalBindings = {}
        bindings = [
            f"{utils.getDlgDir()}:{utils.getDlgDir()}",
            f"{utils.getDlgDir()}/workspace/settings/passwd:/etc/passwd",
            f"{utils.getDlgDir()}/workspace/settings/group:/etc/group",
        ]
        additionalBindings = self._popArg(kwargs, "additionalBindings", [])
        additionalBindings = (
            additionalBindings.split(",")
            if isinstance(additionalBindings, str)
            else additionalBindings
        )
        bindings += additionalBindings
        for binding in bindings:
            if len(binding) == 0:
                continue
            if binding.find(":") == -1:
                host_path = container_path = binding
            else:
                host_path, container_path = binding.split(":")

            # NOTE: The following prevents mounts from host directly into secondary container if DALiuGE is running inside docker already.
            #            if not os.path.exists(host_path):
            #                raise InvalidDropException(self, "'Path %s doesn't exist, cannot use as additional volume binding" % (host_path,))
            self._additionalBindings[host_path] = container_path

        logger.info(
            "%r with image '%s' and command '%s' created",
            self,
            self._image,
            self._command,
        )

        # Check if we have the image; otherwise pull it.
        c = DockerApp._get_client()
        found = any([self._image in im.tags for im in c.images.list()])

        if not found:
            logger.debug("Image '%s' not found, pulling it", self._image)
            start = time.time()
            c.images.pull(self._image)
            end = time.time()
            logger.debug("Took %.2f [s] to pull image '%s'", (end - start), self._image)
        else:
            logger.debug("Image '%s' found, no need to pull it", self._image)

        # Check if the image specifies a working directory
        # If it doesn't use the one provided by the user.
        # If none is provided use the session directory
        inspection = c.api.inspect_image(self._image)
        logger.debug("Docker Image inspection: %r", inspection)
        self.workdir = inspection.get("ContainerConfig", {}).get("WorkingDir", None)
        self.defaultEntryPoint = inspection.get("Config", {}).get("Entrypoint", None)
        # self.workdir = None
        self._sessionId = self._dlg_session_id
        if not self.workdir:
            default_workingdir = os.path.join(utils.getDlgWorkDir(), self._sessionId)
            self.workdir = self._popArg(kwargs, "workingDir", default_workingdir)

        c.api.close()

        self._containerIp = None
        self._containerId = None
        self._waiters = []
        self._recompute_data = {
            "image": self._image,
            "user": self._user,
            "command": self._command,
        }

    @property
    def containerIp(self):
        return self._containerIp

    @containerIp.setter
    def containerIp(self, containerIp):
        self._containerIp = containerIp
        self._fire("containerIp", containerIp=containerIp)

    @property
    def containerId(self):
        return self._containerId

    def handleInterest(self, drop):
        # The only interest we currently have is the containerIp of other
        # DockerApps, and only if our command actually uses this IP
        if isinstance(drop, DockerApp):
            if f"%containerIp[{{{drop.uid}}}]%" in self._command:
                self._waiters.append(ContainerIpWaiter(drop))
                logger.debug("%r: Added ContainerIpWaiter for %r", self, drop)

    @track_current_drop
    def run(self):
        # lock this object to prevent other processes from signaling until the
        # container object is running.
        with self._containerLock:
            # Replace any placeholder in the commandline with the proper path or
            # dataURL, depending on the type of input/output it is
            # In the case of fs-based i/o we replace the command-line with the path
            # that the Drop will receive *inside* the docker container (see below)

            iitems = self._inputs.items()
            oitems = self._outputs.items()
            fsInputs = {uid: i for uid, i in iitems if droputils.has_path(i)}
            fsOutputs = {uid: o for uid, o in oitems if droputils.has_path(o)}
            dockerInputs = {
                # uid: DockerPath(utils.getDlgDir() + i.path) for uid, i in fsInputs.items()
                uid: DockerPath(i.path)
                for uid, i in fsInputs.items()
            }
            dockerOutputs = {
                # uid: DockerPath(utils.getDlgDir() + o.path) for uid, o in fsOutputs.items()
                uid: DockerPath(o.path)
                for uid, o in fsOutputs.items()
            }

            # We bind the inputs and outputs inside the docker under the utils.getDlgDir()
            # directory, maintaining the rest of their original paths.
            # Outputs are bound only up to their dirname (see class doc for details)
            # Volume bindings are setup for FileDROPs and DirectoryContainers only
            binds = [
                i.path + ":" + dockerInputs[uid].path for uid, i in fsInputs.items()
            ]
            binds += [
                os.path.dirname(o.path) + ":" + os.path.dirname(dockerOutputs[uid].path)
                for uid, o in fsOutputs.items()
            ]
            logger.debug("Input/output bindings: %r", binds)
            if (
                len(self._additionalBindings.items()) > 0
            ):  # else we end up with a ':' in the mounts list
                logger.debug("Additional bindings: %r", self._additionalBindings)
                binds += [
                    host_path + ":" + container_path
                    for host_path, container_path in self._additionalBindings.items()
                ]
            binds = list(set(binds))  # make this a unique list else docker complains
            try:
                binds.remove(":")
            except ValueError:
                pass
            logger.debug("Volume bindings: %r", binds)

            portMappings = {}
            for mapping in self._portMappings.split(","):
                if mapping:
                    if mapping.find(":") == -1:
                        host_port = container_port = mapping
                    else:
                        host_port, container_port = mapping.split(":")
                    if host_port not in portMappings:
                        logger.debug("mapping port %s -> %s",
                                     host_port, container_port)
                        portMappings[host_port] = int(container_port)
                    else:
                        raise Exception(
                            f"Duplicate port {host_port} in container port mappings"
                        )
            logger.debug("port mappings: %s", portMappings)

            # deal with environment variables
            env = {}
            env.update({"DLG_UID": self._uid})
            if self._dlg_session_id:
                env.update({"DLG_SESSION_ID": self._dlg_session_id})
            if self._user is not None:
                env.update({"USER": self._user, "DLG_ROOT": utils.getDlgDir()})
            if self._env is not None:
                logger.debug("Found environment variable setting: %s", self._env)
                if (
                    self._env.lower() == "all"
                ):  # pass on all environment variables from host
                    env.update(os.environ)
                elif self._env[0] in ["{", "["]:
                    try:
                        addEnv = json.loads(self._env)
                    except json.JSONDecodeError:
                        logger.warning(
                            "Ignoring provided environment variables: Format wrong? Check documentation"
                        )
                        addEnv = {}
                    if isinstance(addEnv, dict):  # if it is a dict populate directly
                        # but replace placeholders first
                        for key in addEnv:
                            value = droputils.replace_placeholders(
                                addEnv[key], dockerInputs, dockerOutputs
                            )
                            addEnv[key] = value
                        env.update(addEnv)
                    elif isinstance(
                        addEnv, list
                    ):  # if it is a list populate from host environment
                        for e in addEnv:
                            env.update(os.environ[e])
                else:
                    logger.warning(
                        "Ignoring provided environment variables: Format wrong! Check documentation"
                    )
            logger.debug("Adding environment variables: %s",env)

            # deal with named ports
            appArgs = self._applicationArgs
            inport_names = (
                self.parameters["inputs"] if "inputs" in self.parameters else []
            )
            outport_names = (
                self.parameters["outputs"] if "outputs" in self.parameters else []
            )
            keyargs, pargs = replace_named_ports(
                iitems,
                oitems,
                inport_names,
                outport_names,
                appArgs,
            )

            # complete command including all additional parameters and optional redirects
            if self._command:
                cmd = droputils.replace_placeholders(
                    self._command, dockerInputs, dockerOutputs
                )
                for key, value in keyargs.items():
                    cmd = cmd.replace(f"{{{key}}}", str(value))
                for key, value in pargs.items():
                    cmd = cmd.replace(f"{{{key}}}", str(value))
            else:
                cmd = ""


            ###############
            # Wait until the DockerApps this application runtime depends on have
            # started, and replace their IP placeholders by the real IPs
            for waiter in self._waiters:
                uid, ip = waiter.waitForIp()
                cmd = cmd.replace(f"%containerIp[{{{uid}}}]%", ip)
                logger.debug("Command after IP replacement is: %s", cmd)

            # Wrap everything inside bash
            if len(cmd) > 0 and not self._noBash:
                self._entryPoint = (
                    '/bin/bash -c "%s"'
                    % (utils.escapeQuotes(cmd, singleQuotes=False)).strip()
                )
                logger.info("Command after user creation and wrapping is: %s", cmd)
            else:
                # cmd = f"{utils.escapeQuotes(cmd, singleQuotes=False)}".strip()
                # cmd = cmd.split(" ")
                logger.info(
                    "executing container with default cmd %s and wrapped arguments: %s",
                    self.defaultEntryPoint,
                    cmd,
                )

            c = DockerApp._get_client()
            logger.debug("Final user for container: %s:%s",
                         self._user, self._userid)

            # Create container
            self._container = c.containers.create(  # type: ignore
                self._image,
                cmd,
                volumes=binds,
                ports=portMappings,
                user=f"{self._userid}:{self._groupid}",
                environment=env,
                working_dir=self.workdir,
                init=True,
                shm_size=self._shmSize,
                # TODO: daliuge will handle automatic removal (otherwise check before stopping container)
                # auto_remove=self._removeContainer,
                detach=True,
                entrypoint=f"{self._entryPoint}" if self._entryPoint else None,
            )

        if not self.container:
            raise DaliugeException("docker container failed")
        else:
            self._containerId = cId = self.container.id
            logger.info("Created container %s for %r", cId, self)
            logger.debug("autoremove container %s", self._removeContainer)

            # Start it
            start = time.time()
            self.container.start()
            logger.info("Started container %s", cId)

            # Figure out the container's IP and save it
            # Setting self.containerIp will trigger an event being sent to the
            # registered listeners
            inspection = c.api.inspect_container(cId)
            logger.debug("Docker inspection: %r", inspection)
            self.containerIp = inspection["NetworkSettings"]["IPAddress"]

            # Wait until it finishes
            # In docker-py < 3 the .wait() method returns the exit code directly
            # In docker-py >= 3 the .wait() method returns a dictionary with the API response
            x = self.container.wait()
            logger.debug("container %s finished", cId)

            if isinstance(x, dict) and "StatusCode" in x:
                self._exitCode = x["StatusCode"]
            else:
                self._exitCode = x

            end = time.time()

            # Capture output
            stdout = self.container.logs(stream=False, stdout=True, stderr=False)
            stderr = self.container.logs(stream=False, stdout=False, stderr=True)
            if isinstance(stdout, bytes):
                stdout = stdout.decode()
                stderr = stderr.decode()
            logger.info(
                "Container %s finished in %.2f [s] with exit code %d",
                cId,
                (end - start),
                self._exitCode,
            )

            if self._exitCode == 0 and logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "Container %s finished successfully, output follows.\n==STDOUT==\n%s==STDERR==\n%s",
                    cId,
                    stdout,
                    stderr,
                )
            elif self._exitCode != 0:
                msg = f"Container {cId} didn't finish successfully (exit code {self._exitCode})"
                if (
                    self._exitCode == 137
                    or self._exitCode == 139
                    or self._exitCode == 143
                ):
                    # termination via SIGKILL, SIGSEGV, and SIGTERM is expected for some services
                    logger.warning(
                        "%s, output follows.\n==STDOUT==\n%s==STDERR==\n%s",
                        msg,
                        stdout,
                        stderr,
                    )
                else:
                    logger.error(
                        "%s, output follows.\n==STDOUT==\n%s==STDERR==\n%s",
                        msg,
                        stdout,
                        stderr,
                    )
                    raise Exception(msg)

            if self._removeContainer:
                self.container.remove()
        c.api.close()

    @overrides
    def setCompleted(self):
        with self._containerLock:
            super().setCompleted()
            if self.container is not None:
                self.container.stop()
            else:
                logger.debug("docker app completed but container is None")

    @overrides
    def setError(self):
        with self._containerLock:
            super().setError()
            if self.container is not None:
                self.container.stop()
            else:
                logger.debug("docker app errored but container is None")

    @overrides
    def cancel(self):
        with self._containerLock:
            super().cancel()
            if self.container is not None:
                self.container.stop()
            else:
                logger.debug("docker app canceled but container is None")

    @overrides
    def skip(self):
        with self._containerLock:
            super().skip()
            if self.container is not None:
                self.container.stop()
            else:
                logger.debug("docker app skipped but container is None")

    @classmethod
    def _get_client(cls):
        return docker.from_env(version="auto", **cls._kwargs_from_env())

    @classmethod
    def _kwargs_from_env(cls):
        """
        Look for parameters to make Docker work under OS X
        :return:
        """
        config_file_name = os.path.join(utils.getDlgDir(), "dlg.settings")
        if os.path.exists(config_file_name):
            return ConfigObj(config_file_name)
        return {}

    def generate_recompute_data(self):
        return self._recompute_data
