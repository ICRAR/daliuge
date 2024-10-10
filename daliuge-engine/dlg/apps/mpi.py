#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2017
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
"""Module containing MPI application wrapping support"""
<<<<<<< HEAD
import json
import logging
import os
=======

import logging
>>>>>>> master
import signal
import subprocess
import sys

<<<<<<< HEAD
from dlg import utils, droputils
from dlg.apps.app_base import BarrierAppDROP
from dlg.named_port_utils import (
    DropParser,
    get_port_reader_function,
    replace_named_ports,
)
from dlg.exceptions import InvalidDropException
from ..meta import (
    dlg_enum_param,
)
=======
from dlg.apps.app_base import BarrierAppDROP
from ..exceptions import InvalidDropException
>>>>>>> master

logger = logging.getLogger(__name__)


##
# @brief MPI
# @details An application component using the Message Passing Interface (MPI)
# @par EAGLE_START
# @param category Mpi
# @param tag template
<<<<<<< HEAD
# @param command /String/ComponentParameter/NoPort/ReadWrite//False/False/The command to be executed
# @param args /String/ComponentParameter/NoPort/ReadWrite//False/False/Additional command line arguments to be added to the command line to be executed
# @param num_of_procs 1/Integer/ComponentParameter/NoPort/ReadWrite//False/False/Number of processes used for this application
# @param use_wrapper False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/If the command should be executed inside the existing MPI communicator set this to True
# @param input_redirection /String/ComponentParameter/NoPort/ReadWrite//False/False/The command line argument that specifies the input into this application
# @param output_redirection /String/ComponentParameter/NoPort/ReadWrite//False/False/The command line argument that specifies the output from this application
# @param paramValueSeparator " "/String/ComponentParameter/NoPort/ReadWrite//False/False/Separator character(s) between parameters on the command line
# @param argumentPrefix "--"/String/ComponentParameter/NoPort/ReadWrite//False/False/Prefix to each keyed argument on the command line
# @param dropclass dlg.apps.mpi.MPIApp/String/ComponentParameter/NoPort/ReadWrite//False/False/Drop class
=======
# @param num_of_procs 1/Integer/ComponentParameter/NoPort/ReadWrite//False/False/Number of processes used for this application
# @param command /String/ComponentParameter/NoPort/ReadWrite//False/False/The command to be executed
# @param input_redirection /String/ComponentParameter/NoPort/ReadWrite//False/False/The command line argument that specifies the input into this application
# @param output_redirection /String/ComponentParameter/NoPort/ReadWrite//False/False/The command line argument that specifies the output from this application
# @param command_line_arguments /String/ComponentParameter/NoPort/ReadWrite//False/False/Additional command line arguments to be added to the command line to be executed
# @param paramValueSeparator " "/String/ComponentParameter/NoPort/ReadWrite//False/False/Separator character(s) between parameters on the command line
# @param argumentPrefix "--"/String/ComponentParameter/NoPort/ReadWrite//False/False/Prefix to each keyed argument on the command line
# @param dropclass dlg.apps.mpi.MPIApp/String/ComponentParameter/NoPort/ReadWrite//False/False/Drop class
# @param mpi construct/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
>>>>>>> master
# @param execution_time 5/Float/ConstraintParameter/NoPort/ReadOnly//False/False/Estimated execution time
# @param num_cpus 1/Integer/ConstraintParameter/NoPort/ReadOnly//False/False/Number of cores used
# @param group_end False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the end of a group?
# @param input_error_threshold 0/Integer/ComponentParameter/NoPort/ReadWrite//False/False/the allowed failure rate of the inputs (in percent), before this component goes to ERROR state and is not executed
# @param n_tries 1/Integer/ComponentParameter/NoPort/ReadWrite//False/False/Specifies the number of times the 'run' method will be executed before finally giving up
# @param input_parser pickle/Select/ComponentParameter/NoPort/ReadWrite/raw,pickle,eval,npy,path,dataurl/False/False/Input port parsing technique
# @param output_parser pickle/Select/ComponentParameter/NoPort/ReadWrite/raw,pickle,eval,npy,path,dataurl/False/False/Output port parsing technique
# @par EAGLE_END
class MPIApp(BarrierAppDROP):
    """
    An application drop representing an MPI job.

    This application needs to be launched from within an MPI environment,
    and therefore the hosting NM must be part of an MPI communicator.
    This application uses MPI_Comm_Spawn to fire up the requested MPI
    application, which must *not* be aware of it having a parent.
    This drop will gather the individual exit codes from the launched
    applications and transition to ERROR if any of them did not exit cleanly,
    or to FINISHED if all of them finished successfully.
<<<<<<< HEAD

    """

    input_parser: DropParser = dlg_enum_param(DropParser, "input_parser", DropParser.PICKLE)  # type: ignore

    def initialize(self, **kwargs):
        super(MPIApp, self).initialize(**kwargs)

        self._maxprocs = self._popArg(kwargs, "maxprocs", 1)
        self._use_wrapper = self._popArg(kwargs, "use_wrapper", False)
        self._args = self._popArg(kwargs, "args", "")
        self._applicationArgs = self._popArg(kwargs, "applicationArgs", {})
        self._argumentPrefix = self._popArg(kwargs, "argumentPrefix", "--")
        self._paramValueSeparator = self._popArg(kwargs, "paramValueSeparator", " ")
        self._inputRedirect = self._popArg(kwargs, "input_redirection", "")
        self._outputRedirect = self._popArg(kwargs, "output_redirection", "")

        self._command = self._popArg(kwargs, "command", None)
=======
    """

    def initialize(self, **kwargs):
        super(MPIApp, self).initialize(**kwargs)

        self._command = self._popArg(kwargs, "command", None)
        self._maxprocs = self._popArg(kwargs, "maxprocs", 1)
        self._use_wrapper = self._popArg(kwargs, "use_wrapper", False)
        self._args = self._popArg(kwargs, "args", [])
>>>>>>> master
        if not self._command:
            raise InvalidDropException(
                self, "No command specified, cannot create MPIApp"
            )
<<<<<<< HEAD
        self._recompute_data = {}
=======
>>>>>>> master

    def run(self):
        from mpi4py import MPI

        cmd, args = self._command, self._args
<<<<<<< HEAD
        inputs = self._inputs
        outputs = self._outputs

        logger.debug("Parameters found: %s", json.dumps(self.parameters))
        logger.debug("MPI Inputs: %s; MPI Outputs: %s", inputs, outputs)
        # we only support passing a path for bash apps
        fsInputs = {uid: i for uid, i in inputs.items() if droputils.has_path(i)}
        fsOutputs = {uid: o for uid, o in outputs.items() if droputils.has_path(o)}
        dataURLInputs = {
            uid: i for uid, i in inputs.items() if not droputils.has_path(i)
        }
        dataURLOutputs = {
            uid: o for uid, o in outputs.items() if not droputils.has_path(o)
        }
        # deal with named ports
        inport_names = self.parameters["inputs"] if "inputs" in self.parameters else []
        outport_names = (
            self.parameters["outputs"] if "outputs" in self.parameters else []
        )
        reader = get_port_reader_function(self.input_parser)
        keyargs, pargs = replace_named_ports(
            inputs.items(),
            outputs.items(),
            inport_names,
            outport_names,
            self._applicationArgs,
            argumentPrefix=self._argumentPrefix,
            separator=self._paramValueSeparator,
            parser=reader,
        )
        argumentString = (
            f"{' '.join(map(str,pargs + keyargs))}"  # add kwargs to end of pargs
        )
        # complete command including all additional parameters and optional redirects
        if len(argumentString.strip()) > 0:
            # the _cmdLineArgs would very likely make the command line invalid
            cmd = f"{self._command} {argumentString} "
        else:
            cmd = f"{self._command} {argumentString} {args} "
        if self._outputRedirect:
            cmd = f"{cmd} > {self._outputRedirect}"
        if self._inputRedirect:
            cmd = f"cat {self._inputRedirect} > {cmd}"
        cmd = cmd.strip()

        app_uid = self.uid

        # Replace inputs/outputs in command line with paths or data URLs
        cmd = droputils.replace_path_placeholders(cmd, fsInputs, fsOutputs)

        cmd = droputils.replace_dataurl_placeholders(cmd, dataURLInputs, dataURLOutputs)

        # Pass down daliuge-specific information to the subprocesses as environment variables
        env = os.environ.copy()
        env.update({"DLG_UID": self._uid})
        if self._dlg_session_id:
            env.update({"DLG_SESSION_ID": self._dlg_session_id})

        env.update({"DLG_ROOT": utils.getDlgDir()})

        logger.info("Command after wrapping is: %s", cmd)

=======
>>>>>>> master
        if self._use_wrapper:
            # We spawn this very same module
            # When invoked as a program (see at the bottom) this module
            # will get the parent communicator, run the program we're giving in the
            # command line, and send back the exit code.
            # Likewise, we barrier on the children communicator, and thus
            # we wait until all children processes are completed
<<<<<<< HEAD
            args = ["-m", __name__, cmd]
            cmd = sys.executable
=======
            cmd = sys.executable
            args = ["-m", __name__, self._command] + self._args
>>>>>>> master

        errcodes = []

        # Spawn the new MPI communicator and wait until it finishes
        # (it sends the stdout, stderr and exit codes of the programs)
        logger.info(
            "Executing MPI app in new communicator with %d ranks and command: %s %s",
            self._maxprocs,
            cmd,
            args,
        )

        vendor, version = MPI.get_vendor()  # @UndefinedVariable
        info = MPI.Info.Create()  # @UndefinedVariable
        logger.debug(
            "MPI vendor is %s, version %s",
            vendor,
            ".".join([str(x) for x in version]),
        )  # @UndefinedVariable
        comm_children = MPI.COMM_SELF.Spawn(
            cmd,
            args=args,
            maxprocs=self._maxprocs,
            errcodes=errcodes,
            info=info,
        )  # @UndefinedVariable

        n_children = comm_children.Get_remote_size()
        logger.info("%d MPI children apps spawned, gathering exit data", n_children)

        if self._use_wrapper:
            children_data = comm_children.gather(
                ("", "", 0), root=MPI.ROOT
            )  # @UndefinedVariable
            exit_codes = [x[2] for x in children_data]
            logger.info("Exit codes gathered from children processes: %r", exit_codes)

            any_failed = False
            for rank, (stdout, stderr, code) in enumerate(children_data):
                self._recompute_data[str(rank)] = [
                    code,
                    str(stdout),
                    str(stderr),
                ]
                if code == 0:
                    continue
                any_failed = True
                logger.error(
                    "stdout/stderr follow for rank %d:\nSTDOUT\n======\n%s\n\nSTDERR\n======\n%s",
                    rank,
                    stdout,
                    stderr,
                )

            if any_failed:
                raise Exception("One or more MPI children didn't exit cleanly")
        else:
            comm_children.barrier()

    def generate_recompute_data(self):
        return self._recompute_data


# When we are called by the MPIApp
def module_as_main():
    # Get the parent communicator before anything else happens
    # This way we ensure the communicator is valid
    from mpi4py import MPI

    parent_comm = MPI.Comm.Get_parent()  # @UndefinedVariable

    def handle(signNo, stack_frame):
        parent_comm.gather(("", "Received signal %d" % (signNo,), -1), root=0)

    signal.signal(signal.SIGINT, handle)
    signal.signal(signal.SIGTERM, handle)
    signal.signal(signal.SIGABRT, handle)

    # argv[0] is the name of this module
    # argv[1:] is the actual command + args
    try:
        proc = subprocess.Popen(
            sys.argv[1:],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            close_fds=False,
        )
        stdout, stderr = proc.communicate()
        code = proc.returncode
    except Exception as e:
        stdout, stderr, code = "", str(e), -1

    # Gather the results in the spawner rank and good bye
    parent_comm.gather((stdout, stderr, code), root=0)


if __name__ == "__main__":
    module_as_main()
