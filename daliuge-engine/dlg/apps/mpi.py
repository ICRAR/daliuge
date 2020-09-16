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

import logging
import signal
import subprocess
import sys

from ..drop import BarrierAppDROP
from ..exceptions import InvalidDropException


logger = logging.getLogger(__name__)

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
    """

    def initialize(self, **kwargs):
        super(MPIApp, self).initialize(**kwargs)

        self._command = self._getArg(kwargs, 'command', None)
        self._maxprocs = self._getArg(kwargs, 'maxprocs', 1)
        self._use_wrapper = self._getArg(kwargs, 'use_wrapper', False)
        self._args = self._getArg(kwargs, 'args', [])
        if not self._command:
            raise InvalidDropException(self, 'No command specified, cannot create MPIApp')

    def run(self):
        from mpi4py import MPI

        cmd, args = self._command, self._args
        if self._use_wrapper:
            # We spawn this very same module
            # When invoked as a program (see at the bottom) this module
            # will get the parent communicator, run the program we're giving in the
            # command line, and send back the exit code.
            # Likewise, we barrier on the children communicator, and thus
            # we wait until all children processes are completed
            cmd = sys.executable
            args = ['-m', __name__, self._command] + self._args

        errcodes = []

        # Spawn the new MPI communicator and wait until it finishes
        # (it sends the stdout, stderr and exit codes of the programs)
        logger.info("Executing MPI app in new communicator with %d ranks and command: %s %s", self._maxprocs, cmd, args)

        vendor, version = MPI.get_vendor()  # @UndefinedVariable
        info = MPI.Info.Create()  # @UndefinedVariable
        logger.debug("MPI vendor is %s, version %s", vendor, '.'.join([str(x) for x in version]))  # @UndefinedVariable
        comm_children = MPI.COMM_SELF.Spawn(cmd, args=args, maxprocs=self._maxprocs, errcodes=errcodes, info=info)  # @UndefinedVariable

        n_children = comm_children.Get_remote_size()
        logger.info("%d MPI children apps spawned, gathering exit data", n_children)

        if self._use_wrapper:
            children_data = comm_children.gather(('','', 0), root=MPI.ROOT)  # @UndefinedVariable
            exit_codes = [x[2] for x in children_data]
            logger.info("Exit codes gathered from children processes: %r", exit_codes)

            any_failed = False
            for rank, (stdout, stderr, code) in enumerate(children_data):
                if code == 0:
                    continue
                any_failed = True
                logger.error("stdout/stderr follow for rank %d:\nSTDOUT\n======\n%s\n\nSTDERR\n======\n%s", rank, stdout, stderr)

            if any_failed:
                raise Exception("One or more MPI children didn't exit cleanly")
        else:
            comm_children.barrier()


# When we are called by the MPIApp
def module_as_main():

    # Get the parent communicator before anything else happens
    # This way we ensure the communicator is valid
    from mpi4py import MPI
    parent_comm = MPI.Comm.Get_parent()  # @UndefinedVariable

    def handle(signNo, stack_frame):
        parent_comm.gather(('', 'Received signal %d' % (signNo,), -1), root=0)

    signal.signal(signal.SIGINT, handle)
    signal.signal(signal.SIGTERM, handle)
    signal.signal(signal.SIGABRT, handle)

    # argv[0] is the name of this module
    # argv[1:] is the actual command + args
    try:
        proc = subprocess.Popen(sys.argv[1:], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False, close_fds=False)
        stdout, stderr = proc.communicate()
        code = proc.returncode
    except Exception as e:
        stdout, stderr, code = '', str(e), -1

    # Gather the results in the spawner rank and good bye
    parent_comm.gather((stdout, stderr, code), root=0)

if __name__ == '__main__':
    module_as_main()