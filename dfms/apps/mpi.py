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
import subprocess
import sys

from dfms.drop import BarrierAppDROP
from dfms.exceptions import InvalidDropException


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
        self._args = self._getArg(kwargs, 'args', [])
        if not self._command:
            raise InvalidDropException(self, 'No command specified, cannot create MPIApp')

    def run(self):
        from mpi4py import MPI

        # We spawn this very same module
        # When invoked as a program (see at the bottom) this module
        # will get the parent communicator, run the program we're giving in the
        # command line, and send back the exit code.
        # Likewise, we barrier on the children communicator, and thus
        # we wait until all children processes are completed
        args = ['-m', __name__, self._command] + self._args

        # Spawn the new MPI communicator and wait until it finishes
        # (it sends the stdout, stderr and exit codes of the programs)
        logger.info("Executing MPIApp %r in a new communicator", self)
        comm_self = MPI.COMM_SELF  # @UndefinedVariable
        comm_children = comm_self.Spawn(sys.executable, args=args, maxprocs=self._maxprocs)
        logger.info("MPI children apps spawned, gathering exit data")
        children_data = comm_children.gather((0, '','', 0), root=MPI.ROOT)  # @UndefinedVariable
        exit_codes = [x[3] for x in children_data]
        logger.debug("Exit codes gathered from children processes: %r", exit_codes)

        any_failed = False
        for rank, stdout, stderr, code in children_data:
            if code == 0:
                continue
            any_failed = True
            logger.error("stdout/stderr follow for rank %d:\nSTDOUT\n======\n%s\n\nSTDERR\n======\n%s", rank, stdout, stderr)

        if any_failed:
            raise Exception("One or more MPI children didn't exit cleanly")


# When we are called by the MPIApp
def module_as_main():

    # Get the parent communicator
    from mpi4py import MPI
    r = MPI.COMM_WORLD.Get_rank()  # @UndefinedVariable
    parent_comm = MPI.Comm.Get_parent()  # @UndefinedVariable

    # Now spawn the actual process, wait until it finishes,
    # and barrier on the parent communicator so the MPIApp drop
    # knows when all children have finished
    try:
        proc = subprocess.Popen(sys.argv[1:], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        code = proc.returncode
    except Exception as e:
        stdout, stderr, code = '', str(e), -1
    finally:
        parent_comm.gather((r, stdout, stderr, code), root=0)

if __name__ == '__main__':
    module_as_main()