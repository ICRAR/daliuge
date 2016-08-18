#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia
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
An app to run bash commands
"""

import logging
import os
import subprocess
import time

from dfms import utils, droputils
from dfms.drop import BarrierAppDROP, FileDROP, DirectoryContainer
from dfms.exceptions import InvalidDropException


logger = logging.getLogger(__name__)


class BashShellApp(BarrierAppDROP):
    def __init__(self, oid, uid, **kwargs):
        self._command = None
        self._exit_code = None
        super(BashShellApp, self).__init__(oid, uid, **kwargs)

    def initialize(self, **kwargs):
        BarrierAppDROP.initialize(self, **kwargs)

        self._command = self._getArg(kwargs, 'command', None)
        if not self._command:
            raise InvalidDropException(self, 'No command specified, cannot create BashShellApp')

    def run(self):

        def isFSBased(x):
            return isinstance(x, (FileDROP, DirectoryContainer))

        fsInputs = {uid: i for uid,i in self._inputs.items() if isFSBased(i)}
        fsOutputs = {uid: o for uid,o in self._outputs.items() if isFSBased(o)}
        dataURLInputs = {uid: i for uid,i in self._inputs.items() if not isFSBased(i)}
        dataURLOutputs = {uid: o for uid,o in self._outputs.items() if not isFSBased(o)}

        cmd = droputils.replace_path_placeholders(self._command, fsInputs, fsOutputs)
        cmd = droputils.replace_dataurl_placeholders(cmd, dataURLInputs, dataURLOutputs)

        # Wrap everything inside bash
        cmd = '/bin/bash -c "{0}"'.format(utils.escapeQuotes(cmd, singleQuotes=False))

        logger.debug("Command after user creation and wrapping is: %s", cmd)

        start = time.time()

        # Wait until it finishes
        process = subprocess.Popen(cmd, bufsize=1, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os.environ.copy())
        stdout, stderr = process.communicate()
        self._exit_code = process.returncode
        end = time.time()
        logger.info("Finished in %.3f [s] with exit code %d", (end-start), self._exit_code)

        if self._exit_code == 0 and logger.isEnabledFor(logging.DEBUG):
            logger.debug("Command finished successfully, output follows.\n==STDOUT==\n{0}==STDERR==\n{1}".format(stdout, stderr))
        elif self._exit_code != 0:
            message = "Command didn't finish successfully (exit code {0})".format(self._exit_code)
            logger.error(message + ", output follows.\n==STDOUT==\n%s==STDERR==\n%s" % (stdout, stderr))
            raise Exception(message)

    def dataURL(self):
        return type(self).__name__