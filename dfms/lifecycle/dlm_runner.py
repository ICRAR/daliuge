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
A small module that runs the the DLM as a standalone application and registers
it with Pyro so others can access it

@author: rtobar
'''

import dlm
import logging
import sys

import Pyro4


logger = logging.getLogger(__name__)


class DataLifecycleManagerRunner(object):

    def __init__(self, *args):
        self._dlm = dlm.DataLifecycleManager()

    def start(self):

        logger.debug("Starting DLM")
        self._dlm.startup()

        logger.debug("Serving DLM through pyro")
        daemon = Pyro4.Daemon()
        daemon.register(self._dlm)

        # Main loop
        daemon.requestLoop()

        daemon.close()
        self._dlm.cleanup()

def main():
    logging.basicConfig(level=logging.DEBUG)
    logger.info("Starting DLM application")
    runner = DataLifecycleManagerRunner(sys.argv)
    runner.start()

if __name__ == '__main__':
    main()