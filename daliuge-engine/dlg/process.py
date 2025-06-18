#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2014
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
Module extends the multiprocessing.process class to include some convenient accouterments.
Based on approach in https://stackoverflow.com/questions/19924104/python-multiprocessing-handling-child-errors-in-parent
"""
import multiprocessing
import traceback


class DlgProcess(multiprocessing.Process):
    def __init__(self, *args, **kwargs):
        multiprocessing.Process.__init__(self, *args, **kwargs)
        self._parentConn, self._childConn = multiprocessing.Pipe()
        self._exception = None

    def run(self):
        try:
            multiprocessing.Process.run(self)
            self._childConn.send(None)
        except multiprocessing.ProcessError as e:
            tb = traceback.format_exc()
            self._childConn.send((e, tb))

    @property
    def exception(self):
        if self._parentConn.poll():
            e = self._parentConn.recv()
            if e:
                e = type(e[0])(e[1])
            self._exception = e
        return self._exception
