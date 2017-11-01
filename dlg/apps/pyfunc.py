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
"""Module implementing the PyFuncApp class"""

import importlib

import six.moves.cPickle as pickle  # @UnresolvedImport

from .. import droputils
from ..drop import BarrierAppDROP
from ..exceptions import InvalidDropException


class PyFuncApp(BarrierAppDROP):
    """
    An application that wraps a simple python function.

    The inputs of the application are treated as the arguments of the function.
    Conversely, the output of the function is treated as the output of the
    application. If the application has more than one output, the result of
    calling the function is treated as an iterable, with each individual object
    being written to its corresponding output.

    Users indicate the function to be wrapped via the ``function`` parameter,
    which is of course mandatory.

    Both inputs and outputs are serialized using the pickle protocol.
    """

    def initialize(self, **kwargs):
        BarrierAppDROP.initialize(self, **kwargs)

        fname = self._getArg(kwargs, 'function', None)
        if not fname:
            raise InvalidDropException(self, 'No function specified')

        # The name has the form pack1.pack2.mod.func
        parts = fname.split('.')
        if len(parts) < 2:
            msg = '%s does not contain a module name' % fname
            raise InvalidDropException(self, msg)

        modname, fname = '.'.join(parts[:-1]), parts[-1]
        try:
            mod = importlib.import_module(modname, __name__)
            self.f = getattr(mod, fname)
        except ImportError as e:
            raise InvalidDropException(self, 'Error when loading module %s: %s' % (modname, str(e)))
        except AttributeError:
            raise InvalidDropException(self, 'Module %s has no member %s' % (modname, fname))

    def run(self):

        # Inputs are un-pickled and treated as the arguments of the function
        args = map(lambda x: pickle.loads(droputils.allDropContents(x)), self.inputs)  # @UndefinedVariable
        result = self.f(*args)

        # Depending on how many outputs we have we treat our result
        # as an iterable or as a single object. Each result is pickled
        # and written to its corresponding output
        outputs = self.outputs
        if len(outputs) == 1:
            result = [result]
        for r, o in zip(result, outputs):
            o.write(pickle.dumps(r))  # @UndefinedVariable