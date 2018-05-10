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

import base64
import collections
import importlib
import inspect
import logging

import dill
import six
import six.moves.cPickle as pickle  # @UnresolvedImport

from .. import droputils
from ..drop import BarrierAppDROP
from ..exceptions import InvalidDropException


logger = logging.getLogger(__name__)

def serialize_func(f):

    if isinstance(f, six.string_types):
        parts = f.split('.')
        f = getattr(importlib.import_module('.'.join(parts[:-1])), parts[-1])

    fser = dill.dumps(f)
    fdefaults = {}
    a = inspect.getargspec(f)
    if a.defaults:
        fdefaults = dict(zip(a.args[-len(a.defaults):], a.defaults))
    return fser, fdefaults


def import_using_name(app, fname):
    # The name has the form pack1.pack2.mod.func
    parts = fname.split('.')
    if len(parts) < 2:
        msg = '%s does not contain a module name' % fname
        raise InvalidDropException(app, msg)

    modname, fname = '.'.join(parts[:-1]), parts[-1]
    try:
        mod = importlib.import_module(modname, __name__)
        return getattr(mod, fname)
    except ImportError as e:
        raise InvalidDropException(app, 'Error when loading module %s: %s' % (modname, str(e)))
    except AttributeError:
        raise InvalidDropException(app, 'Module %s has no member %s' % (modname, fname))

def import_using_code(code):
    return dill.loads(code)

class PyFuncApp(BarrierAppDROP):
    """
    An application that wraps a simple python function.

    The inputs of the application are treated as the arguments of the function.
    Conversely, the output of the function is treated as the output of the
    application. If the application has more than one output, the result of
    calling the function is treated as an iterable, with each individual object
    being written to its corresponding output.

    Users indicate the function to be wrapped via the ``func_name`` parameter.
    Otherwise, users can also *send* over the python code using the ``func_code``
    parameter. The code needs to be base64-encoded and produced with the marshal
    module of the same Python version used to run DALiuGE.

    Both inputs and outputs are serialized using the pickle protocol.
    """

    def initialize(self, **kwargs):
        BarrierAppDROP.initialize(self, **kwargs)

        self.fname = fname = self._getArg(kwargs, 'func_name', None)
        fcode = self._getArg(kwargs, 'func_code', None)
        if not fname and not fcode:
            raise InvalidDropException(self, 'No function specified (either via name or code)')

        if not fcode:
            self.f = import_using_name(self, fname)
        else:
            if not isinstance(fcode, six.binary_type):
                fcode = base64.b64decode(six.b(fcode))
            self.f = import_using_code(fcode)

        # Mapping from argname to default value. Should match only the last part
        # of the argnames list
        self.fdefaults = self._getArg(kwargs, 'func_defaults', {}) or {}
        logger.debug("Default values for function: %r", self.fdefaults)

        # Mapping between argument name and input drop uids
        self.func_arg_mapping = self._getArg(kwargs, 'func_arg_mapping', {})
        logger.debug("Input mapping: %r", self.func_arg_mapping)

    def run(self):

        # Inputs are un-pickled and treated as the arguments of the function
        # Their order must be preserved, so we use an OrderedDict
        all_contents = lambda x: pickle.loads(droputils.allDropContents(x))
        inputs = collections.OrderedDict()
        for uid, i in self._inputs.items():
            inputs[uid] = all_contents(i)

        # Keyword arguments are made up by the default values plus the inputs
        # that match one of the keyword argument names
        kwargs = {name: inputs.pop(uid)
                  for name, uid in self.func_arg_mapping.items()
                  if name in self.fdefaults}

        # The rest of the inputs are the positional arguments
        args = list(inputs.values())

        logger.debug("Running %s with args=%r, kwargs=%r", self.fname, args, kwargs)
        result = self.f(*args, **kwargs)

        # Depending on how many outputs we have we treat our result
        # as an iterable or as a single object. Each result is pickled
        # and written to its corresponding output
        outputs = self.outputs
        if len(outputs) == 1:
            result = [result]
        for r, o in zip(result, outputs):
            o.write(pickle.dumps(r))  # @UndefinedVariable