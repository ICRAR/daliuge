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
"""Utilities to emulate the `dask.delayed` function"""

import base64
import contextlib
import logging
import socket
import struct
import time

import six.moves.cPickle as pickle  # @UnresolvedImport

from . import utils, droputils
from .apps import pyfunc
from .drop import dropdict, BarrierAppDROP
from .exceptions import InvalidDropException
from dlg.ddap_protocol import DROPStates


logger = logging.getLogger(__name__)

class ResultTransmitter(BarrierAppDROP):
    '''Collects data from all inputs and transmits it to whomever connects to
    the given host/port'''

    def initialize(self, **kwargs):
        BarrierAppDROP.initialize(self, input_error_threshold=100, **kwargs)
        self.host = self._getArg(kwargs, 'host', '127.0.0.1')
        self.port = self._getArg(kwargs, 'port', None)
        if self.port is None:
            raise InvalidDropException(self, "Missing port parameter")

    def run(self):

        def read_result(x):
            if x.status == DROPStates.ERROR:
                return 'Error'
            return pickle.loads(droputils.allDropContents(x))

        results = map(read_result, self.inputs)  # @UndefinedVariable
        results = list(results)
        if len(self.inputs) == 1:
            results = results[0]
        results = pickle.dumps(results)

        s = socket.socket()
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
        s.bind((self.host, self.port))
        s.listen(1)
        client, _ = s.accept()
        with contextlib.closing(client):
            client = client.makefile("wb")
            client.write(struct.pack('>i', len(results)))
            client.write(results)


def _get_client(**kwargs):

    if 'client' in kwargs:
        return kwargs['client']

    from .manager.client import NodeManagerClient
    from .manager import constants
    host = kwargs.get('host', '127.0.0.1')
    port = kwargs.get('port', constants.NODE_DEFAULT_REST_PORT)
    timeout = kwargs.get('timeout', None)
    return NodeManagerClient(host, port, timeout)

class _DelayedDrop(object):

    _drop_count = 0

    def __init__(self, producer=None):
        self._dropdict = None
        self.producer = producer
        self.inputs = []

    @property
    def next_drop_oid(self):
        i = _DelayedDrop._drop_count
        _DelayedDrop._drop_count += 1
        return i

    @property
    def dropdict(self):
        if self._dropdict is None:
            self._dropdict = self.make_dropdict()
        return self._dropdict

    def reset(self):
        self._dropdict = None

    @property
    def oid(self):
        return self.dropdict['oid']

    def compute(self, **kwargs):
        """Returns the result of the (possibly) delayed computation by sending
        the graph to a Drop Manager and waiting for the result to arrive back"""

        graph = self.get_graph()
        port = 10000
        # Add one final application that will wait for all results
        # and transmit them back to us
        transmitter_oid = str(self.next_drop_oid)
        transmitter = dropdict({'type': 'app', 'app': 'dlg.dask_emulation.ResultTransmitter', 'oid': transmitter_oid, 'port': port})
        for leaf_oid in droputils.get_leaves(graph.values()):
            graph[leaf_oid].addConsumer(transmitter)
        graph[transmitter_oid] = transmitter

        graph = list(graph.values())

        # Submit and wait
        session_id = 'session-%f' % time.time()
        client = _get_client(**kwargs)
        client.create_session(session_id)
        client.append_graph(session_id, graph)
        client.deploy_session(session_id, completed_uids=droputils.get_roots(graph))

        s = utils.connect_to('localhost', port, 300)
        s.settimeout(kwargs.get('timeout', None))
        with contextlib.closing(s):
            s = s.makefile("rb")
            nbytes = struct.unpack('>i', s.read(4))[0]
            ret = pickle.loads(s.read(nbytes))
            logger.info("Received %r from graph computation", ret)
            return ret

    def get_graph(self):
        _DelayedDrop._drop_count = 0
        graph = {}
        visited = set()
        self._to_physical_graph(visited, graph)
        for d in visited:
            d.reset()
        return graph

    def _append_to_graph(self, visited, graph):
        oid = str(self.next_drop_oid)
        dd = self.dropdict
        dd['oid'] = oid
        visited.add(self)
        graph[oid] = dd
        logger.debug("Appended %r/%s to the Physical Graph", self, oid)

    def _to_physical_graph(self, visited, graph):

        self._append_to_graph(visited, graph)

        dependencies = list(self.inputs)
        if self.producer:
            dependencies.append(self.producer)
        for d in dependencies:
            if isinstance(d, list):
                d = tuple(d)
            if d in visited:
                self._add_upstream(d)
                continue

            d = d._to_physical_graph(visited, graph)
            self._add_upstream(d)

        return self

    def _add_upstream(self, upstream):
        """Link the given drop as either a producer or input of this drop"""
        self_dd = self.dropdict
        up_dd = upstream.dropdict
        if isinstance(self, _DataDrop):
            self_dd.addProducer(up_dd)
            logger.debug("Set %r/%s as producer of %r/%s", upstream, upstream.oid, self, self.oid)
        else:
            self_dd.addInput(up_dd)
            logger.debug("Set %r/%s as input of %r/%s", upstream, upstream.oid, self, self.oid)


class _Listifier(BarrierAppDROP):
    """Returns a list with all objects as contents"""
    def run(self):
        self.outputs[0].write(pickle.dumps([pickle.loads(droputils.allDropContents(x)) for x in self.inputs]))

class _DelayedDrops(_DelayedDrop):
    """One or more _DelayedDrops treated as a single item"""

    def __init__(self, *drops):
        super(_DelayedDrops, self).__init__()
        self.drops = drops
        self.inputs.extend(drops)

    def _to_physical_graph(self, visited, graph):

        output = _DataDrop(producer=self)
        output._append_to_graph(visited, graph)

        self._append_to_graph(visited, graph)
        output._add_upstream(self)

        for d in self.drops:
            d._to_physical_graph(visited, graph)
            self._add_upstream(d)

        return output

    def __iter__(self):
        return iter(self.drops)

    def __len__(self):
        return len(self.drops)

    def __getitem__(self, i):
        return self.drops[i]

    def make_dropdict(self):
        return dropdict({'type': 'app', 'app': 'dlg.dask_emulation._Listifier'})

    def __repr__(self):
        return "<_DelayedDrops n=%d>" % (len(self.drops),)

class _AppDrop(_DelayedDrop):
    """Defines a PyFuncApp drop for a given function `f`"""

    def __init__(self, f, nout):
        _DelayedDrop.__init__(self)
        self.f = f
        self.fname = None
        if hasattr(f, '__name__'):
            self.fname = f.__name__
        self.fcode, self.fdefaults = pyfunc.serialize_func(f)
        self.original_kwarg_names = []
        self.nout = nout

    def make_dropdict(self):

        self.kwarg_names = list(self.original_kwarg_names)
        self.kwarg_names.reverse()
        my_dropdict = dropdict({'type': 'app', 'app': 'dlg.apps.pyfunc.PyFuncApp', 'func_arg_mapping': {}})
        if self.fname is not None:
            simple_fname = self.fname.split('.')[-1]
            my_dropdict['func_name'] = self.fname
            my_dropdict['nm'] = simple_fname
        if self.fcode is not None:
            my_dropdict['func_code'] = utils.b2s(base64.b64encode(self.fcode))
        if self.fdefaults:
            my_dropdict['func_defaults'] = self.fdefaults
        return my_dropdict

    def _add_upstream(self, dep):
        _DelayedDrop._add_upstream(self, dep)
        if self.kwarg_names:
            name = self.kwarg_names.pop()
            if name is not None:
                logger.debug("Adding %s/%s to function mapping for %s", name, dep.oid, self.fname)
                self.dropdict['func_arg_mapping'][name] = dep.oid

    def _to_delayed_arg(self, arg):

        if isinstance(arg, _DelayedDrop):
            return arg

        if arg is None:
            return None

        # Turn lists/tuples of _DataDrop objects into a _DelayedDrops
        if isinstance(arg, (list, tuple)) and len(arg) > 0 and isinstance(arg[0], _DataDrop):
            return _DelayedDrops(*arg)

        # Plain data gets turned into a _DataDrop
        return _DataDrop(pydata=arg)

    def __call__(self, *args, **kwargs):

        logger.debug("Delayed function %s called with %d args and %d kwargs", self.fname, len(args), len(kwargs))
        for arg in args:
            self.inputs.append(self._to_delayed_arg(arg))
            self.original_kwarg_names.append(None)

        for name, arg in kwargs.items():
            logger.debug("Adding named argument %s=%r to call for %s", name, arg, self.fname)
            self.inputs.append(self._to_delayed_arg(arg))
            self.original_kwarg_names.append(name)

        if self.nout == 1:
            return _DataDrop(producer=self)

        outputs = [_DataDrop(producer=self) for _ in range(self.nout)]
        return _DelayedDrops(*outputs)

    def __repr__(self):
        return "<_DelayedApp fname=%s>" % (self.fname)

class _DataDrop(_DelayedDrop):
    """Defines an in-memory drop"""

    def __init__(self, producer=None, pydata=None):
        _DelayedDrop.__init__(self, producer)

        if bool(producer is None) == bool(pydata is None):
            raise ValueError("either producer or pydata must be not None")
        self.pydata = pydata

    def make_dropdict(self):
        my_dropdict = dropdict({'type': 'plain', 'storage': 'memory'})
        if not self.producer:
            my_dropdict['pydata'] = utils.b2s(base64.b64encode(pickle.dumps(self.pydata)))
        return my_dropdict

    def __repr__(self):
        if not self.producer:
            return "<_DelayedDataDrop, pydata=%r>" % self.pydata
        return "<_DelayedDataDrop>"

def delayed(x, *args, **kwargs):
    """Like dask.delayed, but quietly swallowing anything other than `nout`"""
    if 'nout' in kwargs:
        nout = kwargs['nout']
    elif args:
        nout = args[0]
    else:
        nout = 1
    if callable(x):
        return _AppDrop(x, nout=nout)
    return _DataDrop(pydata=x)