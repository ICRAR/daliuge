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

from __future__ import annotations

import contextlib
import dill
import base64
import logging
import pickle
import socket
import struct
import time

from inspect import signature

from dlg import utils, droputils
from dlg.apps import pyfunc
from dlg.common import dropdict
from dlg.ddap_protocol import DROPStates
from dlg.apps.app_base import BarrierAppDROP
from dlg.exceptions import InvalidDropException

logger = logging.getLogger(f"dlg.{__name__}")


class ResultTransmitter(BarrierAppDROP):
    """Collects data from all inputs and transmits it to whomever connects to
    the given host/port"""

    def initialize(self, **kwargs):
        BarrierAppDROP.initialize(self, input_error_threshold=100, **kwargs)
        self.host = self._popArg(kwargs, "host", "localhost")
        self.port = self._popArg(kwargs, "port", None)
        if self.port is None:
            raise InvalidDropException(self, "Missing port parameter")

    def run(self):
        def read_result(x):
            if x.status == DROPStates.ERROR:
                return "Error"
            try:
                content = pickle.loads(droputils.allDropContents(x))
            except EOFError:
                content = None
            return content

        results = map(read_result, self.inputs)  # @UndefinedVariable
        results = list(results)
        if len(self.inputs) == 1:
            results = results[0]
        results = pickle.dumps(results)

        s = socket.socket()
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
        logger.debug(
            "Attempting to bind ResultTransmitter to %s:%i", self.host, self.port
        )
        s.bind((self.host, self.port))
        s.listen(1)
        client, _ = s.accept()
        with contextlib.closing(client):
            client = client.makefile("wb")
            client.write(struct.pack(">i", len(results)))
            client.write(results)


def _get_client(**kwargs):
    if "client" in kwargs:
        return kwargs["client"]

    from .manager.client import NodeManagerClient
    from dlg import constants

    host = kwargs.get("host", "localhost")
    port = kwargs.get("port", constants.NODE_DEFAULT_REST_PORT)
    timeout = kwargs.get("timeout", None)
    return NodeManagerClient(host, port, timeout)


def _is_list_of_delayeds(x):
    return isinstance(x, (list, tuple)) and len(x) > 0 and isinstance(x[0], _DataDrop)


def compute(value, **kwargs):
    """Returns the result of the (possibly) delayed computation by sending
    the graph to a Drop Manager and waiting for the result to arrive back"""

    # Support calling compute with a list of DelayedDrops
    if _is_list_of_delayeds(value):
        value = _DelayedDrops(*value)

    graph = value.get_graph()
    port = 10001
    # Add one final application that will wait for all results
    # and transmit them back to us
    transmitter_oid = "-1"
    transmitter = dropdict(
        {
            "categoryType": "Application",
            #            "categoryType": CategoryType.APPLICATION,
            # "Application": "dlg.dask_emulation.ResultTransmitter",
            "dropclass": "dlg.dask_emulation.ResultTransmitter",
            "oid": transmitter_oid,
            "uid": transmitter_oid,
            "port": port,
            "name": "result transmitter",
        }
    )
    for leaf_oid in droputils.get_leaves(graph.values()):
        graph[leaf_oid].addConsumer(transmitter)
    graph[transmitter_oid] = transmitter

    graph = list(graph.values())

    # Submit and wait
    session_id = "session-%f" % time.time()
    client = _get_client(**kwargs)
    client.create_session(session_id)
    client.append_graph(session_id, graph)
    client.deploy_session(session_id, completed_uids=droputils.get_roots(graph))

    timeout = kwargs.get("timeout", None)
    s = utils.connect_to("localhost", port, 10)
    s.settimeout(timeout)
    with contextlib.closing(s):
        s = s.makefile("rb")
        nbytes = struct.unpack(">i", s.read(4))[0]
        ret = pickle.loads(s.read(nbytes))
        logger.info("Received %r from graph computation", ret)
        return ret


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
        return self.dropdict["oid"]

    def compute(self, **kwargs):
        return compute(self, **kwargs)

    def get_graph(self):
        _DelayedDrop._drop_count = 0
        graph = {}
        visited = set()
        self.to_physical_graph(visited, graph)
        for d in visited:
            d.reset()
        return graph

    def append_to_graph(self, visited, graph):
        if self in visited:
            return
        oid = str(self.next_drop_oid)
        dd = self.dropdict
        dd["oid"] = oid
        dd["inputs"] = []

        visited.add(self)
        graph[oid] = dd
        logger.debug("Appended %r/%s to the Physical Graph", self, oid)

    def to_physical_graph(self, visited, graph):
        self.append_to_graph(visited, graph)
        dependencies = self.inputs
        if self.producer:
            dependencies.append(self.producer)
        for d in dependencies:
            if isinstance(d, list):
                d = tuple(d)
            if d in visited:
                self.add_upstream(d)
                continue

            d = d.to_physical_graph(visited, graph)
            self.add_upstream(d)

        return self

    def add_upstream(self, upstream: _DelayedDrop):
        """Link the given drop as either a producer or input of this drop"""
        self_dd = self.dropdict
        up_dd = upstream.dropdict
        if isinstance(self, _DataDrop):
            self_dd.addProducer(up_dd)
            logger.debug(
                "Set %r/%s as producer of %r/%s",
                upstream,
                upstream.oid,
                self,
                self.oid,
            )
        else:
            logger.debug(
                "Set %r/%s as input of %r/%s",
                upstream,
                upstream.oid,
                self,
                self.oid,
            )


class _Listifier(BarrierAppDROP):
    """Returns a list with all objects as contents"""

    def run(self):
        logger.debug("Listifier running...")
        self.outputs[0].write(
            pickle.dumps(
                [pickle.loads(droputils.allDropContents(x)) for x in self.inputs]
            )
        )


class _DelayedDrops(_DelayedDrop):
    """One or more _DelayedDrops treated as a single item"""

    def __init__(self, *drops):
        super(_DelayedDrops, self).__init__()
        self.drops = drops
        self.inputs.extend(drops)
        logger.debug("Created %r", self)

    def to_physical_graph(self, visited, graph):
        output = _DataDrop(producer=self)
        output.append_to_graph(visited, graph)

        self.append_to_graph(visited, graph)
        output.add_upstream(self)

        for d in self.drops:
            d.to_physical_graph(visited, graph)
            self.add_upstream(d)

        return output

    def __iter__(self):
        return iter(self.drops)

    def __len__(self):
        return len(self.drops)

    def __getitem__(self, i):
        return self.drops[i]

    def add_upstream(self, upstream: _DelayedDrop):
        _DelayedDrop.add_upstream(self, upstream)
        self.dropdict["inputs"].append(upstream.oid)

    def make_dropdict(self):
        return dropdict(
            {
                "categoryType": "Application",
                "dropclass": "dlg.dask_emulation._Listifier",
                "name": "listifier",
                "inputs": self.inputs,
            }
        )

    def __repr__(self):
        return "<_DelayedDrops n=%d>" % (len(self.drops),)


class _AppDrop(_DelayedDrop):
    """Defines a PyFuncApp drop for a given function `f`"""

    def __init__(self, f, nout):
        _DelayedDrop.__init__(self)
        self.f = f
        self.fname = None
        if hasattr(f, "__name__"):
            self.fname = f.__name__
        self.fcode, self.fdefaults = pyfunc.serialize_func(f)
        # self.fcode = inspect.getsource(f)
        self.original_kwarg_names = []
        self.original_arg_names = []
        self.nout = nout
        logger.debug("Created %r", self)

    def make_dropdict(self):
        self.kwarg_names = list(self.original_kwarg_names)
        self.arg_names = list(self.original_arg_names)
        self.kwarg_names.reverse()
        self.arg_names.reverse()
        my_dropdict = dropdict(
            {
                # "oid": uuid.uuid1(),
                "categoryType": "Application",
                "dropclass": "dlg.apps.pyfunc.PyFuncApp",
                "func_arg_mapping": {},
                "inputs": self.inputs,
            }
        )
        if self.fname is not None:
            simple_fname = self.fname.split(".")[-1]
            my_dropdict["func_name"] = self.fname
            my_dropdict["name"] = simple_fname
        if self.fcode is not None:
            my_dropdict["func_code"] = self.fcode
            logger.debug("func_code provided: %s", dill.loads(base64.b64decode(self.fcode)))
        if self.fdefaults:
            # APPLICATION ARGUMENTS
            my_dropdict["func_defaults"] = self.fdefaults
        return my_dropdict

    def add_upstream(self, upstream):
        _DelayedDrop.add_upstream(self, upstream)
        if self.kwarg_names:
            name = self.kwarg_names.pop()
            if name is not None:
                logger.debug(
                    "Adding %s/%s to function mapping for %s",
                    name,
                    upstream.oid,
                    self.fname,
                )
                self.dropdict["inputs"].append({upstream.oid: name})
                self.dropdict["func_arg_mapping"][name] = upstream.oid
        if self.arg_names:
            name = self.arg_names.pop()
            if name is not None:
                logger.debug(
                    "Adding %s/%s to function mapping for %s",
                    name,
                    upstream.oid,
                    self.fname,
                )
                self.dropdict["inputs"].append({upstream.oid: name})
                self.dropdict["func_arg_mapping"][name] = upstream.oid

    def _to_delayed_arg(self, arg):
        logger.info("Turning into delayed arg for %r: %r", self, arg)
        if isinstance(arg, _DelayedDrop):
            return arg

        # Turn lists/tuples of _DataDrop objects into a _DelayedDrops
        if _is_list_of_delayeds(arg):
            return _DelayedDrops(*arg)

        # Plain data gets turned into a _DataDrop
        return _DataDrop(pydata=arg)

    def __call__(self, *args, **kwargs):
        logger.debug(
            "Delayed function %s called with %d args and %d kwargs",
            self.fname,
            len(args),
            len(kwargs),
        )

        for name, arg in kwargs.items():
            self.inputs.append(self._to_delayed_arg(arg))
            self.original_kwarg_names.append(name)

        leftover_args = [
            x
            for x in list(signature(self.f).parameters.keys())
            if x not in list(kwargs.keys())
        ]

        iter_range = min(len(args), len(leftover_args))
        for i in range(iter_range):
            self.inputs.append(self._to_delayed_arg(args[i]))  # , leftover_args[i]))
            self.original_arg_names.append(leftover_args[i])

        if self.nout is None:
            return _DataDrop(producer=self)

        return _DataDropSequence(nout=self.nout, producer=self)

    def __repr__(self):
        return "<_DelayedApp fname=%s, nout=%s>" % (self.fname, str(self.nout))


_no_data = object()


class _DataDrop(_DelayedDrop):
    """Defines an in-memory drop"""

    def __init__(self, producer=None, pydata=_no_data):
        _DelayedDrop.__init__(self, producer)

        if bool(producer is None) == bool(pydata is _no_data):
            raise ValueError("either producer or pydata must be not None")
        self.pydata = pydata
        logger.debug("Created %r", self)

    def make_dropdict(self):
        my_dropdict = dropdict(
            {
                # "oid": uuid.uuid1(),
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
            }
        )
        if not self.producer:
            my_dropdict["pydata"] = pyfunc.serialize_data(self.pydata)
            # my_dropdict["pydata"] = self.pydata
        return my_dropdict

    def __repr__(self):
        if not self.producer:
            return "<_DataDrop, pydata=%r>" % (self.pydata,)
        return "<_DataDrop, producer=%r>" % self.producer


def getitem(x, i):
    """
    Helper function to ensure the delayed() __getitem__ gets the appropriate arguments
    in the correct order.
    """
    try:
        return x[i]
    except TypeError:
        return x


class _DataDropSequence(_DataDrop):
    """One or more _DataDrops that can be subscribed"""

    def __init__(self, nout, producer):
        super(_DataDrop, self).__init__(producer=producer)
        self.nout = nout
        logger.debug("Created %r", self)
        self._drops = []

    def __iter__(self):
        for i in range(self.nout):
            yield self[i]

    def __len__(self):
        return self.nout

    def __getitem__(self, i):
        return delayed(getitem)(x=self, i=i)

    def __repr__(self):
        return "<_DataDropSequence nout=%d, producer=%r>" % (
            self.nout,
            self.producer,
        )


def delayed(x, *args, **kwargs):
    """Like dask.delayed, but quietly swallowing anything other than `nout`"""
    if "nout" in kwargs:
        nout = kwargs["nout"]
    elif args:
        nout = args[0]
    else:
        nout = None

    if callable(x):
        return _AppDrop(x, nout=nout)
        # return x(*args, **kwargs)
    return _DataDrop(pydata=x)
