#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2015, 2024
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


import codecs
import dataclasses
import http.client
import json
import random

from time import sleep

from dlg import utils
from dlg import droputils
from dlg.data.drops import DataDROP
from dlg.ddap_protocol import DROPStates
from dlg.manager.node_manager import NodeManager
from dlg.manager.manager_data import Node

from test.dlg_engine_testconstants import DEFAULT_TEST_REPRO, DEFAULT_TEST_GRAPH_REPRO


class RESTTestUtils:

    @staticmethod
    def _get(url, port):
        conn = http.client.HTTPConnection("localhost", port, timeout=3)
        conn.request("GET", "/api" + url)
        return conn.getresponse(), conn


    @staticmethod
    def get(test, url, port):
        res, conn = RESTTestUtils._get(url, port)
        test.assertEqual(http.HTTPStatus.OK, res.status)
        jsonRes = json.load(codecs.getreader("utf-8")(res))
        res.close()
        conn.close()
        return jsonRes


    @staticmethod
    def post(test, url, port, content=None, mimeType=None):
        conn = http.client.HTTPConnection("localhost", port, timeout=3)
        headers = {mimeType or "Content-Type": "application/json"} if content else {}
        conn.request("POST", "/api" + url, content, headers)
        res = conn.getresponse()
        test.assertEqual(http.HTTPStatus.OK, res.status)
        conn.close()


    @staticmethod
    def delete(test, url, port):
        conn = http.client.HTTPConnection("localhost", port, timeout=3)
        conn.request("DELETE", "/api" + url)
        res = conn.getresponse()
        test.assertEqual(http.HTTPStatus.OK, res.status)
        conn.close()


class TerminatingTestHelper(object):
    """
    A context manager that makes sure a process always exits.
    """

    def __init__(self, proc, timeout):
        self.proc = proc
        self.timeout = timeout

    def __enter__(self):
        return self.proc

    def __exit__(self, typ, val, traceback):
        utils.terminate_or_kill(self.proc, self.timeout)
        return False


class DROPManagerUtils:

    @staticmethod
    def nm_conninfo(n, return_tuple=False):
        if return_tuple:
            return "localhost", 5553 + n, 6666 + n
        else:
            return Node(f"localhost:8000:{5553 + n}:{6666 + n}")


    @staticmethod
    def add_test_reprodata(graph: list):
        for drop in graph:
            drop["reprodata"] = DEFAULT_TEST_REPRO.copy()
        graph.append(DEFAULT_TEST_GRAPH_REPRO.copy())
        return graph

    @staticmethod
    def quickDeploy(nm, sessionId, graphSpec, node_subscriptions: dict = None):
        if not node_subscriptions:
            node_subscriptions = {}
        nm.createSession(sessionId)
        nm.addGraphSpec(sessionId, graphSpec)
        nm.add_node_subscriptions(sessionId, node_subscriptions)
        nm.deploySession(sessionId)

class NMTestsMixIn:
    def __init__(self, *args, **kwargs):
        super(NMTestsMixIn, self).__init__(*args, **kwargs)
        self._dms = {}
        self.use_processes = False

    def _start_dm(self, threads=0, **kwargs):
        host, events_port, rpc_port = DROPManagerUtils.nm_conninfo(len(self._dms),
                                                                   return_tuple=True)

        nm = NodeManager(
            host=host,
            events_port=events_port,
            rpc_port=rpc_port,
            max_threads=threads,
            use_processes=self.use_processes,
            **kwargs,
        )
        key = f"{host}:{rpc_port}"
        self._dms[key] = nm
        return nm

    def tearDown(self):
        super(NMTestsMixIn, self).tearDown()
        for key, nm in self._dms.items():
            nm.shutdown()

    @property
    def host_names(self):
        return list(self._dms.keys())

    def start_hosts(self, num_hosts=1, threads=1):
        _ = [self._start_dm(threads=threads) for _ in range(num_hosts)]

    def _test_runGraphInTwoNMs(
        self,
        g1,
        g2,
        rels,
        root_data,
        leaf_data,
        root_oids=("A",),
        leaf_oid="C",
        expected_failures: list=None,
        sessionId=f"s{random.randint(0, 1000)}",
        node_managers=None,
        threads=0,
    ):
        """Utility to run a graph in two Node Managers"""


        dm1, dm2 = node_managers or [
            self._start_dm(threads=threads) for _ in range(2)
        ]
        DROPManagerUtils.add_test_reprodata(g1)
        DROPManagerUtils.add_test_reprodata(g2)
        DROPManagerUtils.quickDeploy(dm1, sessionId, g1, {DROPManagerUtils.nm_conninfo(1): rels})
        DROPManagerUtils.quickDeploy(dm2, sessionId, g2, {DROPManagerUtils.nm_conninfo(0): rels})
        self.assertEqual(len(g1), len(dm1.sessions[sessionId].drops))
        self.assertEqual(len(g2), len(dm2.sessions[sessionId].drops))

        # Run! We wait until c is completed
        drops = {}
        drops.update(dm1.sessions[sessionId].drops)
        drops.update(dm2.sessions[sessionId].drops)

        leaf_drop = drops[leaf_oid]
        with droputils.DROPWaiterCtx(self, leaf_drop, 5):
            for oid in root_oids:
                drop = drops[oid]
                drop.write(root_data)
                drop.setCompleted()

        if not expected_failures:
            expected_failures = []
        expected_successes = [
            drops[oid] for oid in drops if oid not in expected_failures
        ]
        expected_failures = [drops[oid] for oid in drops if oid in expected_failures]

        for drop in expected_successes:
            self.assertEqual(DROPStates.COMPLETED, drop.status)
        for drop in expected_failures:
            self.assertEqual(DROPStates.ERROR, drop.status)

        leaf_drop_data = None
        if leaf_drop not in expected_failures:
            leaf_drop_data = droputils.allDropContents(leaf_drop)
            if leaf_data is not None:
                self.assertEqual(len(leaf_data), len(leaf_drop_data))
                self.assertEqual(leaf_data, leaf_drop_data)

        sleep(0.1)  # just make sure all events have been processed.
        dm1.destroySession(sessionId)
        dm2.destroySession(sessionId)
        return leaf_drop_data


class AppArgsStore:

    ARGS = "applicationArgs"

    DEFAULT_ARGS = {
        "defaultValue": "",
        "description": "",
        "encoding": "pickle",
        "id": "8c81f45f-6f79-4033-a454-3aa4ebb21228",
        "name": "",
        "options": [],
        "parameterType": "ApplicationArgument",
        "positional": "false",
        "precious": "false",
        "readonly": "false",
        "type": "Integer",
        "usage": "NoPort",
        "value":""
    }

    def __init__(self):
        self.application_args = {self.ARGS:{}}

    def add_args(self,
        name: str = "",
        default_value: str = "",
        value: object = None,
        encoding: str = "dill",
        usage: str = "NoPort",
    ):
        args = {k: v for k, v in self.DEFAULT_ARGS.items()}
        args["name"] = name
        args["defaultValue"] = default_value
        args["value"] = value
        args["encoding"] = encoding
        args["usage"] = usage

        self.application_args[self.ARGS].update({name: args})

    def get_args(self):
        return self.application_args