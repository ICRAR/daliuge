#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2026
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

import json
import tempfile
import unittest
from copy import deepcopy
from unittest.mock import MagicMock, patch

from dlg import common
from dlg.common import path_utils
from dlg.translator.web import translator_rest
from dlg.translator.web.pg_manager import PGManager
from dlg.translator.web.translator_utils import (
    prepare_lgt,
    unroll_and_partition_with_params,
)

DROPS = [
    {
        "oid": "A",
        "categoryType": "Data",
        "consumers": ["B"],
        "node": "#0",
        "island": "#0",
    },
    {
        "oid": "B",
        "categoryType": "Application",
        "inputs": ["A"],
        "outputs": ["C"],
        "node": "#0",
        "island": "#0",
    },
    {
        "oid": "C",
        "categoryType": "Data",
        "producers": ["B"],
        "node": "#0",
        "island": "#0",
    },
]
REPRODATA = {"rmode": "1", "meta_data": {"repro_protocol": 0.1}}


def _pgtp():
    """
    Fake PGT(P): a fresh drop list per call, as func to_pg_spec builds one.
    """
    pgtp = MagicMock()
    pgtp.gojs_json_obj = {
        "nodeDataArray": [{"key": -1, "isGroup": True}, {"key": -2, "isGroup": True}]
    }
    pgtp.reprodata = REPRODATA
    pgtp.to_pg_spec.side_effect = lambda *a, **kw: [dict(d) for d in DROPS]
    return pgtp


def _gen_pg(**kwargs):
    params = {
        "pgt_id": "graph_pgt.graph",
        "dlg_mgr_deploy": None,
        "dlg_mgr_url": None,
        "dlg_mgr_host": None,
        "dlg_mgr_port": None,
        "tpl_nodes_len": 0,
    }
    params.update(kwargs)
    return translator_rest.gen_pg(**params)


class TestGenPgReprodata(unittest.TestCase):
    """
    Pin the reprodata trailing element on the original `/gen_pg` route.
    """

    def setUp(self):
        self.pgtp = _pgtp()
        pg_mgr = MagicMock()
        pg_mgr.get_pgt.return_value = self.pgtp
        patcher = patch.object(translator_rest, "pg_mgr", pg_mgr, create=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_template_appends_reprodata(self):
        response = _gen_pg(tpl_nodes_len=4)

        self.assertEqual(json.loads(response.body), DROPS + [REPRODATA])
        self.pgtp.to_pg_spec.assert_called_once_with([], ret_str=False, tpl_nodes_len=2)

    @patch.object(translator_rest, "CompositeManagerClient")
    def test_deploy_appends_reprodata(self, client_cls):
        """
        graph sent to manager ends with reprodata, and 
        roots passed to deploy_session are the graph's real roots.

        The order of get_roots and append is not pinned: 
        get_roots skips elements without an `oid`, so it returns the same set either way.
        """
        client = client_cls.return_value
        client.nodes.return_value = ["node1"]

        _gen_pg(dlg_mgr_host="mgr", dlg_mgr_port=8001, dlg_mgr_deploy="deploy")

        ssid, graph = client.append_graph.call_args.args
        self.assertEqual(graph, DROPS + [REPRODATA])
        client.create_session.assert_called_once_with(ssid)
        client.deploy_session.assert_called_once_with(ssid, completed_uids={"A"})

    @patch.object(translator_rest, "CompositeManagerClient")
    def test_no_deploy_returns_drops_only(self, client_cls):
        client_cls.return_value.nodes.return_value = ["node1"]

        response = _gen_pg(dlg_mgr_host="mgr", dlg_mgr_port=8001)

        self.assertEqual(json.loads(response.body), DROPS)
        client_cls.return_value.append_graph.assert_not_called()


class TestGenPgReprodataRealGraph(unittest.TestCase):
    """
    The same contract on real PGT(P)s, built the way `GET /gen_pgt` builds and
    registers them, so the drops are real `to_pg_spec` output.

    `mysarkar` is used because it does not need the METIS library.
    """

    GRAPHS = ["testLoop.graph", "chiles_simple.graph", "test_grpby_gather.graph"]

    def _register(self, lg_name):
        lgt = prepare_lgt(path_utils.get_lg_fpath("logical_graphs", lg_name), "1")
        pgtp = unroll_and_partition_with_params(lgt, False, "mysarkar", 1, 0)
        pgt_dir = tempfile.TemporaryDirectory()
        self.addCleanup(pgt_dir.cleanup)
        pg_mgr = PGManager(pgt_dir.name)
        pgt_id = pg_mgr.add_pgt(pgtp, lg_name)
        patcher = patch.object(translator_rest, "pg_mgr", pg_mgr, create=True)
        patcher.start()
        self.addCleanup(patcher.stop)

        # keep a copy of what to_pg_spec returned, before the route uses it
        specs = []
        to_pg_spec = pgtp.to_pg_spec

        def spy(*args, **kwargs):
            spec = to_pg_spec(*args, **kwargs)
            specs.append(deepcopy(spec))
            return spec

        pgtp.to_pg_spec = spy
        self.assertTrue(pgtp.reprodata)
        return pgtp, pgt_id, specs

    def test_template_appends_reprodata(self):
        for lg_name in self.GRAPHS:
            with self.subTest(lg_name):
                pgtp, pgt_id, specs = self._register(lg_name)

                response = _gen_pg(pgt_id=pgt_id, tpl_nodes_len=1)

                expected = json.loads(json.dumps(specs[0] + [pgtp.reprodata]))
                self.assertEqual(json.loads(response.body), expected)

    @patch.object(translator_rest, "CompositeManagerClient")
    def test_deploy_appends_reprodata(self, client_cls):
        client = client_cls.return_value
        client.nodes.return_value = ["node1"]
        for lg_name in self.GRAPHS:
            with self.subTest(lg_name):
                client.reset_mock()
                pgtp, pgt_id, specs = self._register(lg_name)

                _gen_pg(
                    pgt_id=pgt_id,
                    dlg_mgr_host="mgr",
                    dlg_mgr_port=8001,
                    dlg_mgr_deploy="deploy",
                )

                ssid, graph = client.append_graph.call_args.args
                self.assertEqual(graph, specs[0] + [pgtp.reprodata])
                client.deploy_session.assert_called_once_with(
                    ssid, completed_uids=common.get_roots(specs[0])
                )
