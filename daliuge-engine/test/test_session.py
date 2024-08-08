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
import json
import logging
import os
import time
import unittest
import pkg_resources
import pytest

from pathlib import Path

from dlg.runtime import version  # Imported to setup DlgLogger

from dlg.apps.app_base import BarrierAppDROP
from dlg.ddap_protocol import DROPLinkType, DROPStates, AppDROPStates
from dlg.droputils import DROPWaiterCtx
from dlg.exceptions import InvalidGraphException
from dlg.manager.session import SessionStates, Session, generateLogFileName

default_repro = {
    "rmode": "1",
    "RERUN": {
        "lg_blockhash": "x",
        "pgt_blockhash": "y",
        "pg_blockhash": "z",
    },
}
default_graph_repro = {
    "rmode": "1",
    "meta_data": {"repro_protocol": 0.1, "hashing_alg": "_sha3.sha3_256"},
    "merkleroot": "a",
    "RERUN": {
        "signature": "b",
    },
}


class MockThrowingDrop(BarrierAppDROP):
    def run(self):
        raise RuntimeError("App drop thrown")


def add_test_reprodata(graph: list):
    for drop in graph:
        drop["reprodata"] = default_repro.copy()
    graph.append(default_graph_repro.copy())
    return graph


def test_logs(caplog):
    """
    Confirm that when we run a session in which the AppDrop experiences a runtime error,
    we produce the trackback for that application in the file.

    This acts as a regression test to make sure changes in the future don't lead to
    app/data drop tracking no longer adding appropriate attributes to the LogRecords such
    that they pass the filter setup in the Session constructor. For further information,
    review the runtime/__init__.py file.

    The test uses the pytest.caplog fixture to first confirm that:
    1. An exception is logged in the DlgLogger, and
    2. That the exception passes through the filters setup in the Session constructor, and
        properly described in the session log file.

    This aims to detect regressions when re-organising class structures or logging in the
    future.
    """
    tmp_root = os.environ["DLG_ROOT"]
    os.environ["DLG_ROOT"] = str(Path(__file__).cwd())
    with caplog.at_level(logging.INFO):
        with Session("1") as s:
            # caplog.handler.addFilter(SessionFilter("1"))
            s.addGraphSpec(
                add_test_reprodata(
                    [
                        {
                            "oid": "A",
                            "categoryType": "Data",
                            "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                            "consumers": ["B"],
                        },
                        {
                            "oid": "B",
                            "categoryType": "Application",
                            "dropclass": "test.test_session.MockThrowingDrop",
                            "sleep_time": 2,
                        },
                        {
                            "oid": "C",
                            "categoryType": "Data",
                            "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                            "producers": ["B"],
                        },
                    ]
                )
            )

            s.deploy()
            with DROPWaiterCtx(None, s.drops["C"], 1):
                s.drops["A"].write(b"x")
                s.drops["A"].setCompleted()

            # Logger needs time to get messages.
            time.sleep(5)
            logfile = Path(generateLogFileName(s._sessionDir, s.sessionId))
            exception_logged = False
            for record in caplog.records:
                if record.name == 'dlg.apps.app_base' and record.levelname == 'ERROR':
                    exception_logged = True
                    with logfile.open('r') as f:
                        buffer = f.read()
                        assert record.name in buffer
                        assert 'Traceback' in buffer
                        assert 'App drop thrown' in buffer
            assert exception_logged
            logfile.unlink(missing_ok=True)
    os.environ["DLG_ROOT"] = tmp_root


class TestSession(unittest.TestCase):
    def test_sessionStates(self):
        with Session("1") as s:
            self.assertEqual(SessionStates.PRISTINE, s.status)
            self.assertRaises(Exception, s.linkGraphParts, "", "", 0)
            s.addGraphSpec(
                add_test_reprodata([{"oid": "A", "categoryType": "container"}])
            )
            self.assertEqual(SessionStates.BUILDING, s.status)

            s.deploy()
            self.assertEqual(SessionStates.RUNNING, s.status)

            # Now we can't do any of these
            self.assertRaises(Exception, s.deploy)
            self.assertRaises(Exception, s.addGraphSpec, "")
            self.assertRaises(Exception, s.linkGraphParts, "", "", 0)

    def test_sessionStates_noDrops(self):
        # No drops created, we can deploy right away
        with Session("1") as s:
            self.assertEqual(SessionStates.PRISTINE, s.status)
            s.deploy()
            self.assertEqual(SessionStates.FINISHED, s.status)

        with Session("2") as s:
            self.assertRaises(InvalidGraphException, s.deploy, completedDrops=["a"])

    def test_addGraphSpec(self):
        with Session("1") as s:
            s.addGraphSpec(
                add_test_reprodata([{"oid": "A", "categoryType": "container"}])
            )
            s.addGraphSpec(
                add_test_reprodata([{"oid": "B", "categoryType": "container"}])
            )
            s.addGraphSpec(
                add_test_reprodata([{"oid": "C", "categoryType": "container"}])
            )

            # Adding an existing DROP
            self.assertRaises(
                Exception,
                s.addGraphSpec,
                add_test_reprodata([{"oid": "A", "categoryType": "container"}]),
            )

            # Adding invalid specs
            self.assertRaises(
                Exception,
                s.addGraphSpec,
                add_test_reprodata([{"oid": "D", "categoryType": "Application"}]),
            )  # missing "storage"
            self.assertRaises(
                Exception,
                s.addGraphSpec,
                add_test_reprodata(
                    [
                        {
                            "oid": "D",
                            "categoryType": "Data",
                            "dropclass": "invalid",
                        }
                    ]
                ),
            )  # invalid "storage"
            self.assertRaises(
                Exception,
                s.addGraphSpec,
                add_test_reprodata([{"oid": "D", "categoryType": "invalid"}]),
            )  # invalid "categoryType"
            # s.addGraphSpec(
            #     add_test_reprodata(
            #         [
            #             {
            #                 "oid": "D",
            #                 "categoryType": "Application",
            #                 "dropclass": "dlg.data.drops.NullDROP",
            #                 "outputs": ["X"],
            #             }
            #         ]
            #     ),
            # )
            self.assertRaises(
                Exception,
                s.addGraphSpec,
                add_test_reprodata(
                    [
                        {
                            "oid": "D",
                            "categoryType": "Application",
                            "dropclass": "dlg.data.drops.NullDROP",
                            "outputs": ["X"],
                        }
                    ]
                ),
            )  # missing X DROP

    def test_addGraphSpec_namedPorts(self):
        with pkg_resources.resource_stream(
                "test", "graphs/funcTestPG_namedPorts.graph"
        ) as f:  # @UndefinedVariable
            graphSpec = json.load(f)
        # dropSpecs = graph_loader.loadDropSpecs(graphSpec)
        with Session("1") as s:
            s.addGraphSpec(graphSpec)
            s.deploy()

    def test_linking(self):
        with Session("1") as s:
            s.addGraphSpec(
                add_test_reprodata([{"oid": "A", "categoryType": "container"}])
            )
            s.addGraphSpec(
                add_test_reprodata(
                    [
                        {
                            "oid": "B",
                            "categoryType": "Application",
                            # "dropclass": "dlg.data.drops.data_base.NullDROP",
                            "dropclass": "dlg.apps.crc.CRCApp",
                        }
                    ]
                )
            )
            s.addGraphSpec(
                add_test_reprodata([{"oid": "C", "categoryType": "container"}])
            )

            # Link them now
            s.linkGraphParts("A", "B", DROPLinkType.CONSUMER)
            s.linkGraphParts("B", "C", DROPLinkType.OUTPUT)

            # Deploy and check that the actual DROPs are linked together
            s.deploy()
            roots = s.roots
            self.assertEqual(1, len(roots))
            a = s.roots[0]
            self.assertEqual("A", a.oid)
            self.assertEqual(1, len(a.consumers))
            b = a.consumers[0]
            self.assertEqual("B", b.oid)
            self.assertEqual(1, len(b.outputs))
            c = b.outputs[0]
            self.assertEqual("C", c.oid)

    def test_cancel(self):
        """Cancels a whole graph execution"""
        with Session("1") as s:
            s.addGraphSpec(
                add_test_reprodata(
                    [
                        {
                            "oid": "A",
                            "categoryType": "Data",
                            "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                            "consumers": ["B"],
                        },
                        {
                            "oid": "B",
                            "categoryType": "Application",
                            "dropclass": "dlg.apps.simple.SleepApp",
                            "sleep_time": 2,
                        },
                        {
                            "oid": "C",
                            "categoryType": "Data",
                            "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                            "producers": ["B"],
                        },
                    ]
                )
            )
            s.deploy()
            self.assertEqual(SessionStates.RUNNING, s.status)
            s.cancel()
            self.assertEqual(SessionStates.CANCELLED, s.status)
            for uid in "ABC":
                self.assertEqual(DROPStates.CANCELLED, s.drops[uid].status)
            self.assertEqual(AppDROPStates.CANCELLED, s.drops["B"].execStatus)

    def test_partial_cancel(self):
        """Like test_cancel, but only part of the graph should be cancelled"""
        with Session("1") as s:
            s.addGraphSpec(
                add_test_reprodata(
                    [
                        {
                            "oid": "A",
                            "categoryType": "Data",
                            "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                            "consumers": ["B"],
                        },
                        {
                            "oid": "B",
                            "categoryType": "Application",
                            "dropclass": "dlg.apps.simple.SleepApp",
                            "sleep_time": 0,
                        },
                        {
                            "oid": "C",
                            "categoryType": "Data",
                            "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                            "producers": ["B"],
                            "consumers": ["D"],
                        },
                        {
                            "oid": "D",
                            "categoryType": "Application",
                            "dropclass": "dlg.apps.simple.SleepApp",
                            "sleep_time": 10,
                        },
                        {
                            "oid": "E",
                            "categoryType": "Data",
                            "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                            "producers": ["D"],
                        },
                    ]
                )
            )
            s.deploy()
            self.assertEqual(SessionStates.RUNNING, s.status)

            # Move first three drops to completed, D should take longer to execute
            with DROPWaiterCtx(self, s.drops["C"], 1):
                s.drops["A"].write(b"x")
                s.drops["A"].setCompleted()
            # Cancel the session, A, B and C should remain COMPLETED
            s.cancel()
            self.assertEqual(SessionStates.CANCELLED, s.status)
            for uid in "ABC":
                self.assertEqual(DROPStates.COMPLETED, s.drops[uid].status)
            for uid in "DE":
                self.assertEqual(DROPStates.CANCELLED, s.drops[uid].status)
