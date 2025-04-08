import logging
import time
import pytest

from pathlib import Path


from dlg.runtime import version  # Imported to setup DlgLogger
from dlg.apps.app_base import BarrierAppDROP
from dlg.droputils import DROPWaiterCtx
from dlg.manager.session import Session, generateLogFileName

from test.dlg_engine_testconstants import DEFAULT_TEST_REPRO, DEFAULT_TEST_GRAPH_REPRO


class MockThrowingDrop(BarrierAppDROP):
    def run(self):
        raise RuntimeError("App drop thrown")


def add_test_reprodata(graph: list):
    for drop in graph:
        drop["reprodata"] = DEFAULT_TEST_REPRO.copy()
    graph.append(DEFAULT_TEST_GRAPH_REPRO.copy())
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

    with caplog.at_level(logging.INFO):
        with Session("log-session") as s:
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
                            "dropclass": "test.test_logs.MockThrowingDrop",
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
            with DROPWaiterCtx(None, s.drops["C"], 300):
                s.drops["A"].write(b"x")
                s.drops["A"].setCompleted()

            # Logger needs time to get messages.
            time.sleep(5)
            logfile = Path(generateLogFileName(s._sessionDir, s.sessionId))
            exception_logged = False
            for record in caplog.records:
                if (
                    record.name == "dlg.dlg.apps.app_base"
                    and record.levelname == "ERROR"
                ):
                    exception_logged = True
                    with logfile.open("r") as f:
                        buffer = f.read()
                        assert record.name in buffer
                        assert "Traceback" in buffer
                        assert "App drop thrown" in buffer
            assert exception_logged
            logfile.unlink(missing_ok=True)
