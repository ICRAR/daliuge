import threading
from dlg.apps.app_base import InputFiredAppDROP
from dlg.event import Event, EventHandler
import pytest


class MockThrowingDrop(InputFiredAppDROP):
    def run():
        raise RuntimeError("Drop throw")


class MockThrowingHandler(EventHandler):
    def handleEvent(self, e: Event) -> None:
        raise RuntimeError("Handler throw")


def test_async_execute_catches_and_logs_unexpected_exception(
    caplog: pytest.LogCaptureFixture,
):
    drop = MockThrowingDrop("t", "t", n_effective_inputs=1)
    handler = MockThrowingHandler()
    drop.subscribe(handler)

    thread = drop.async_execute()
    assert isinstance(thread, threading.Thread)
    thread.join()

    assert "Handler throw" in caplog.text
    # execute should handle exceptions in the run method
    assert "Drop throw" not in caplog.text


def test_execute_propogates_unexpected_exception():
    drop = MockThrowingDrop("t", "t", n_effective_inputs=1)
    handler = MockThrowingHandler()
    drop.subscribe(handler)

    with pytest.raises(RuntimeError) as e:
        drop.execute()

    assert "Handler throw" in str(e.value)
    assert "Drop throw" not in str(e.value)
