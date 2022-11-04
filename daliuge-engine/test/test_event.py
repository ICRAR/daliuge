from typing import Optional
from dlg.event import EventFirer, EventHandler, Event
import pytest


class MockEventSource(EventFirer):
    def fireEvent(self, eventType, **kwargs):
        self._fireEvent(eventType, **kwargs)


class MockThrowingEventHandler(EventHandler):
    def __init__(self) -> None:
        self.wasCalled = False

    def handleEvent(self, e: Event) -> None:
        self.wasCalled = True
        raise RuntimeError("MockThrow", e)


class MockEventHandler(EventHandler):
    def __init__(self) -> None:
        self.lastEvent: Optional[Event] = None

    def handleEvent(self, e: Event) -> None:
        self.lastEvent = e


def test_listener_exception_interrupts_later_handlers():
    eventSource = MockEventSource()
    handler1 = MockEventHandler()
    handler2 = MockEventHandler()
    throwingHandler = MockThrowingEventHandler()
    eventSource.subscribe(handler1, "raise")
    eventSource.subscribe(throwingHandler, "raise")
    eventSource.subscribe(handler2, "raise")

    with pytest.raises(RuntimeError):
        eventSource.fireEvent("raise", prop="value")

    assert throwingHandler.wasCalled
    assert handler1.lastEvent is not None
    assert getattr(handler1.lastEvent, "prop") == "value"
    assert handler2.lastEvent is None
