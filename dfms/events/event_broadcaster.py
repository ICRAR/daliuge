#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2014
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
# Who                   When          What
# ------------------------------------------------
# dave.pallot@icrar.org   9/Apr/2015     Created
#
import threading
import logging
from collections import defaultdict

_logger = logging.getLogger(__name__)

class Event(object):
    pass

class EventBroadcaster(object):

    def subscribe(self, uid, callback, eventType=None):
        pass

    def unsubscribe(self, uid, callback, eventType=None):
        pass

    def fire(self, eventType, **attrs):
        pass

    def _createEvent(self, eventType, **attrs):
        e = Event()
        e.type = eventType
        for k, v in attrs.iteritems():
            setattr(e, k, v)
        return e

class AbstractEventBroadcaster(EventBroadcaster):

    __ALL_EVENTS = 'SPECIAL_EVENT_TYPE_THAT_WILL_NEVER_EXIST_EXCEPT_HERE'

    def __init__(self):
        self._callbacks = defaultdict(lambda: defaultdict(list))

    def subscribe(self, uid, callback, eventType=None):

        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug('Adding subscription to uid=%s, eventType=%s: %s' %(uid, eventType, callback))

        if eventType is None:
            eventType = self.__ALL_EVENTS
        self._callbacks[uid][eventType].append(callback)

    def unsubscribe(self, uid, callback, eventType=None):
        if eventType is None:
            eventType = self.__ALL_EVENTS
        if self._callbacks.has_key(uid):
            self._callbacks[uid][eventType].remove(callback)

    def fire(self, eventType, **attrs):

        uid = attrs['uid']

        # Nobody subscribed to us
        if uid not in self._callbacks:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug('No callbacks found for uid=%s' %(uid))
            return

        # Which callbacks should we call?
        callbacks = []
        if eventType in self._callbacks[uid]:
            callbacks.extend(self._callbacks[uid][eventType])
        if self.__ALL_EVENTS in self._callbacks[uid]:
            callbacks.extend(self._callbacks[uid][self.__ALL_EVENTS])
        if not callbacks:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug('No callbacks found for uid=%s, eventType=%s' %(uid, eventType))

            return

        e = self._createEvent(eventType, **attrs)
        self.callBack(callbacks, e)

    def callBack(self, callbacks, event):
        pass

class LocalEventBroadcaster(AbstractEventBroadcaster):
    """
    A simple event broadcaster that calls all necessary callbacks in sequence
    """
    def callBack(self, callbacks, event):
        for fn in callbacks:
            fn(event)

class ThreadedEventBroadcaster(AbstractEventBroadcaster):
    """
    An simple event broadcaster that creates a new daemon thread for each
    subscriber when an event is fired and starts them. This way even firing is
    handled asynchronously from the caller, and also in parallel for different
    subscribers.
    """

    def __init__(self):
        super(ThreadedEventBroadcaster, self).__init__()
        self._countLock = threading.RLock()
        self._counter = 1

    def getAndIncreaseCounter(self):
        with self._countLock:
            c = self._counter
            self._counter +=1
            return c

    def callBack(self, callbacks, event):
        for fn in callbacks:
            t = threading.Thread(None, lambda fn, e: fn(e), 'eb-%d' % (self.getAndIncreaseCounter()), [fn, event])
            t.daemon = 1
            t.start()