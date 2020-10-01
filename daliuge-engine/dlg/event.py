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

import collections
import logging


logger = logging.getLogger(__name__)

class Event(object):
    """
    An event sent through the DALiuGE framework.

    Events have at least a field describing the type of event they are (instead
    of having subclasses of the `Event` class), and therefore this class makes
    sure that at least that field exists. Any other piece of information can be
    attached to individual instances of this class, depending on the event type.
    """

    def __init__(self):
        self.type = None

    def __repr__(self, *args, **kwargs):
        return '<Event %r>' % (self.__dict__)

class EventFirer(object):
    """
    An object that fires events.

    Objects that have an interest on receiving events from this object subscribe
    to it via the `subscribe` method; likewise they can unsubscribe from it via
    the `unsubscribe` method. Events are handled to the listeners by calling
    their `handleEvent` method with the event as its sole argument.

    Listeners can specify the type of event they listen to at subscription time,
    or can also prefer to receive all events fired by this object if they wish
    so.
    """

    __ALL_EVENTS = object()

    def __init__(self):
        self._listeners = collections.defaultdict(list)

    def subscribe(self, listener, eventType=None):
        """
        Subscribes `listener` to events fired by this object. If `eventType` is
        not `None` then `listener` will only receive events of `eventType` that
        originate from this object, otherwise it will receive all events.
        """
        logger.debug('Adding listener to %r eventType=%s: %r', self, eventType, listener)
        eventType = eventType or EventFirer.__ALL_EVENTS
        self._listeners[eventType].append(listener)

    def unsubscribe(self, listener, eventType=None):
        """
        Unsubscribes `listener` from events fired by this object.
        """
        logger.debug('Removing listener to %r eventType=%s: %r', self, eventType, listener)

        eventType = eventType or EventFirer.__ALL_EVENTS
        if listener in self._listeners[eventType]:
            self._listeners[eventType].remove(listener)

    def _fireEvent(self, eventType, **attrs):
        """
        Delivers an event of `eventType` to all interested listeners.

        All the key-value pairs contained in `attrs` are set as attributes of
        the event being sent.
        """

        # Which listeners should we call?
        listeners = []
        if eventType in self._listeners:
            listeners += self._listeners[eventType]
        if EventFirer.__ALL_EVENTS in self._listeners:
            listeners += self._listeners[EventFirer.__ALL_EVENTS]
        if not listeners:
            logger.debug('No listeners found for eventType=%s', eventType)
            return

        # Now that we are sure there are listeners for our event
        # create it and send it to all of them
        e = Event()
        e.type = eventType
        for k, v in attrs.items():
            setattr(e, k, v)

        for l in listeners:
            l.handleEvent(e)