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

# TODO: Maybe these classes should be moved down to the dfms.drop module
#       now that they are only used there. The only ones actually used are
#       Event and LocalEventBroadcaster. We could also collapse the hierarchy of
#       the latter since there is no point in maintaining it anymore

from collections import defaultdict
import logging


logger = logging.getLogger(__name__)

class Event(object):
    pass

class EventBroadcaster(object):

    def subscribe(self, callback, eventType=None):
        pass

    def unsubscribe(self, callback, eventType=None):
        pass

    def fire(self, eventType, **attrs):
        pass

    def _createEvent(self, eventType, **attrs):
        e = Event()
        e.type = eventType
        for k, v in attrs.iteritems():
            setattr(e, k, v)
        return e

class LocalEventBroadcaster(EventBroadcaster):

    __ALL_EVENTS = 'SPECIAL_EVENT_TYPE_THAT_WILL_NEVER_EXIST_EXCEPT_HERE'

    def __init__(self):
        self._callbacks = defaultdict(list)

    def subscribe(self, callback, eventType=None):

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug('Adding subscription to %r eventType=%s: %s' %(self, eventType, callback))

        if eventType is None:
            eventType = self.__ALL_EVENTS
        self._callbacks[eventType].append(callback)

    def unsubscribe(self, callback, eventType=None):
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug('Removing subscription to %r eventType=%s: %s' %(self, eventType, callback))
        if eventType is None:
            eventType = self.__ALL_EVENTS
        if callback in self._callbacks[eventType]:
            self._callbacks[eventType].remove(callback)

    def fire(self, eventType, **attrs):

        # Which callbacks should we call?
        callbacks = []
        if eventType in self._callbacks:
            callbacks += self._callbacks[eventType]
        if self.__ALL_EVENTS in self._callbacks:
            callbacks += self._callbacks[self.__ALL_EVENTS]
        if not callbacks:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug('No callbacks found for eventType=%s' %(eventType))
            return

        e = self._createEvent(eventType, **attrs)
        for fn in callbacks:
            fn(e)