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

class Event(object):
    pass

class EventBroadcaster(object):
    
    def subscribe(self, uid, callback):
        pass
    
    def unsubscribe(self, uid, callback):
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

    def __init__(self):
        self._callbacks = {}

    def subscribe(self, uid, callback):
        if self._callbacks.has_key(uid):
            self._callbacks[uid].append(callback)
        else:
            self._callbacks[uid] = [callback]

    def unsubscribe(self, uid, callback):
        if self._callbacks.has_key(uid):
            self._callbacks[uid].remove(callback)

    def fire(self, eventType, **attrs):
        e = self._createEvent(eventType, **attrs)
        uid = attrs['uid']
        if self._callbacks.has_key(uid):
            for fn in self._callbacks[uid]:
                fn(e)
