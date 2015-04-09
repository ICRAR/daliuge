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
    
    def subscribe(self, callback):
        pass
    
    def unsubscribe(self, callback):
        pass
    
    def fire(self, **attrs):
        pass

class LocalEventBroadcaster(EventBroadcaster):

    def __init__(self, callbacks = []):
        self._callbacks = callbacks

    def subscribe(self, callback):
        self._callbacks.append(callback)
    
    def unsubscribe(self, callback):
        self._callbacks.remove(callback)
    
    def fire(self, **attrs):
        e = Event()
        e.source = self
        for k, v in attrs.iteritems():
            setattr(e, k, v)
        for fn in self._callbacks:
            fn(e)
            