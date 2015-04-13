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

from dfms.events.event_broadcaster import Event
from dfms.events.event_broadcaster import EventBroadcaster
from Pyro.EventService.Clients import Subscriber
from Pyro.EventService.Clients import Publisher

import threading
import Pyro.core
import Pyro.constants


class PyroSubscriber(Subscriber):
    
    def __init__(self, bcaster):
        super(PyroSubscriber, self).__init__()
        self._bcaster = bcaster
        
        self.pyroSubThread = threading.Thread(None, self._startPyroSub, 'PyroSubThread', ())
        self.pyroSubThread.setDaemon(True)
        self.pyroSubThread.start() 
    
    def _startPyroSub(self):
        self.listen()
        
    def event(self, event):
        self._bcaster._callSubscribers(event.msg)


class PyroPublisher(Publisher):
    
    def __init__(self):
        super(PyroPublisher, self).__init__()


class PyroEventBroadcaster(EventBroadcaster):

    def __init__(self):        
        self._callbacks = {}

        self._pyroPub = PyroPublisher()
        self._pyroSub = PyroSubscriber(self)
        
    def subscribe(self, uid, callback):
        if self._callbacks.has_key(uid):
            self._callbacks[uid].append(callback)
        else:
            self._callbacks[uid] = [callback]
            self._pyroSub.subscribe(uid)
    
    def unsubscribe(self, uid, callback):
        if self._callbacks.has_key(uid):
            cb = self._callbacks[uid]
            cb.remove(callback)
            if len(cb) == 0:
                del self._callbacks[uid]
                self._pyroSub.unsubscribe(uid)
    
    #NOTE: THis is not thread safe!
    def _callSubscribers(self, event):
        if self._callbacks.has_key(event.uid):
            for fn in self._callbacks[event.uid]:
                fn(event)
    
    def fire(self, **attrs):
        e = Event()
        for k, v in attrs.iteritems():
            setattr(e, k, v)
        # publish to uid channel
        self._pyroPub.publish(attrs['uid'], e)
