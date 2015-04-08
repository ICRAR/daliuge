class Event(object):
    pass

class Observable(object):
    
    def __init__(self):
        self._callbacks = []
    
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