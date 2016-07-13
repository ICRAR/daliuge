#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2015
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
"""
The dfms package contains the modules implementing the core functionality of
the system.
"""

import logging

def _setupPyro():
    """
    Sets up Pyro to add support for sending Event objects through and to set a
    finite default call timeout.

    Pyro >= 4.20 uses the 'serpent' serializer by default. In this serializer
    "most custom classes aren't dealt with automatically" [1], including our
    Event class. Thus, in order to support passing events via Pyro we need to
    either instruct Pyro how to serialize the Event class, or to use the
    'pickle' serializer.

    We currently chose to add explicit support for the Event class and keep
    using the 'serpent' serializer. Although more complex, it should be safer
    (see [1] again). We left commented out the lines that would instruct Pyro to
    use the 'pickle' serializer instead, in which case the custom converter
    registration isn't needed.

    [1] https://pythonhosted.org/Pyro4/clientcode.html#serialization
    """

    import importlib
    import Pyro4
    from dfms.event import Event

    def __pyro4_class_to_dict(o):
        d = {'__class__' : o.__class__.__name__, '__module__': o.__class__.__module__}
        d.update(o.__dict__)
        return d

    def __pyro4_dict_to_class(classname, d):
        modname = d['__module__']
        module = importlib.import_module(modname)
        clazz = getattr(module, classname)
        o = clazz()
        for k in d:
            if k in ['__class__', '__module__']: continue
            setattr(o, k, d[k])
        return o

    Pyro4.util.SerializerBase.register_class_to_dict(Event, __pyro4_class_to_dict)
    Pyro4.util.SerializerBase.register_dict_to_class('Event', __pyro4_dict_to_class)

    # This is another way of setting up Pyro to let it serialize the Event
    # class properly: using the pickle serializer
    #Pyro4.config.SERIALIZER = 'pickle'
    #Pyro4.config.SERIALIZERS_ACCEPTED = ['pickle']

    # In Pyro4 >= 4.46 the default for this option changed to True, which would
    # mean we need to decorate all our classes with Pyro-specific code.
    # We don't want that, and thus we restore the old "everything is exposed"
    # behavior.
    Pyro4.config.REQUIRE_EXPOSE = False

    # A final thing: we use a default timeout of 60 [s], which should be more
    # than enough
    Pyro4.config.COMMTIMEOUT = 60

_setupPyro()

# To avoid 'No handlers could be found for logger' messages during testing
logging.getLogger(__name__).addHandler(logging.NullHandler())