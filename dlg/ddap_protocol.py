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

class DROPLinkType:
    """
    An enumeration of the different relationships that can exist between DROPs.

    Although not explicitly stated in this enumeration, each link type has a
    corresponding inverse. This way, if X is a consumer of Y, Y is an input of
    X. The full list is:
     * CONSUMER           / INPUT
     * STREAMING_CONSUMER / STREAMING_INPUT
     * PRODUCER           / OUTPUT
     * PARENT             / CHILD
    """
    CONSUMER, STREAMING_CONSUMER, PRODUCER, \
    PARENT, CHILD, \
    INPUT, STREAMING_INPUT, OUTPUT = range(8)

class DROPStates:
    """
    An enumeration of the different states a DROP can be found in. DROPs start
    in the INITIALIZED state, go optionally through WRITING and arrive to
    COMPLETED. Later, they transition through EXPIRED, eventually arriving to
    DELETED.
    """
    INITIALIZED, WRITING, COMPLETED, ERROR, EXPIRED, DELETED = range(6)

class AppDROPStates:
    """
    An enumeration of the different execution states an AppDROP can be found in.
    AppDROPs start in the NOT_RUN state, and move to the RUNNING state when they
    are started. Depending on the execution result they eventually move to the
    FINISHED or ERROR state.
    """
    NOT_RUN, RUNNING, FINISHED, ERROR = range(4)

class DROPPhases:
    """
    An enumeration of the different phases a DROP can be found in. Phases
    represent the persistence of the data associated to a DROP and the presence
    of replicas. Phases range from PLASMA (no replicas, volatile storage) to
    SOLID (fully backed up replica available).
    """
    PLASMA, GAS, SOLID, LIQUID, LOST = range(5)

# https://en.wikipedia.org/wiki/Cyclic_redundancy_check#Standards_and_common_use
class ChecksumTypes:
    """
    An enumeration of different methods to calculate the checksum of a piece of
    data. DROPs (in certain conditions) calculate and keep the checksum of
    the data they represent, and therefore also know the method used to
    calculate it.
    """
    CRC_32, CRC_32C = range(2)

class ExecutionMode:
    """
    Execution modes for a DROP. DROP means that a DROP will trigger
    its consumers automatically when it becomes COMPLETED. EXTERNAL means that
    a DROP will *not* trigger its consumers automatically, and instead
    this should be done by an external entity, probably by subscribing to
    changes on the DROP's status.

    This value exists per DROP, and therefore we can achieve a mixed
    execution mode for the entire graph, where some DROPs trigger automatically
    their consumers, while others must be manually executed from the outside.

    Note that if all DROPs in a graph have ExecutionMode == DROP it means that
    the graph effectively drives its own execution without external intervention.
    """
    DROP, EXTERNAL = range(2)

# This is read: "lhs is rel of rhs" (e.g., A is PRODUCER of B)
# lhs and rhs are DROP OIDs
# rel is one of DROPLinkType
DROPRel = collections.namedtuple('DROPRel', ['lhs', 'rel', 'rhs'])
