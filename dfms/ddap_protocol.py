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
# chen.wu@icrar.org   11/12/2014     Created
#

CST_NS_DOM = 'ddap.dom' # naming prefix for data object manager

class ArchElType:
    """
    Architecture Element Type
    """
    DOM = 'DOM' # data object manager

class DOLinkType:
    CONSUMER, PRODUCER, PARENT, CHILD = range(1, 5)

class DOType:
    ABSTRACT, APP, FILE, STREAM, PROXY = range(1, 6)

class DOStates:
    INITIALIZED, WRITING, COMPLETED, EXPIRED, DELETED = range(1, 6)

class DOPhases:
    PLASMA, GAS, SOLID, LIQUID, LOST = xrange(5)

class GraphExecutionMode:
    """
    Execution modes for a DataObject graph. DO means that DataObjects drive the
    execution of the graph themselves by triggering the consumers automatically
    when DataObjects become COMPLETED. EXTERNAL means that an external actor
    drives the execution of the nodes in the graph, probably by watching over
    the state of DataObjects.
    """
    DO, EXTERNAL = xrange(2)

class REST_API_DOM: # RESTful API (url patterns)
    """
    Refer to http://www.vinaysahni.com/best-practices-for-a-pragmatic-restful-api

    GET /data_objects - Retrieves a list of data objects
    GET /data_objects/{oid} - Retrieves a specific data object (12 is the object id)
    POST /data_objects - Creates a new data object
    POST /data_objects/{oid}/data - Ingests data into data object #12
    POST /data_objects/{oid}/chunk - Streams data chunk into data object # 12
    PUT /data_objects/{oid}/{attr}
    GET /data_objects/{oid}/{attr[=]}

    GET /data_objects/{oid}/run
    POST /data_objects/{oid}/run


    DELETE /data_objects/12 - Deletes data object #12
    """
    DO_CREATE = r"^/data_objects$" # has to be exact match
    DO_INGEST = r"/data_objects/[\S]*/data" # oid could be anything

    LINK = r"/data_objects/12/link?oid={oid}"