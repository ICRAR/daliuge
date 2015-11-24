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
# chen.wu@icrar.org   10/12/2014     Created
#

"""
Dataflow manager is responsible for producing the physical graph at "design" time

dataflow manager has all the environment-specific implementations, e.g.
"""

import Pyro4

from ddap_protocol import CST_NS_DOM

def buildSimpleIngestPDG(ssid, nsHost=None, nsPort=9090, lifespan=3600):
    """
    This is an example of building a physical dataflow graph (PDG) manually
    Here "building" also includes deploying

    We assume we know hardware infrastructure: two data object managers.
    And the mapping strategy is to let data object manager 001 have data objects A and B, and data object manager 002
    have data objects C and D. [A] and [C] are App data objects, and (B) and (D) are normal data objects

    Both data object managers are located on the same Data Island, and the dataflow goes as follows:

    ---------------------Data-Island---------------
    |                     |                       |
    |[A]-------->(B)------|------>[C]-------->(D) |
    |   Data Object Mgr   |    Data Object Mgr    |
    |       001           |        002            |
    -----------------------------------------------

    To test this, make sure the two Data Object Mgrs (managers) named '001' and '002'
    are up running somewhere within the network, see data_object_mgr.py

    ssid:      sessionId, e.g. partitionId, observationId, sub-graphId, anything that can identify this particular graph
    nsHost:    name service used to locate the compute island manager daemon service (string)
               if None, it will search across the entire network

    returns a tuple with fields: (physical_graph, a_list_of_data_objects_refs)
    """
    print 'Locating Naming Service'
    ns = Pyro4.locateNS(host=nsHost, port=nsPort)

    print 'Contact data object manager 001'
    uriDOM001 = ns.lookup("%s_%s" % (CST_NS_DOM, '001')).asString()
    domServ001 = Pyro4.Proxy(uriDOM001)
    print domServ001.ping()

    print 'Contact data object manager 002'
    uriDOM002 = ns.lookup("%s_%s" % (CST_NS_DOM, '002')).asString()
    domServ002 = Pyro4.Proxy(uriDOM002)
    print domServ002.ping()

    print 'Creating data objects'
    #ssid = datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%S.%f') #sessionId
    uriA = domServ001.createDrop('A', 'A', ssid, lifespan=lifespan)
    uriB = domServ001.createDrop('B', 'B', ssid, lifespan=lifespan, appDataObj = True)
    uriC = domServ002.createDrop('C', 'C', ssid, lifespan=lifespan, appDataObj = True)
    uriD = domServ002.createDrop('D', 'D', ssid, lifespan=lifespan, appDataObj = True)

    print 'Starting data objects daemons on both data object managers'
    ret = domServ001.startDOBDaemon(ssid)
    ret = domServ002.startDOBDaemon(ssid)

    print 'Constructing the graph by connecting the objects...'

    try:
        dobA = Pyro4.Proxy(uriA) # get hold of the root of the graph
        dobB = Pyro4.Proxy(uriB) # each DROP is already associated with a data object mgr
        dobC = Pyro4.Proxy(uriC)
        dobD = Pyro4.Proxy(uriD)
        #"""
        dobA.addConsumer(dobB)
        dobB.addConsumer(dobC)
        dobC.addConsumer(dobD)

        #print 'Executing the graph...'
        #dobA.run(None, None)
        return (dobA, [domServ001, domServ002])
    except Exception, exc:
        print 'shutting down two DOB daemons'
        ret = domServ001.shutdownDOBDaemon(ssid)
        ret = domServ002.shutdownDOBDaemon(ssid)
        raise exc

def buildLoopPGraph(ssid, nsHost = None):
    """
    This is an example of creating a physical graph manually
    The example is based on the PDR document:

    """
    pass

if __name__ == '__main__':
    # test
    buildSimpleIngestPDG('123456', nsHost = 'localhost')
