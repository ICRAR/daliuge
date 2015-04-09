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
Data manager knows the configuration of compute islands, data islands
It also holds remote proxy data objects pointing to real data objects located 
on various compute islands

Data manager does not "drive" the execution of the entire graph, 
but only gets instructed by Data Object to execute a specific tasks on some locations
as stipulated by the physical graph

"""
from ddap_protocol import DOStates
from data_object import AbstractDataObject, AppDataObject

import Pyro4, threading

def _startDmgrDaemonThread(daemon):
    daemon.requestLoop()

class DataManager():
    """
    Some features:
    start" the physical graph, 
    "enforce" the tasks execution on remote locations
    maintain dataflow semantics (e.g. increment counters, etc.)
    """
    def __init__(self):
        self._eh = DMDOStateEventHandler()
        self._dmr_daemon = Pyro4.Daemon()
    
    def start(self):
        args = (self._dmr_daemon,)
        thref = threading.Thread(None, _startDmgrDaemonThread, 'DMgrThrd', args)
        thref.setDaemon(1)
        print 'Launching data manager daemon'
        thref.start() # TODO - change to multiprocessing
    
    def shutdown(self):
        # spawn a thread
        self._dmr_daemon.shutdown()
    
    def submitPDG(self, pdg, cims):
        """
        traverse the graph, and records all the information, check resource availability
        most importantly, subscribe events to be fired by data objects
        """
        print "the PDG looks OK"
        uri = self._dmr_daemon.register(self._eh)
        proxy_eh = Pyro4.Proxy(uri)
        dolist = []
        self.traverseGraph(dolist, pdg)
        for dob in dolist:
            try:
                dob.subscribeStateChange(proxy_eh)
            except Exception, err:
                print str(err)
        
        return True
    
    def traverseGraph(self, relist, root, excludeAppDo = True):
        """
        a naive tree traverse method
        """
        if (excludeAppDo and isinstance(root, AppDataObject)):
            print "ignore"
        else:
            relist.append(root)
        cl = root.getConsumers()
        if (len(cl) > 0):
            for ch in cl:
                self.traverseGraph(relist, ch, excludeAppDo)
        elif (root.getParent() is not None):
            self.traverseGraph(relist, root.getParent(), excludeAppDo)
        else:
            return
    
    def createDataObject(self, oid, uid, sessionId, appDataObj = False):
        """
        return the URI of the data object (to DFM)
        this method was moved from the compute_island_mgr
        """
        if (self.daemon_dict.has_key(sessionId)):
            dob_daemon = self.daemon_dict[sessionId]
        else:
            dob_daemon = Pyro4.Daemon()
            self.daemon_dict[sessionId] = dob_daemon
        
        if (appDataObj):
            mydo = AppDataObject(oid, uid)
        else:
            mydo = AbstractDataObject(oid, uid)
        
        uri = dob_daemon.register(mydo)
        mydo.setURI(uri)
        self.daemon_dob_dict[sessionId][uri] = mydo
        return uri

class DMDOStateEventHandler():
    """
    """
    def __init__(self):
        #self._dmgr = dmgr
        pass

    def recordDOStateChanges(self, doUri, doState):
        """
        a dummy implementation, should keep them in database (as our PDR docs)
        """
        if (doState == DOStates.DIRTY):
            print "Data object %s is being written" % doUri
        elif (doState == DOStates.COMPLETED):
            print "Data object %s is completed" % doUri
        else:
            print "Data object %s's state changed to %d" % (doUri, doState)
    
    def onStateChange(self, doUri, oldState, newState):
        """
        notify the data manager synchronously, which may have an embarrassingly high latency!
        need to change it to asynchronous
        """
        self.recordDOStateChanges(doUri, newState)
    
    def filterStateChange(self, oldState, newState):
        if (oldState == newState):
            return False
        elif (newState == DOStates.COMPLETED): # Data Manager is currently only interested in the "data object completed/dirty" event
            return True
        elif (newState == DOStates.DIRTY): 
            return True
        else:
            return False


class TaskReScheduler:
    """
    In principle, data manager does not need to schedule anything
    since the physical graph has binded everything
    
    But this is just for failover consideration
    """
    pass
