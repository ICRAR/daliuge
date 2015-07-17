"""
This module contains classes to "glue" Luigi, NGAS, and DataObject together into
a dataflow deployment and execution system

created on 14-June-2015 by chen.wu@icrar.org
"""

import luigi
import urllib2, time, os, random
import cPickle as pickle
from dfms.events.event_broadcaster import LocalEventBroadcaster
from dfms.data_object import ContainerDataObject, AppConsumer, InMemoryDataObject,\
    ContainerAppConsumer
from collections import defaultdict
import signal
import logging
import sys
from dfms import doutils

_logger = logging.getLogger(__name__)

DEBUG = True

class DataFlowException(Exception):
    pass

class DummyAppDO(InMemoryDataObject, AppConsumer):
    pass

class DataObjectTask(luigi.Task):
    """
    A Luigi Task that wraps a DataObject
    Constructor parameters: a data object it wants to wrap, and the
    session ID
    """
    data_obj = luigi.Parameter()
    session_id = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        """
        Constructor: to include additional parameters (if any)
        """
        super(DataObjectTask, self).__init__(*args, **kwargs)
        self._dot = None
        self._output_dot = None

    @property
    def dot(self): #dot - Data Object Target
        """
        Get an instance of DataObjectTarget (DOT)
        """
        if (self._dot is None):
            self._dot = DataObjectTarget([self.data_obj])
        return self._dot

    @property
    def outputdot(self):
        if (self._output_dot is None):
            self._output_dot = DataObjectTarget(self.data_obj.consumers)
        return self._output_dot

    def requires(self):
        """
        The list of DataObjectTasks that are required by this one.
        We use self.__class__ to create the new dependencies so this method
        doesn't need to be rewritten by all subclasses
        """
        re = []
        taskType = self.__class__
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("Checking requirements for %s %s/%s" %(taskType, self.data_obj.oid, self.data_obj.uid))
        for req in doutils.getUpstreamObjects(self.data_obj):
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug("Added requirement %s/%s" %(req.oid, req.uid))
            re.append(taskType(req, self.session_id))
        return re

class DeployDataObjectTask(DataObjectTask):
    """
    Deploy the data object to a "remote" data manager
    The run method should return a DataObjectTarget
    """

    def __init__(self, *args, **kwargs):
        """
        Constructor: to include additional parameters (if any)
        """
        super(DeployDataObjectTask, self).__init__(*args, **kwargs)
        self._completed = False

    def run(self):
        """
        deploy the data object to the data manager
        TODO - remove dummy code
        """
        print "Deploying data object {0} on {1}".format(self.data_obj.oid, self.data_obj.location)
        #time.sleep(random.randint(1, 3))
        time.sleep(round(random.uniform(1.0, 3.0), 3))
        self._completed = True

    def complete(self):
        return self._completed

class PGEngine():
    """
    """
    def __init__(self):
        self.eventbc = LocalEventBroadcaster()

    def traverse_pg(self, node, leafs, visited):
        """
        Depth-first traversal of the graph
        """
        dependencies = node.consumers
        if isinstance(node, ContainerAppConsumer) and node.children:
            dependencies.extend(node.children)
        elif node.parent:
            dependencies.append(node.parent)

        visited.append(node)
        if not dependencies:
            leafs.append(node)
        else:
            for do in [d for d in dependencies if d not in visited]:
                self.traverse_pg(do, leafs, visited)

    def process(self, pg):
        leafs = []
        if not isinstance(pg, list):
            pg = [pg]
        visited = []
        for root in pg:
            self.traverse_pg(root, leafs, visited)
        return leafs

    def create_test_pg(self):
        island_one = "192.168.1.1:7777"
        island_two = "192.168.1.2:7777"
        dobA = InMemoryDataObject('Obj-A', 'Obj-A', self.eventbc)
        dobB = DummyAppDO('Obj-B', 'Obj-B', self.eventbc)
        dobC = DummyAppDO('Obj-C', 'Obj-C', self.eventbc)

        dobA.location = island_one
        dobB.location = island_one
        dobC.location = island_two

        dobA.addConsumer(dobB)
        dobB.addProducer(dobA)
        dobB.addConsumer(dobC)

        return dobA

    def create_container_pg(self):
        '''
        Creates the following graph:

               |--> A --|
         one --|        | --> C --> D
               |--> B --|
        '''
        island_one = "192.168.1.1:7777"
        island_two = "192.168.1.2:7777"
        dob_one =  InMemoryDataObject('Obj-one', 'Obj-one', self.eventbc)
        dobA = DummyAppDO('Obj-A', 'Obj-A', self.eventbc)
        dobB = DummyAppDO('Obj-B', 'Obj-B', self.eventbc)
        dobC = ContainerDataObject('Obj-C', 'Obj-C', self.eventbc)
        dobD = DummyAppDO('Obj-D', 'Obj-D', self.eventbc)

        dob_one.location = island_one
        dobA.location = island_one
        dobB.location = island_one
        dobC.location = island_two
        dobD.location = island_two

        # Wire together
        dob_one.addConsumer(dobA)
        dob_one.addConsumer(dobB)
        dobC.addChild(dobA)
        dobC.addChild(dobB)
        dobC.addConsumer(dobD)

        return dob_one

    def create_complex_pg(self):
        return self._create_complex_pg(False)

    def create_complex2_pg(self):
        return self._create_complex_pg(True)

    def _create_complex_pg(self, useContainerApps):
        """
        This method creates the following graph

        A --> E -----|                         |--> M --> O
                     |              |---> J ---|
        B -----------|--> G --> H --|          |--> N --> P -->|
                     |              |                          |--> Q
        C --|        |              |-------> K -----> L ----->|
            |--> F --|                  |
        D --|                    I -----|

        In this example the "leaves" of the graph are O and Q, while the
        "roots" are A, B, C, D and I.

        Depending on useContainerApps, node J, M and N will be implemented using
        a DummyAppDO for J and having M and N as its consumers
        (useContainerApps==False) or using a ContainerAppConsumer for J and
        having M and N as its children. The same cannot be applied to the
        H/J/K group because K is already a ContainerDataObject, and having
        H as a ContainerAppConsumer would create a circular dependency between
        the two.
        """

        se = self.eventbc

        if useContainerApps:
            parentType = ContainerAppConsumer
            op = lambda lhs, rhs: lhs.addChild(rhs)
        else:
            parentType = DummyAppDO
            op = lambda lhs, rhs: lhs.addConsumer(rhs)

        a =  InMemoryDataObject('oid:A', 'uid:A', se)
        b =  InMemoryDataObject('oid:B', 'uid:B', se)
        c =  InMemoryDataObject('oid:C', 'uid:C', se)
        d =  InMemoryDataObject('oid:D', 'uid:D', se)
        e =          DummyAppDO('oid:E', 'uid:E', se)
        f = ContainerDataObject('oid:F', 'uid:F', se)
        g = ContainerDataObject('oid:G', 'uid:G', se)
        h =          DummyAppDO('oid:H', 'uid:H', se)
        i =  InMemoryDataObject('oid:I', 'uid:I', se)
        j =          parentType('oid:J', 'uid:J', se)
        k = ContainerDataObject('oid:K', 'uid:K', se)
        l =          DummyAppDO('oid:L', 'uid:L', se)
        m =          DummyAppDO('oid:M', 'uid:M', se)
        n =          DummyAppDO('oid:N', 'uid:N', se)
        o =          DummyAppDO('oid:O', 'uid:O', se)
        p =          DummyAppDO('oid:P', 'uid:P', se)
        q = ContainerDataObject('oid:Q', 'uid:Q', se)

        a.addConsumer(e)
        f.addChild(c)
        f.addChild(d)
        g.addChild(e)
        g.addChild(b)
        g.addChild(f)
        g.addConsumer(h)
        h.addConsumer(j)
        k.addChild(h)
        k.addChild(i)
        k.addConsumer(l)
        op(j, m)
        op(j, n)
        m.addConsumer(o)
        n.addConsumer(p)
        q.addChild(p)
        q.addChild(l)

        return [a, b, c, d, i]

    def create_mwa_fornax_pg(self):
        num_coarse_ch = 24
        num_split = 3 # number of time splits per channel
        se = self.eventbc
        dob_root = InMemoryDataObject("MWA_LTA", "MWA_LTA", se)
        dob_root.location = "Pawsey"

        #container
        dob_comb_img_oid = "Combined_image"
        dob_comb_img = ContainerDataObject(dob_comb_img_oid, dob_comb_img_oid, se)
        dob_comb_img.location = "f032.fornax"

        for i in range(1, num_coarse_ch + 1):
            stri = "%02d" % i
            oid = "Subband_{0}".format(stri)
            dob_obs = ContainerDataObject(oid, oid, se)
            dob_obs.location = "f%03d.fornax" % i

            oid_ingest = "NGAS_{0}".format(stri)
            dob_ingest = DummyAppDO(oid_ingest, oid_ingest, se)
            dob_ingest.location = "f%03d.fornax:7777" % i

            dob_root.addConsumer(dob_ingest)

            for j in range(1, num_split + 1):
                strj = "%02d" % j
                split_oid = "Subband_{0}_Split_{1}".format(stri, strj)
                dob_split = DummyAppDO(split_oid, split_oid, se)
                dob_split.location = dob_obs.location
                dob_ingest.addConsumer(dob_split)
                dob_obs.addChild(dob_split)

            oid_rts = "RTS_{0}".format(stri)
            dob_rts = DummyAppDO(oid_rts, oid_rts, se)
            dob_rts.location = dob_obs.location
            dob_obs.addConsumer(dob_rts)

            oid_subimg = "Subcube_{0}".format(stri)
            dob_subimg = DummyAppDO(oid_subimg, oid_subimg, se)
            dob_subimg.location = dob_obs.location
            dob_rts.addConsumer(dob_subimg)
            dob_comb_img.addChild(dob_subimg)

        #concatenate all images
        adob_concat_oid = "Concat_image"
        adob_concat = DummyAppDO(adob_concat_oid, adob_concat_oid, se)
        adob_concat.location = dob_comb_img.location
        dob_comb_img.addConsumer(adob_concat)

        # produce cube
        dob_cube_oid = "Cube_30.72MHz"
        dob_cube = DummyAppDO(dob_cube_oid, dob_cube_oid, se)
        dob_cube.location = dob_comb_img.location
        adob_concat.addConsumer(dob_cube)

        return dob_root


    def create_chiles_pg(self):

        total_bandwidth = 480
        num_obs = 8 # the same as num of data island
        subband_width = 60 # MHz
        num_subb = total_bandwidth / subband_width
        subband_dict = defaultdict(list) # for corner turning
        img_list = []
        start_freq = 940

        # this should be removed
        dob_root = InMemoryDataObject("JVLA", "JVLA", self.eventbc)
        dob_root.location = "NRAO"

        class CopyAppConsumer(AppConsumer):
            '''An application that fully copies the data from another DO'''
            def run(self, dataObject):
                doutils.copyDataObjectContents(dataObject, self)
                self.setCompleted()
        class InMemoryCopyDataObject(InMemoryDataObject, CopyAppConsumer): pass

        for i in range(1, num_obs + 1):
            stri = "%02d" % i
            oid = "Obs_day_{0}".format(stri)
            dob_obs = InMemoryCopyDataObject(oid, oid, self.eventbc)
            dob_obs.location = "{0}.aws-ec2.sydney".format(i)
            dob_root.addConsumer(dob_obs)
            for j in range(1, num_subb + 1):
                app_oid = "mstransform_{0}_{1}".format(stri, "%02d" % j)
                adob_split = DummyAppDO(app_oid, app_oid, self.eventbc)
                adob_split.location = dob_obs.location
                dob_obs.addConsumer(adob_split)

                dob_sboid = "Split_{0}_{1}~{2}MHz".format(stri,
                                                          start_freq + subband_width * j,
                                                          start_freq + subband_width * (j + 1))
                dob_sb = DummyAppDO(dob_sboid, dob_sboid, self.eventbc)
                dob_sb.location = dob_obs.location
                adob_split.addConsumer(dob_sb)

                subband_dict[j].append(dob_sb)

        for j, v in subband_dict.items():
            oid = "Subband_{0}~{1}MHz".format(start_freq + subband_width * j,
                                              start_freq + subband_width * (j + 1))
            dob = ContainerDataObject(oid, oid, self.eventbc)
            dob.location = "{0}.aws-ec2.sydney".format(j % num_obs)
            for dob_sb in v:
                dob.addChild(dob_sb)

            app_oid = oid.replace("Subband_", "Clean_")
            adob_clean = DummyAppDO(app_oid, app_oid, self.eventbc)
            adob_clean.location = dob.location
            dob.addConsumer(adob_clean)

            img_oid = oid.replace("Subband_", "Image_")
            dob_img = DummyAppDO(img_oid, img_oid, self.eventbc)
            dob_img.location = dob.location
            adob_clean.addConsumer(dob_img)
            img_list.append(dob_img)

        #container
        dob_comb_img_oid = "Combined_image"
        dob_comb_img = ContainerDataObject(dob_comb_img_oid, dob_comb_img_oid, self.eventbc)
        dob_comb_img.location = "10.1.1.100:7777"
        for dob_img in img_list:
            dob_comb_img.addChild(dob_img)

        #concatenate all images
        adob_concat_oid = "Concat_image"
        adob_concat = DummyAppDO(adob_concat_oid, adob_concat_oid, self.eventbc)
        adob_concat.location = dob_comb_img.location
        dob_comb_img.addConsumer(adob_concat)

        # produce cube
        dob_cube_oid = "Cube_Day{0}~{1}_{2}~{3}MHz".format(1,
                                                           num_obs,
                                                           start_freq,
                                                           start_freq + total_bandwidth)

        dob_cube = DummyAppDO(dob_cube_oid, dob_cube_oid, self.eventbc)
        dob_cube.location = dob_comb_img.location
        adob_concat.addConsumer(dob_cube)

        return dob_root


    def pg_to_json(self, pg):
        pass

class PGDeployTask(luigi.Task):
    """
    similar to flow start
    """
    session_id = luigi.Parameter(default=str(time.time()))
    pg_name = luigi.Parameter(default="test")

    #leafs = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        self._doTaskType = DeployDataObjectTask
        super(PGDeployTask, self).__init__(*args, **kwargs)
        self._leafs = self._deploy()
        self._req = []
        self._completed = False

    def _deploy(self):
        pg_eng = PGEngine()
        call_nm = "create_{0}_pg".format(self.pg_name).lower()
        if (not hasattr(pg_eng, call_nm)):
            raise DataFlowException("Invalid physical graph '{0}'".format(self.pg_name))
        pg = getattr(pg_eng, call_nm)()
        return pg_eng.process(pg)

    def requires(self):
        if (len(self._req) == 0):
            for dob in self._leafs:
                if _logger.isEnabledFor(logging.DEBUG):
                    _logger.debug("Adding leaf DO as requirement to PGDeployTask: %s/%s" % (dob.oid, dob.uid))
                self._req.append(self._doTaskType(dob, self.session_id))
        return self._req

    def run(self):
        self._completed = True

    def complete(self):
        return self._completed

class NGASException(Exception):
    """
    Base class for generic NGAS exceptions.
    """
    pass

class NGASDownException(NGASException):
    """
    NGAS server is not running
    """
    pass

class DataObjectTarget(luigi.Target):
    """
    DataObjectTarget is a "deployed" data object on a DataManager / Container,
    Concretely it points to a file managed by an NGAS server
    """

    def __init__(self, data_obj_list):
        """
        Initializes a DataObjectTarget instance.

        data_obj is a LIST of data objects
        """
        self._data_obj_list = data_obj_list
        self.checked = dict()

    def exists(self):
        """
        Returns ``True`` if the :py:class:`Target` exists and ``False`` otherwise.
        """
        for dob in self._data_obj_list:
            if not dob.exists():
                return False
        return True

    def dob_exists(self, dob):
        #self._uri = "http://{0}/RETRIEVE?file_id={1}".format(dob.location, dob.oid)
        try:
            if (DEBUG):
                k = dob.location + " - " + dob.oid
                if (self.checked.has_key(k)):
                    res = 'Status="SUCCESS"'
                else:
                    self.checked[k] = 1
                    res = 'NO'
            else:
                _exists_url = "http://{0}/STATUS?file_id={1}".format(dob.location, dob.oid)
                res = urllib2.urlopen(_exists_url, timeout=10).read()
            if (res.find('Status="SUCCESS"') > -1):
                return True
            else:
                return False
        except urllib2.URLError, urlerr:
            raise NGASDownException(str(urlerr))
        except urllib2.HTTPError, httperr:
            return False
        except Exception, ex:
            raise NGASException(str(ex))
    """
    @property
    def uri(self):
        return self._uri
    """

class NGASTaskClient():
    """
    """
    def __init__(self):
        pass

    def submit_task(self, nhost, nltask, timeout=10):
        """
        nhost:  NGAS host which this task runs (string)
        nltask: an instance of (NGASLocalTask)
        """
        strLT = pickle.dumps(nltask)
        try:
            strRes = urllib2.urlopen('http://%s/RUNTASK' % nhost, data=strLT, timeout=timeout).read()
            #logger.debug('local task {0} submitted, ACK received: {1}'.format(nltask.id, strRes))
            return (0, strRes)
        except urllib2.URLError, urlerr:
            raise NGASDownException(str(urlerr))
        except urllib2.HTTPError, httperr:
            return(1, str(httperr))
        except Exception, ex:
            raise NGASException(str(ex))

    def cancel_task(self, nhost, task_id, timeout=10):
        """
        nhost:  NGAS host which this task runs (string)
        task_id:    string
        """
        try:
            strRes = urllib2.urlopen('http://{0}/RUNTASK?action=cancel&task_id={1}'.format(nhost, urllib2.quote(task_id)), timeout=timeout).read()
            #logger.debug('local task {0} cancel request submitted, ACK received: {1}'.format(nltask.id, strRes))
            return (0, strRes)
        except urllib2.URLError, urlerr:
            raise NGASDownException(str(urlerr))
        except urllib2.HTTPError, httperr:
            return(1, str(httperr))
        except Exception, ex:
            raise NGASException(str(ex))

class NGASLocalTask():
    """
    This is a generic interface
    of task running on each NGAS node

    Sub-class of this class will be marshaled
    to be sent across network
    """
    def __init__(self, taskId):
        """
        Constructor
        taskId:    must be unique (string)
        """
        self._taskId = taskId
        self._subproc = None

    @property
    def id(self):
        return self._taskId

    def execute(self):
        """
        Task manager calls this function to
        execute the task.

        Return:    NGASLocalTaskResult (TODD - link to DataObjectTarget)
        """
        pass

    def stop(self):
        """
        Terminate the current running sub-process

        Return    0-Success, -1 - process does not exist, 1 - termination error
        """
        if (self._subproc):
            try:
                os.killpg(self._subproc.pid, signal.SIGTERM)
                return (0, '')
            except Exception, oserr:
                #logger.error('Fail to kill process %d: %s' % (self._subproc.pid, str(oserr)))
                return (1, str(oserr))
        else:
            return (-1, 'process %s does not exist' % self._subproc.pid)

class NGASLocalTaskResult():
    """
    This class contains result of running
    NGASLocalTask

    TODD - link to DataObjectTarget
    """

    def __init__(self, taskId, errCode, infoMsg, resultAsFile = False):
        """
        Constructor

        taskId:         unique (string)

        errCode:        errCode 0 - Success, [1 , 255] - some sort of error (integer)

        infoMsg:        If True == resultAsFile
                        infoMsg should be the local filesystem path to a file;
                        Otherwise, it is the execution result
                        If errCode != 0, then it is the error message
                        (any)

        resultAsFile:   Whether or not the result should be
                        provided as a separate file for reducers to
                        retrieve. "False" (by default) means
                        the result is directly available in infoMsg.
                        If set to True, infoMsg should be the local
                        filesystem path to a file.
        """
        self._taskId = taskId
        self._errCode = errCode
        self._infoMsg = infoMsg
        self._resultAsFile = resultAsFile
        self._resultUrl = None

    def isResultAsFile(self):
        """
        Return    True or False
        """
        return self._resultAsFile

    def setResultURL(self, resultUrl):
        self._resultUrl = resultUrl

    def getResultURL(self):
        return self._resultUrl

    def getInfo(self):
        return self._infoMsg

    def getErrCode(self):
        return self._errCode

    def setInfo(self, infoMsg):
        self._infoMsg = infoMsg

    def setErrCode(self, errCode):
        self._errCode = errCode

if __name__ == "__main__":
    """
    e.g. python ngas_dm.py PGDeployTask --PGDeployTask-pg-name test
    """
    logging.basicConfig(format="%(asctime)-15s [%(levelname)-5s] [%(threadName)-15s] %(name)s#%(funcName)s:%(lineno)s %(msg)s", level=logging.DEBUG, stream=sys.stdout)
    luigi.run()