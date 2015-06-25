"""
This module contains classes to "glue" Luigi, NGAS, and DataObject together into
a dataflow deployment and execution system

created on 14-June-2015 by chen.wu@icrar.org
"""

import luigi
from luigi import interface, scheduler, worker
import urllib2, time, os, random
import cPickle as pickle
from dfms.events.event_broadcaster import LocalEventBroadcaster
from dfms.data_object import AbstractDataObject, AppDataObject, StreamDataObject, FileDataObject, ComputeStreamChecksum, ComputeFileChecksum, ContainerDataObject
from collections import defaultdict

DEBUG = True

class DataFlowException(Exception):
    pass

class DataObjectTask(luigi.Task):
    """
    A Luigi Task that wraps a DataObject
    Constructor parameters: a data object it wants to wrap (AbstractDataObject)
    """
    data_obj = luigi.Parameter()

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

class RunDataObjectTask(DataObjectTask):
    """
    The run method should invoke data object on a "remote" data manager
    """
    def output(self):
        """
        if real data object, then return itself
        if app data object, return my consumers (output)
        """
        if (isinstance(self.data_obj, AppDataObject)):
            return self.outputdot
        else:
            return self.dot

    def run(self):
        """
        either ingesting a real data object or running an AppDataObject
        that produces data objects

        TODO - remove dummy code
        """
        msg = "data object {0} on {1}".format(self.data_obj.oid, self.data_obj.location)
        if (isinstance(self.data_obj, AppDataObject)):
            print "Executing application {0}".format(msg)
        else:
            print "Ingesting {0}".format(msg)

    def requires(self):
        """
        producers
        """
        re = [RunDataObjectTask(dob) for dob in self.data_obj.producers]
        if (isinstance(self.data_obj, ContainerDataObject)):
            re += [RunDataObjectTask(dob) for dob in self.data_obj._children]
        return re

class DeployDataObjectTask(DataObjectTask):
    """
    Deploy the data object to a "remote" data manager
    The run method should return a DataObjectTarget
    """
    def output(self):
        return self.dot

    def run(self):
        """
        deploy the data object to the data manager
        TODO - remove dummy code
        """
        print "Deploying data object {0} on {1}".format(self.data_obj.oid, self.data_obj.location)
        #time.sleep(random.randint(1, 3))
        time.sleep(round(random.uniform(1.0, 3.0), 3))

    def requires(self):
        """
        the producer!
        """
        re = [DeployDataObjectTask(dob) for dob in self.data_obj.producers]
        if (isinstance(self.data_obj, ContainerDataObject)):
            re += [DeployDataObjectTask(dob) for dob in self.data_obj._children]
        return re

class PGEngine():
    """
    """
    def __init__(self):
        self.eventbc = LocalEventBroadcaster()

    def traverse_funct(self, data_obj):
        pass

    def traverse_pg(self, node, leafs):
        """
        a naive way
        """
        self.traverse_funct(node)
        cl = node.consumers
        if (len(cl) > 0):
            for ch in cl:
                self.traverse_pg(ch, leafs)
        elif (node.parent is not None):
            self.traverse_pg(node.parent, leafs)
        else:
            leafs.append(node)
            return

    def process(self, pg):
        leafs = []
        self.traverse_pg(pg, leafs)
        return leafs

    def create_test_pg(self):
        island_one = "192.168.1.1:7777"
        island_two = "192.168.1.2:7777"
        dobA = AbstractDataObject('Obj-A', 'Obj-A', self.eventbc)
        dobB = AppDataObject('Obj-B', 'Obj-B', self.eventbc)
        dobC = AbstractDataObject('Obj-C', 'Obj-C', self.eventbc)
        dobD = AppDataObject('Obj-D', 'Obj-D', self.eventbc)

        dobA.location = island_one
        dobB.location = island_one
        dobC.location = island_two
        dobD.location = island_two

        dobA.addConsumer(dobB)
        dobB.addProducer(dobA)
        dobB.addConsumer(dobC)
        dobC.addProducer(dobB)
        dobC.addConsumer(dobD)
        dobD.addProducer(dobC)

        return dobA

    def create_container_pg(self):
        island_one = "192.168.1.1:7777"
        island_two = "192.168.1.2:7777"
        dob_one =  AbstractDataObject('Obj-one', 'Obj-one', self.eventbc)
        dobA = AbstractDataObject('Obj-A', 'Obj-A', self.eventbc)
        dobB = AbstractDataObject('Obj-B', 'Obj-B', self.eventbc)
        dobC = ContainerDataObject('Obj-C', 'Obj-C', self.eventbc)
        dobD = AppDataObject('Obj-D', 'Obj-D', self.eventbc)

        dob_one.location = island_one
        dobA.location = island_one
        dobB.location = island_one
        dobC.location = island_two
        dobD.location = island_two

        dobA.parent = dobC
        dobB.parent = dobC
        dobC.addChild(dobA)
        dobC.addChild(dobB)

        dobC.addConsumer(dobD)
        dobD.addProducer(dobC)

        dob_one.addConsumer(dobA)
        dob_one.addConsumer(dobB)

        dobA.addProducer(dob_one)
        dobB.addProducer(dob_one)

        return dob_one

    def create_mwa_fornax_pg(self):
        num_coarse_ch = 24
        num_split = 3 # number of time splits per channel
        se = self.eventbc
        dob_root = AbstractDataObject("MWA_LTA", "MWA_LTA", se)
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
            dob_ingest = AppDataObject(oid_ingest, oid_ingest, se)
            dob_ingest.location = "f%03d.fornax:7777" % i

            dob_ingest.addProducer(dob_root)
            dob_root.addConsumer(dob_ingest)

            for j in range(1, num_split + 1):
                strj = "%02d" % j
                split_oid = "Subband_{0}_Split_{1}".format(stri, strj)
                dob_split = AbstractDataObject(split_oid, split_oid, se)
                dob_split.location = dob_obs.location
                dob_split.addProducer(dob_ingest)
                dob_ingest.addConsumer(dob_split)
                dob_split.parent = dob_obs
                dob_obs.addChild(dob_split)

            oid_rts = "RTS_{0}".format(stri)
            dob_rts = AppDataObject(oid_rts, oid_rts, se)
            dob_rts.location = dob_obs.location
            dob_rts.addProducer(dob_obs)
            dob_obs.addConsumer(dob_rts)

            oid_subimg = "Subcube_{0}".format(stri)
            dob_subimg = AbstractDataObject(oid_subimg, oid_subimg, se)
            dob_subimg.location = dob_obs.location
            dob_rts.addConsumer(dob_subimg)
            dob_subimg.addProducer(dob_rts)
            dob_subimg.parent = dob_comb_img
            dob_comb_img.addChild(dob_subimg)

        #concatenate all images
        adob_concat_oid = "Concat_image"
        adob_concat = AppDataObject(adob_concat_oid, adob_concat_oid, se)
        adob_concat.location = dob_comb_img.location
        dob_comb_img.addConsumer(adob_concat)
        adob_concat.addProducer(dob_comb_img)

        # produce cube
        dob_cube_oid = "Cube_30.72MHz"
        dob_cube = AbstractDataObject(dob_cube_oid, dob_cube_oid, se)
        dob_cube.location = dob_comb_img.location
        adob_concat.addConsumer(dob_cube)
        dob_cube.addProducer(adob_concat)

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
        dob_root = AbstractDataObject("JVLA", "JVLA", self.eventbc)
        dob_root.location = "NRAO"

        for i in range(1, num_obs + 1):
            stri = "%02d" % i
            oid = "Obs_day_{0}".format(stri)
            dob_obs = AbstractDataObject(oid, oid, self.eventbc)
            dob_obs.location = "{0}.aws-ec2.sydney".format(i)
            dob_obs.addProducer(dob_root)
            dob_root.addConsumer(dob_obs)
            for j in range(1, num_subb + 1):
                app_oid = "mstransform_{0}_{1}".format(stri, "%02d" % j)
                adob_split = AppDataObject(app_oid, app_oid, self.eventbc)
                adob_split.location = dob_obs.location
                dob_obs.addConsumer(adob_split)
                adob_split.addProducer(dob_obs)

                dob_sboid = "Split_{0}_{1}~{2}MHz".format(stri,
                                                          start_freq + subband_width * j,
                                                          start_freq + subband_width * (j + 1))
                dob_sb = AbstractDataObject(dob_sboid, dob_sboid, self.eventbc)
                dob_sb.location = dob_obs.location
                adob_split.addConsumer(dob_sb)
                dob_sb.addProducer(adob_split)

                subband_dict[j].append(dob_sb)

        for j, v in subband_dict.items():
            oid = "Subband_{0}~{1}MHz".format(start_freq + subband_width * j,
                                              start_freq + subband_width * (j + 1))
            dob = ContainerDataObject(oid, oid, self.eventbc)
            dob.location = "{0}.aws-ec2.sydney".format(j % num_obs)
            for dob_sb in v:
                dob.addChild(dob_sb)
                dob_sb.parent = dob

            app_oid = oid.replace("Subband_", "Clean_")
            adob_clean = AppDataObject(app_oid, app_oid, self.eventbc)
            adob_clean.location = dob.location
            dob.addConsumer(adob_clean)
            adob_clean.addProducer(dob)

            img_oid = oid.replace("Subband_", "Image_")
            dob_img = AbstractDataObject(img_oid, img_oid, self.eventbc)
            dob_img.location = dob.location
            adob_clean.addConsumer(dob_img)
            dob_img.addProducer(adob_clean)
            img_list.append(dob_img)

        #container
        dob_comb_img_oid = "Combined_image"
        dob_comb_img = ContainerDataObject(dob_comb_img_oid, dob_comb_img_oid, self.eventbc)
        dob_comb_img.location = "10.1.1.100:7777"
        for dob_img in img_list:
            dob_img.parent = dob_comb_img
            dob_comb_img.addChild(dob_img)

        #concatenate all images
        adob_concat_oid = "Concat_image"
        adob_concat = AppDataObject(adob_concat_oid, adob_concat_oid, self.eventbc)
        adob_concat.location = dob_comb_img.location
        dob_comb_img.addConsumer(adob_concat)
        adob_concat.addProducer(dob_comb_img)

        # produce cube
        dob_cube_oid = "Cube_Day{0}~{1}_{2}~{3}MHz".format(1,
                                                           num_obs,
                                                           start_freq,
                                                           start_freq + total_bandwidth)

        dob_cube = AbstractDataObject(dob_cube_oid, dob_cube_oid, self.eventbc)
        dob_cube.location = dob_comb_img.location
        adob_concat.addConsumer(dob_cube)
        dob_cube.addProducer(adob_concat)

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
        super(PGDeployTask, self).__init__(*args, **kwargs)
        self._leafs = self._deploy()
        self._req = []

    def _deploy(self):
        pg_eng = PGEngine()
        call_nm = "create_{0}_pg".format(self.pg_name).lower()
        if (not hasattr(pg_eng, call_nm)):
            raise DataFlowException("Invalid physical graph '{0}'".format(self.pg_name))
        pg = getattr(pg_eng, call_nm)()
        #pg = pg_eng.create_test_pg()
        return pg_eng.process(pg)

    def requires(self):
        if (len(self._req) == 0):
            for dob in self._leafs:
                self._req.append(DeployDataObjectTask(dob))
        return self._req

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
            if (not self.dob_exists(dob)):
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
    luigi.run()




