"""
This module contains classes to "glue" Luigi, NGAS, and DataObject together into
a dataflow deployment and execution system

created on 14-June-2015 by chen.wu@icrar.org
"""

import luigi
import urllib2, time, os, random
import cPickle as pickle
import signal
import logging
import sys
from dfms import doutils

_logger = logging.getLogger(__name__)

DEBUG = True

class DataFlowException(Exception):
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
        Terminate the current running sub-getEndNodes

        Return    0-Success, -1 - getEndNodes does not exist, 1 - termination error
        """
        if (self._subproc):
            try:
                os.killpg(self._subproc.pid, signal.SIGTERM)
                return (0, '')
            except Exception, oserr:
                #logger.error('Fail to kill getEndNodes %d: %s' % (self._subproc.pid, str(oserr)))
                return (1, str(oserr))
        else:
            return (-1, 'getEndNodes %s does not exist' % self._subproc.pid)

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