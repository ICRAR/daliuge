"""
This module contains classes to bridge the gap between Luigi and NGAS

created on 14-June-2015 by chen.wu@icrar.org
"""

import luigi
import urllib2, time, os

DEBUG = True

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

    def __init__(self, oid, host):
        """
        Initializes a DataObjectTarget instance.
        """
        self._oid = oid
        self._dm_host = host # e.g. 192.168.1.2:7777
        self._exists_url = "http://{0}/STATUS?file_id={1}".format(self._dm_host, self._oid)
        self._uri = "http://{0}/RETRIEVE?file_id={1}".format(self._dm_host, self._oid)
        self.checked = dict()

    def exists(self):
        """
        Returns ``True`` if the :py:class:`Target` exists and ``False`` otherwise.
        """
        try:
            if (DEBUG):
                #print self._exists_url
                k = self._oid + " - " + self._dm_host
                #print "app - Exists called: %s" % k
                if (self.checked.has_key(k)):
                    res = 'Status="SUCCESS"'
                else:
                    self.checked[k] = 1
                    res = 'NO'
            else:
                res = urllib2.urlopen(self._exists_url, timeout=10).read()
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

    @property
    def uri(self):
        return self._uri

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

    def execute(self):
        """
        Task manager calls this function to
        execute the task.

        Return:    NGASLocalTaskResult (TODD - link to DataObjectTarget)
        """
        pass

    def stop(self)
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




