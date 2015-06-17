"""
Adapt Luigi as an graph execution engine
to execute "remote" tasks on each NGAs for MWA imaging pipeline

created on 14-June-2015 by chen.wu@icrar.org
"""

import luigi
import time, datetime

from ngas_dm import DataObjectTarget


class IngestScanSplit(luigi.Task):
    obs_id = luigi.Parameter() # observation id, gps time in seconds
    obs_date = luigi.Parameter() # date string
    cc_id = luigi.IntParameter() # coarse channel id
    seq_id = luigi.IntParameter() # sequence id
    job_id = luigi.Parameter()
    dg = luigi.Parameter() # deploy graph

    def __init__(self, *args, **kwargs):
        super(IngestScanSplit, self).__init__(*args, **kwargs)
        self._oid = None
        self._ngas_host = None
        self._dot = None
        #print "IngestScansplit is created {0}/{1}".format(self.cc_id, self.seq_id)

    @property
    def oid(self):
        if (self._oid is None):
            self._oid = "{0}_{1}_gpubox{2}_{3}.fits".format(self.obs_id,
                                                      self.obs_date,
                                                      "%02d" % (self.cc_id),
                                                      "%02d" % (self.seq_id))
        return self._oid

    @property
    def ngas_host(self):
        if (self._ngas_host is None):
            self._ngas_host = self.dg[self.oid]
        return self._ngas_host

    @property
    def dot(self):
        """
        Get an instance of DataObjectTarget (DOT)
        """
        if (self._dot is None):
            self._dot = DataObjectTarget(self.oid, self.ngas_host)
        return self._dot

    def output(self):
        """
        expect a scan split (data object) ingested by data manager (NGAS)
        """
        return self.dot

    def run(self):
        """
        ingest file into NGAS through remote task
        RUNTASK
        """
        # launch remote RUNTASK command on the NGAS host
        print "Ingesting ScanSplit on scan {0} at {1}".format(self.cc_id, self.ngas_host)
        time.sleep(0.5)

class FlowStart(luigi.Task):
    """
    Used as the start of the flow for testing purpose
    """
    obs_id = luigi.Parameter(default="1113797328")
    obs_date = luigi.Parameter(default="20150423041034")
    job_id = luigi.Parameter(default=datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%S'))

    def __init__(self, *args, **kwargs):
        super(FlowStart, self).__init__(*args, **kwargs)
        self._dg = self._build_deploy_graph()
        self._req = []

    def _build_deploy_graph(self):
        """
        this should have been another dataflow or external process

        a list of mappings between files and nodes/dm
        fake implementation for now
        """
        re = dict()
        #1113797328_20150423041034_gpubox
        obs_date = "{0}_{1}_gpubox".format(self.obs_id, self.obs_date)
        for i in range(1, 25):
            re['{0}{1}_00.fits'.format(obs_date, "%02d" % i)] = 'f{0}:7777'.format("%03d" % i)
            re['{0}{1}_01.fits'.format(obs_date, "%02d" % i)] = 'f{0}:7777'.format("%03d" % i)
        return re

    @property
    def req(self):
        if (len(self._req) == 0):
            for i in range(1, 25):
                #print "IngestScansplit is created - {0}/{1}".format(i, 0)
                self._req.append(IngestScanSplit(self.obs_id, self.obs_date, i, 0, self.job_id, self._dg))
                #print "IngestScansplit is created - {0}/{1}".format(i, 1)
                self._req.append(IngestScanSplit(self.obs_id, self.obs_date, i, 1, self.job_id, self._dg))

        return self._req

    def requires(self):
        """
        Since dict (self._dg) is not hashable, so IngestScanSplit cannot be maintained
        in the Luigi "Instance caching"
        """
        return self.req
        """
        for r in self.req:
            yield r
        """

if __name__ == "__main__":
    luigi.run()