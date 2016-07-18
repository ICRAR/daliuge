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
A modules that contains several functions returning different "physical graphs",
at this moment represented simply by a number of DROPs interconnected.

Graph-creator methods must accept no arguments, and must return the starting
point(s) for the graph they create.

All the graphs created in this repository have (up to now) a corresponding
test method in the test_luigi module, where it is automatically verified that
the graph can actually be executed with luigi.
"""
# All DROPs created in the methods of this module have a lifespan of half an hour.
# This is to allow sufficient time for users to feed data into the initial
# DROPs (often a SocketListener DROP) and let the graph execute

import collections
import inspect
import random
import time

from dfms import droputils
from dfms.apps.socket_listener import SocketListenerApp
from dfms.drop import InMemoryDROP, BarrierAppDROP, ContainerDROP
from dfms.ddap_protocol import ExecutionMode
from test.test_drop import SumupContainerChecksum


lifespan = 1800

#===============================================================================
# Support AppDROP classes
#===============================================================================
class SimpleBarrierApp(BarrierAppDROP):
    def run(self):
        pass

class SleepApp(BarrierAppDROP):
    """
    A simple application consumer that sleeps between 0 and 4 seconds (or the
    specified amount of time) without taking any further action.
    """
    def initialize(self, **kwargs):
        super(SleepApp, self).initialize(**kwargs)
        self._sleepTime = self._getArg(kwargs, 'sleepTime', 0)

    def run(self):
        time.sleep(self._sleepTime)

class SleepAndCopyApp(SleepApp):
    """
    A simple application consumer that sleeps between 0 and 4 seconds (or the
    specified amount of time) and then fully copies the contents of the
    DROP it consumes into each of the DROPs it writes to. If there
    are more than one DROP being consumed, the individual contents of each
    DROPs are written into each output.
    """

    def run(self):
        super(SleepAndCopyApp, self).run()
        self.copyAll()

    def copyAll(self):
        for inputDrop in self.inputs:
            with inputDrop:
                self.copyRecursive(inputDrop)

    def copyRecursive(self, inputDrop):
        if isinstance(inputDrop, ContainerDROP):
            for child in inputDrop.children:
                self.copyRecursive(child)
        else:
            for outputDrop in self.outputs:
                with outputDrop:
                    droputils.copyDropContents(inputDrop, outputDrop)

#===============================================================================
# Methods that create graphs follow. They must have no arguments to be
# recognized as such
#===============================================================================
def testGraphDropDriven():
    return _testGraph(ExecutionMode.DROP)

def testGraphLuigiDriven():
    return _testGraph(ExecutionMode.EXTERNAL)

def testGraphMixed():
    return _testGraph(None)

def _testGraph(execMode):
    """
    A test graph that looks like this:

                 |--> B1 --> C1 --|
                 |--> B2 --> C2 --|
    SL_A --> A --|--> B3 --> C3 --| --> D --> E
                 |--> .. --> .. --|
                 |--> BN --> CN --|

    B and C DROPs are InMemorySleepAndCopyApp DROPs (see above). D is simply a
    container. A is a socket listener, so we can actually write to it externally
    and watch the progress of the luigi tasks. We give DROPs a long lifespan;
    otherwise they will expire and luigi will see it as a failed task (which is
    actually right!)

    If execMode is given we use that in all DROPs. If it's None we use a mixture
    of DROP/EXTERNAL execution modes.
    """

    aMode = execMode if execMode is not None else ExecutionMode.EXTERNAL
    bMode = execMode if execMode is not None else ExecutionMode.DROP
    cMode = execMode if execMode is not None else ExecutionMode.DROP
    dMode = execMode if execMode is not None else ExecutionMode.EXTERNAL
    eMode = execMode if execMode is not None else ExecutionMode.EXTERNAL

    sl_a =      SocketListenerApp('oid:SL_A', 'uid:SL_A', executionMode=aMode, lifespan=lifespan)
    a    =     InMemoryDROP('oid:A', 'uid:A', executionMode=aMode, lifespan=lifespan)
    d    = SumupContainerChecksum('oid:D', 'uid:D', executionMode=dMode, lifespan=lifespan)
    e    =     InMemoryDROP('oid:E', 'uid:E', executionMode=eMode, lifespan=lifespan)

    sl_a.addOutput(a)
    e.addProducer(d)
    for i in xrange(random.SystemRandom().randint(10, 20)):
        b =    SleepAndCopyApp('oid:B%d' % (i), 'uid:B%d' % (i), executionMode=bMode, lifespan=lifespan)
        c = InMemoryDROP('oid:C%d' % (i), 'uid:C%d' % (i), executionMode=cMode, lifespan=lifespan)
        a.addConsumer(b)
        b.addOutput(c)
        c.addConsumer(d)
    return sl_a

def test_pg():
    """
    A very simple graph that looks like this:

    A --> B --> C --> D
    """
    a =  SocketListenerApp('A', 'A', lifespan=lifespan)
    b = InMemoryDROP('B', 'B', lifespan=lifespan)
    c =    SleepAndCopyApp('C', 'C', lifespan=lifespan)
    d = InMemoryDROP('D', 'D', lifespan=lifespan)

    a.addOutput(b)
    b.addConsumer(c)
    c.addOutput(d)

    return a

def container_pg():
    '''
    Creates the following graph:

                |--> B --> D --|
     SL --> A --|              |--> F --> G --> H --> I
                |--> C --> E --|
    '''
    sl= SocketListenerApp('SL', 'SL', lifespan=lifespan)
    a = InMemoryDROP('A', 'A', lifespan=lifespan)
    b = SleepAndCopyApp('B', 'B', lifespan=lifespan)
    c = SleepAndCopyApp('C', 'C', lifespan=lifespan)
    d = InMemoryDROP('D', 'D', lifespan=lifespan)
    e = InMemoryDROP('E', 'E', lifespan=lifespan)
    f = SimpleBarrierApp('F', 'F', lifespan=lifespan)
    g = ContainerDROP('G', 'G', lifespan=lifespan)
    h = SleepAndCopyApp('H', 'H', lifespan=lifespan)
    i = InMemoryDROP('I', 'I', lifespan=lifespan)

    # Wire together
    sl.addOutput(a)
    a.addConsumer(b)
    a.addConsumer(c)
    b.addOutput(d)
    c.addOutput(e)
    d.addConsumer(f)
    e.addConsumer(f)
    f.addOutput(g)
    g.addChild(d)
    g.addChild(e)
    g.addConsumer(h)
    h.addOutput(i)

    return sl

def complex_graph():
    """
    This method creates the following graph

    SL_A --> A -----> E --> G --|                         |--> N ------> Q --> S
                                |              |---> L ---|
    SL_B --> B -----------------|--> I --> J --|          |--> O -->|
                                |              |                    |--> R --> T
    SL_C --> C --|              |              |---> M ------> P -->|
                 |--> F --> H --|                |
    SL_D --> D --|                SL_K --> K ----|

    In this example the "leaves" of the graph are S and T, while the
    "roots" are SL_A, SL_B, SL_C, SL_D and SL_K.

    E, F, I, L, M, Q and R are AppDROPs; SL_* are SocketListenerApps. The
    rest are plain in-memory DROPs
    """

    sl_a,sl_b,sl_c,sl_d,sl_k = [ SocketListenerApp('sl_' + uid, 'sl_' + uid, lifespan=lifespan, port=port) for uid,port in [('a',1111),('b',1112),('c',1113),('d',1114),('k',1115)]]
    a, b, c, d, k            = [InMemoryDROP(uid, uid, lifespan=lifespan) for uid in ['a', 'b', 'c', 'd', 'k']]
    e, f, i, l, m, q, r      = [   SleepAndCopyApp(uid, uid, lifespan=lifespan) for uid in ['e', 'f', 'i', 'l', 'm', 'q', 'r']]
    g, h, j, n, o, p, s, t   = [InMemoryDROP(uid, uid, lifespan=lifespan) for uid in ['g', 'h', 'j', 'n', 'o', 'p', 's', 't']]

    for plainDrop, appDrop in [(a,e), (b,i), (c,f), (d,f), (h,i), (j,l), (j,m), (k,m), (n,q), (o,r), (p,r)]:
        plainDrop.addConsumer(appDrop)
    for appDrop, plainDrop in [(sl_a,a), (sl_b,b), (sl_c,c), (sl_d,d), (sl_k,k), (e,g), (f,h), (i,j), (l,n), (l,o), (m,p), (q,s), (r,t)]:
        appDrop.addOutput(plainDrop)

    return [sl_a, sl_b, sl_c, sl_d, sl_k]

def mwa_fornax_pg():
    num_coarse_ch = 24
    num_split = 3 # number of time splits per channel

    dob_root_sl = SocketListenerApp("MWA_LTA_SL", "MWA_LTA_SL", lifespan=lifespan)
    dob_root = InMemoryDROP("MWA_LTA", "MWA_LTA", lifespan=lifespan)
    dob_root.location = "Pawsey"
    dob_root_sl.addOutput(dob_root)

    #container
    dob_comb_img_oid = "Combined_image"
    combineImgAppOid = "Combine image"
    dob_comb_img = InMemoryDROP(dob_comb_img_oid, dob_comb_img_oid, lifespan=lifespan)
    combineImgApp = SleepAndCopyApp(combineImgAppOid, combineImgAppOid)
    combineImgApp.addOutput(dob_comb_img)
    dob_comb_img.location = "f032.fornax"

    for i in range(1, num_coarse_ch + 1):

        stri = "%02d" % i
        oid = "Subband_{0}".format(stri)
        appOid = "Combine_" + oid
        dob_obs = InMemoryDROP(oid, oid, lifespan=lifespan)
        combineSubbandApp = SleepAndCopyApp(appOid, appOid, lifespan=lifespan)
        combineSubbandApp.addOutput(dob_obs)
        dob_obs.location = "f%03d.fornax" % i

        oid_ingest = "NGAS_{0}".format(stri)
        dob_ingest = SleepAndCopyApp(oid_ingest, oid_ingest, lifespan=lifespan)
        dob_ingest.location = "f%03d.fornax:7777" % i

        dob_root.addConsumer(dob_ingest)

        for j in range(1, num_split + 1):
            strj = "%02d" % j
            split_oid = "Subband_{0}_Split_{1}".format(stri, strj)
            dob_split = InMemoryDROP(split_oid, split_oid, lifespan=lifespan)
            dob_split.location = dob_obs.location
            dob_ingest.addOutput(dob_split)
            combineSubbandApp.addInput(dob_split)

        oid_rts = "RTS_{0}".format(stri)
        dob_rts = SleepAndCopyApp(oid_rts, oid_rts, lifespan=lifespan)
        dob_rts.location = dob_obs.location
        dob_obs.addConsumer(dob_rts)

        oid_subimg = "Subcube_{0}".format(stri)
        dob_subimg = InMemoryDROP(oid_subimg, oid_subimg, lifespan=lifespan)
        dob_subimg.location = dob_obs.location
        dob_rts.addOutput(dob_subimg)
        combineImgApp.addInput(dob_subimg)

    #concatenate all images
    adob_concat_oid = "Concat_image"
    adob_concat = SleepAndCopyApp(adob_concat_oid, adob_concat_oid, lifespan=lifespan)
    adob_concat.location = dob_comb_img.location
    dob_comb_img.addConsumer(adob_concat)

    # produce cube
    dob_cube_oid = "Cube_30.72MHz"
    dob_cube = InMemoryDROP(dob_cube_oid, dob_cube_oid, lifespan=lifespan)
    dob_cube.location = dob_comb_img.location
    adob_concat.addOutput(dob_cube)

    return dob_root_sl

def chunks(l, n):
    """
    Yield successive n-sized chunks from l.
    http://stackoverflow.com/questions/312443/
    how-do-you-split-a-list-into-evenly-sized-chunks-in-python
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

def lofar_standard_pip_pg():
    """
    Lofar simple imaging pipeline
    based on
    Ger's email and
    https://github.com/lofar-astron/lofar-profiling
    """
    roots = []

    num_time_slice = 3
    total_num_subband = 8
    num_subb_per_image = 2

    Cal_model_oid = "A-team/Calibrator"
    dob_cal_model = InMemoryDROP(Cal_model_oid, Cal_model_oid, lifespan=lifespan)
    dob_cal_model.location = "catalogue.Groningen"
    roots.append(dob_cal_model)

    make_source_app_oid = "makesourcedb"
    make_source_app = SleepAndCopyApp(make_source_app_oid, make_source_app_oid)
    make_source_app.location = dob_cal_model.location
    dob_cal_model.addConsumer(make_source_app)

    source_db_oid = "CalSource.db"
    dob_source_db = InMemoryDROP(source_db_oid, source_db_oid, lifespan=lifespan)
    dob_source_db.location = dob_cal_model.location
    make_source_app.addOutput(dob_source_db)

    GSM_oid = "GlobalSkyModel"
    dob_gsm = InMemoryDROP(GSM_oid, GSM_oid, lifespan=lifespan)
    dob_gsm.location = "catalogue.Groningen"
    roots.append(dob_gsm)

    dob_img_dict = dict()
    sb_chunks = chunks(range(1, total_num_subband + 1), num_subb_per_image)
    for k, img_list in enumerate(sb_chunks):
        imger_oid = "AWImager_SB_{0}~{1}".format(img_list[0], img_list[-1])
        dob_img = SleepAndCopyApp(imger_oid, imger_oid, lifespan=lifespan)
        kk = k + 1
        #print k, img_list, kk
        img_prod_oid = "Image_SB_{0}~{1}".format(img_list[0], img_list[-1])
        dob_img_prod = InMemoryDROP(img_prod_oid, img_prod_oid, lifespan=lifespan)
        dob_img.addOutput(dob_img_prod)
        dob_img_dict[kk] = (dob_img, dob_img_prod)

    for j in range(1, num_time_slice + 1):
        sli = "T%02d" % j
        for i in range(1, total_num_subband + 1):
            # for LBA, assume each subband should have two beams
            # one for calibration pipeline, the other for target / imaging pipeline
            stri = "%s_SB%02d" % (sli, i)
            time_slice_oid = "Socket_{0}".format(stri)
            dob_time_slice_sl = SocketListenerApp(time_slice_oid + "_SL", time_slice_oid + "_SL", lifespan=lifespan)
            dob_time_slice = InMemoryDROP(time_slice_oid, time_slice_oid, lifespan=lifespan)
            dob_time_slice_sl.addOutput(dob_time_slice)
            # all the same sub-bands (i) will be on the same node regardless of its time slice j
            dob_time_slice.location = "Node%03d.Groningen" % i
            roots.append(dob_time_slice_sl)

            oid_data_writer = "DataWriter_{0}".format(stri)
            dob_ingest = SleepAndCopyApp(oid_data_writer, oid_data_writer, lifespan=lifespan)
            dob_ingest.location = dob_time_slice.location
            dob_time_slice.addConsumer(dob_ingest)

            # assume a single measuremenset has multiple beams
            oid = "CalNTgt.MS_{0}".format(stri)
            dob_input = InMemoryDROP(oid, oid, lifespan=lifespan)
            dob_ingest.addOutput(dob_input)
            dob_input.location = dob_ingest.location

            #split by beam - calibrator beam and target beam
            appOid = "Split_{0}".format(stri)
            splitApp = SleepAndCopyApp(appOid, appOid, lifespan=lifespan)
            splitApp.location = dob_ingest.location
            dob_input.addConsumer(splitApp)

            oid = "Cal_BEAM_{0}".format(stri)
            dob_cal_beam = InMemoryDROP(oid, oid, lifespan=lifespan)
            dob_cal_beam.location = dob_ingest.location
            splitApp.addOutput(dob_cal_beam)

            oid = "Tgt_BEAM_{0}".format(stri)
            dob_tgt_beam = InMemoryDROP(oid, oid, lifespan=lifespan)
            dob_tgt_beam.location = dob_ingest.location
            splitApp.addOutput(dob_tgt_beam)

            # flag the cal beam
            oid = "FlgCal.MS_{0}".format(stri)
            appOid = "NDPPP_Cal" + stri
            dob_cal_flagged = InMemoryDROP(oid, oid, lifespan=lifespan)
            flagCalApp = SleepAndCopyApp(appOid, appOid, lifespan=lifespan)
            dob_cal_beam.addConsumer(flagCalApp)
            dob_source_db.addConsumer(flagCalApp)
            flagCalApp.addOutput(dob_cal_flagged)
            flagCalApp.location = dob_ingest.location
            dob_cal_flagged.location = dob_ingest.location #"C%03d.Groningen" % i

            # flag the target beam
            oid = "FlgTgt.MS_{0}".format(stri)
            appOid = "NDPPP_Tgt" + stri
            dob_tgt_flagged = InMemoryDROP(oid, oid, lifespan=lifespan)
            flagTgtApp = SleepAndCopyApp(appOid, appOid, lifespan=lifespan)
            dob_tgt_beam.addConsumer(flagTgtApp)
            dob_source_db.addConsumer(flagTgtApp)
            flagTgtApp.addOutput(dob_tgt_flagged)
            flagTgtApp.location = dob_ingest.location
            dob_tgt_flagged.location = dob_ingest.location

            # For calibration, each time slice is calibrated independently, and
            # the result is an output MS per subband
            # solve the gain
            oid = "Gain.TBL_{0}".format(stri)
            appOid = "BBS_GainCal_{0}".format(stri)
            dob_param_tbl = InMemoryDROP(oid, oid, lifespan=lifespan)
            gainCalApp = SleepAndCopyApp(appOid, appOid, lifespan=lifespan)
            dob_cal_flagged.addConsumer(gainCalApp)
            gainCalApp.addOutput(dob_param_tbl)
            gainCalApp.location = dob_ingest.location
            dob_param_tbl.location = dob_ingest.location

            # or apply the gain to the dataset
            oid = "CAL.MS_{0}".format(stri)
            appOid = "BBS_ApplyCal_{0}".format(stri)
            dob_calibrated = InMemoryDROP(oid, oid, lifespan=lifespan)
            applyCalApp = SleepAndCopyApp(appOid, appOid, lifespan=lifespan)
            dob_tgt_flagged.addConsumer(applyCalApp)
            dob_param_tbl.addConsumer(applyCalApp)
            applyCalApp.addOutput(dob_calibrated)
            applyCalApp.location = dob_ingest.location
            dob_calibrated.location = dob_ingest.location

            # extract local sky model (LSM) from GSM
            appOid = "Extract_LSM_{0}".format(stri)
            extractLSMApp = SleepAndCopyApp(appOid, appOid, lifespan=lifespan)
            extractLSMApp.location = dob_ingest.location
            dob_gsm.addConsumer(extractLSMApp)
            LSM_oid = "LSM_{0}".format(stri)
            dob_lsm = InMemoryDROP(LSM_oid, LSM_oid, lifespan=lifespan)
            dob_lsm.location = dob_ingest.location
            extractLSMApp.addOutput(dob_lsm)

            # convert LSM to source.db
            makesource_app_oid = "makesourcedb_{0}".format(stri)
            makesource_app = SleepAndCopyApp(makesource_app_oid, makesource_app_oid)
            makesource_app.location = dob_ingest.location
            dob_lsm.addConsumer(makesource_app)

            sourcedb_oid = "LSM_db_{0}".format(stri)
            dob_lsmdb = InMemoryDROP(sourcedb_oid, sourcedb_oid, lifespan=lifespan)
            dob_lsmdb.location = dob_ingest.location
            makesource_app.addOutput(dob_lsmdb)

            # add awimager
            img_k = (i - 1) / num_subb_per_image + 1
            dob_img, dob_img_prod = dob_img_dict[img_k]
            dob_calibrated.addConsumer(dob_img)
            dob_lsmdb.addConsumer(dob_img)
            dob_img.location = dob_calibrated.location #overwrite to the last i
            dob_img_prod.location = dob_img.location

    return roots

def pip_cont_img_pg():
    """
    PIP continuum imaging pipeline
    """
    num_beam = 1
    num_time = 2
    num_freq = 2
    num_facet = 2
    num_grid = 4
    stokes = ['I', 'Q', 'U', 'V']
    dob_root_sl = SocketListenerApp("FullDataset_SL", "FullDataset_SL", lifespan=lifespan)
    dob_root = InMemoryDROP("FullDataset", "FullDataset", lifespan=lifespan)
    dob_root_sl.addOutput(dob_root)
    dob_root.location = "Buf01"

    adob_sp_beam = SleepAndCopyApp("SPLT_BEAM", "SPLT_BEAM", lifespan=lifespan)
    adob_sp_beam.location = "Buf01"
    dob_root.addConsumer(adob_sp_beam)

    for i in range(1, num_beam + 1):
        uid = i
        dob_beam = InMemoryDROP("BEAM_{0}".format(uid), "BEAM_{0}".format(uid), lifespan=lifespan)
        adob_sp_beam.addOutput(dob_beam)
        adob_sp_time = SleepAndCopyApp("SPLT_TIME_{0}".format(uid), "SPLT_TIME_{0}".format(uid), lifespan=lifespan)
        dob_beam.addConsumer(adob_sp_time)
        for j in range(1, num_time + 1):
            uid = "%d-%d" % (i, j)
            dob_time = InMemoryDROP("TIME_{0}".format(uid), "TIME_{0}".format(uid), lifespan=lifespan)
            adob_sp_time.addOutput(dob_time)
            adob_sp_freq = SleepAndCopyApp("SPLT_FREQ_{0}".format(uid), "SPLT_FREQ_{0}".format(uid), lifespan=lifespan)
            dob_time.addConsumer(adob_sp_freq)
            for k in range(1, num_freq + 1):
                uid = "%d-%d-%d" % (i, j, k)
                dob_freq = InMemoryDROP("FREQ_{0}".format(uid), "FREQ_{0}".format(uid), lifespan=lifespan)
                adob_sp_freq.addOutput(dob_freq)
                adob_sp_facet = SleepAndCopyApp("SPLT_FACET_{0}".format(uid), "SPLT_FACET_{0}".format(uid), lifespan=lifespan)
                dob_freq.addConsumer(adob_sp_facet)
                for l in range(1, num_facet + 1):
                    uid = "%d-%d-%d-%d" % (i, j, k, l)
                    dob_facet = InMemoryDROP("FACET_{0}".format(uid), "FACET_{0}".format(uid), lifespan=lifespan)
                    adob_sp_facet.addOutput(dob_facet)

                    adob_ph_rot = SleepAndCopyApp("PH_ROTATN_{0}".format(uid), "PH_ROTATN_{0}".format(uid), lifespan=lifespan)
                    dob_facet.addConsumer(adob_ph_rot)
                    dob_ph_rot = InMemoryDROP("PH_ROTD_{0}".format(uid), "PH_ROTD_{0}".format(uid), lifespan=lifespan)
                    adob_ph_rot.addOutput(dob_ph_rot)
                    adob_sp_stokes = SleepAndCopyApp("SPLT_STOKES_{0}".format(uid), "SPLT_STOKES_{0}".format(uid), lifespan=lifespan)
                    dob_ph_rot.addConsumer(adob_sp_stokes)

                    adob_w_kernel = SleepAndCopyApp("CACL_W_Knl_{0}".format(uid), "CACL_W_Knl_{0}".format(uid), lifespan=lifespan)
                    dob_facet.addConsumer(adob_w_kernel)
                    dob_w_knl = InMemoryDROP("W_Knl_{0}".format(uid), "W_Knl_{0}".format(uid), lifespan=lifespan)
                    adob_w_kernel.addOutput(dob_w_knl)

                    adob_a_kernel = SleepAndCopyApp("CACL_A_Knl_{0}".format(uid), "CACL_A_Knl_{0}".format(uid), lifespan=lifespan)
                    dob_facet.addConsumer(adob_a_kernel)
                    dob_a_knl = InMemoryDROP("A_Knl_{0}".format(uid), "A_Knl_{0}".format(uid), lifespan=lifespan)
                    adob_a_kernel.addOutput(dob_a_knl)

                    adob_create_kernel = SleepAndCopyApp("CREATE_Knl_{0}".format(uid), "CREATE_Knl_{0}".format(uid), lifespan=lifespan)
                    dob_w_knl.addConsumer(adob_create_kernel)
                    dob_a_knl.addConsumer(adob_create_kernel)


                    for stoke in stokes:
                        uid = "%s-%d-%d-%d-%d" % (stoke, i, j, k, l)
                        #print "uid = {0}".format(uid)

                        dob_stoke = InMemoryDROP("STOKE_{0}".format(uid), "STOKE_{0}".format(uid), lifespan=lifespan)
                        adob_sp_stokes.addOutput(dob_stoke)

                        dob_stoke.addConsumer(adob_create_kernel)


                        dob_aw = InMemoryDROP("A_{0}".format(uid), "A_{0}".format(uid), lifespan=lifespan)
                        adob_create_kernel.addOutput(dob_aw)

                        # we do not do loop yet
                        griders = []
                        for m in range(1, num_grid + 1):
                            gid = "%s-%d-%d-%d-%d-%d" % (stoke, i, j, k, l, m)
                            adob_gridding = SleepAndCopyApp("Gridding_{0}".format(gid), "Gridding_{0}".format(gid), lifespan=lifespan)
                            dob_stoke.addConsumer(adob_gridding)
                            dob_gridded_cell = InMemoryDROP("Grided_Cell_{0}".format(gid), "Grided_Cell_{0}".format(gid), lifespan=lifespan)
                            adob_gridding.addOutput(dob_gridded_cell)
                            griders.append(dob_gridded_cell)
                        adob_gridded_bar = SleepAndCopyApp("GRIDDED_BARRIER_{0}".format(uid), "GRIDDED_BARRIER_{0}".format(uid), lifespan=lifespan)
                        for grider in griders:
                            grider.addConsumer(adob_gridded_bar)
                        dob_gridded_stoke = InMemoryDROP("GRIDDED_STOKE_{0}".format(uid), "GRIDDED_STOKE_{0}".format(uid), lifespan=lifespan)
                        adob_gridded_bar.addOutput(dob_gridded_stoke)

                        FFTers = []
                        for m in range(1, num_grid + 1):
                            gid = "%s-%d-%d-%d-%d-%d" % (stoke, i, j, k, l, m)
                            adob_fft = SleepAndCopyApp("FFT_{0}".format(gid), "FFT_{0}".format(gid), lifespan=lifespan)
                            dob_gridded_stoke.addConsumer(adob_fft)

                            dob_ffted_cell = InMemoryDROP("FFTed_Cell_{0}".format(gid), "FFTed_Cell_{0}".format(gid), lifespan=lifespan)
                            adob_fft.addOutput(dob_ffted_cell)
                            FFTers.append(dob_ffted_cell)
                        adob_ffted_bar = SleepAndCopyApp("FFTed_BARRIER_{0}".format(uid), "FFTed_BARRIER_{0}".format(uid), lifespan=lifespan)
                        for ffter in FFTers:
                            ffter.addConsumer(adob_ffted_bar)
                        dob_ffted_stoke = InMemoryDROP("FFTed_STOKE_{0}".format(uid), "FFTed_STOKE_{0}".format(uid), lifespan=lifespan)
                        adob_ffted_bar.addOutput(dob_ffted_stoke)

                        cleaners = []
                        for m in range(1, num_grid + 1):
                            gid = "%s-%d-%d-%d-%d-%d" % (stoke, i, j, k, l, m)
                            adob_cleaner = SleepAndCopyApp("DECONV_{0}".format(gid), "DECONV_{0}".format(gid), lifespan=lifespan)
                            dob_ffted_stoke.addConsumer(adob_cleaner)

                            dob_cleaned_cell = InMemoryDROP("CLEANed_Cell_{0}".format(gid), "CLEANed_Cell_{0}".format(gid), lifespan=lifespan)
                            adob_cleaner.addOutput(dob_cleaned_cell)
                            cleaners.append(dob_cleaned_cell)
                        adob_cleaned_bar = SleepAndCopyApp("CLEANed_BARRIER_{0}".format(uid), "CLEANed_BARRIER_{0}".format(uid), lifespan=lifespan)
                        for cleaner in cleaners:
                            cleaner.addConsumer(adob_cleaned_bar)
                        dob_decon_stoke = InMemoryDROP("CLEANed_STOKE_{0}".format(uid), "CLEANed_STOKE_{0}".format(uid), lifespan=lifespan)
                        adob_cleaned_bar.addOutput(dob_decon_stoke)

                        adob_create_prod = SleepAndCopyApp("CRT-PROD_{0}".format(uid), "CRT-PROD_{0}".format(uid), lifespan=lifespan)
                        dob_decon_stoke.addConsumer(adob_create_prod)

                        dob_prod = InMemoryDROP("PRODUCT_{0}".format(uid), "PRODUCT_{0}".format(uid), lifespan=lifespan)
                        adob_create_prod.addOutput(dob_prod)


    return dob_root_sl

def chiles_pg():

    total_bandwidth = 480
    num_obs = 8 # the same as num of data island
    subband_width = 60 # MHz
    num_subb = total_bandwidth / subband_width
    subband_dict = collections.defaultdict(list) # for corner turning
    img_list = []
    start_freq = 940

    # this should be removed
    roots = []

    for i in range(1, num_obs + 1):
        stri = "%02d" % i
        oid    = "Obs_day_{0}".format(stri)
        appOid = "Receive_" + oid
        dob_obs_sl = SocketListenerApp(oid + "_SL", oid + "_SL", lifespan=lifespan, port=1110+i)
        dob_obs = InMemoryDROP(oid, oid, lifespan=lifespan)
        dob_obs_sl.addOutput(dob_obs)
        dob_obs.location = "{0}.aws-ec2.sydney".format(i)
        roots.append(dob_obs_sl)

        for j in range(1, num_subb + 1):
            app_oid = "mstransform_{0}_{1}".format(stri, "%02d" % j)
            adob_split = SleepAndCopyApp(app_oid, app_oid, lifespan=lifespan)
            adob_split.location = dob_obs.location
            dob_obs.addConsumer(adob_split)

            dob_sboid = "Split_{0}_{1}~{2}MHz".format(stri,
                                                      start_freq + subband_width * j,
                                                      start_freq + subband_width * (j + 1))
            dob_sb = InMemoryDROP(dob_sboid, dob_sboid, lifespan=lifespan)
            dob_sb.location = dob_obs.location
            adob_split.addOutput(dob_sb)

            subband_dict[j].append(dob_sb)

    for j, v in subband_dict.items():
        oid = "Subband_{0}~{1}MHz".format(start_freq + subband_width * j,
                                          start_freq + subband_width * (j + 1))
        appOid = "Combine_" + oid
        dob = InMemoryDROP(oid, oid)
        dob.location = "{0}.aws-ec2.sydney".format(j % num_obs)
        subbandCombineAppDrop = SleepAndCopyApp(appOid, appOid, lifespan=lifespan)
        subbandCombineAppDrop.addOutput(dob)
        for dob_sb in v:
            subbandCombineAppDrop.addInput(dob_sb)

        app_oid = oid.replace("Subband_", "Clean_")
        adob_clean = SleepAndCopyApp(app_oid, app_oid, lifespan=lifespan)
        adob_clean.location = dob.location
        dob.addConsumer(adob_clean)

        img_oid = oid.replace("Subband_", "Image_")
        dob_img = InMemoryDROP(img_oid, img_oid, lifespan=lifespan)
        dob_img.location = dob.location
        adob_clean.addOutput(dob_img)
        img_list.append(dob_img)

    #container + app that produces it
    combineAppDrop = SleepAndCopyApp('Combine', 'Combine')
    dob_comb_img_oid = "Combined_image"
    dob_comb_img = InMemoryDROP(dob_comb_img_oid, dob_comb_img_oid, lifespan=lifespan)
    dob_comb_img.location = "10.1.1.100:7777"
    combineAppDrop.addOutput(dob_comb_img)
    for dob_img in img_list:
        combineAppDrop.addInput(dob_img)

    #concatenate all images
    adob_concat_oid = "Concat_image"
    adob_concat = SleepAndCopyApp(adob_concat_oid, adob_concat_oid, lifespan=lifespan)
    adob_concat.location = dob_comb_img.location
    dob_comb_img.addConsumer(adob_concat)

    # produce cube
    dob_cube_oid = "Cube_Day{0}~{1}_{2}~{3}MHz".format(1,
                                                       num_obs,
                                                       start_freq,
                                                       start_freq + total_bandwidth)

    dob_cube = InMemoryDROP(dob_cube_oid, dob_cube_oid, lifespan=lifespan)
    dob_cube.location = dob_comb_img.location
    adob_concat.addOutput(dob_cube)

    return roots

def listGraphFunctions():
    """
    Returns a generator that iterates over the names of the functions of this
    module that return a DROP graph. Such functions are recognized because
    they accept no arguments at all.
    """
    allNames = dict(globals())
    for name in allNames:
        if name == 'listGraphFunctions': # ourselves
            continue
        sym = globals()[name]
        if inspect.isfunction(sym):
            args, argsl, kwargs, _ = inspect.getargspec(sym)
            if not args and not argsl and not kwargs:
                yield name

if __name__ == '__main__':
    print('Functions eligible for returning graphs:')
    for name in listGraphFunctions():
        print("\t%s" % (name))
