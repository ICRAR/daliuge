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
at this moment represented simply by a number of DataObjects interconnected.

Graph-creator methods must accept no arguments, and must return the starting
point(s) for the graph they create.

All the graphs created in this repository have (up to now) a corresponding
test method in the test_luigi module, where it is automatically verified that
the graph can actually be executed with luigi.
"""
# All DOs created in the methods of this module have a lifespan of half an hour.
# This is to allow sufficient time for users to feed data into the initial
# DOs (often a SocketListener DO) and let the graph execute

import collections
import inspect
import random
import time

from dfms import doutils
from dfms.data_object import InMemoryDataObject, \
    InMemorySocketListenerDataObject, BarrierAppDataObject, ContainerDataObject
from dfms.ddap_protocol import ExecutionMode
from test.test_data_object import SumupContainerChecksum


lifespan = 1800

# All the SleepAndCopyApp DOs below are created by default with a sleeping time
# of up to 4 seconds (randomly generated). If a specific sleeping time needs to
# be used instead (e.g., during automatic tests) the following variable can be
# changed. This value can still be overridden by the per-DO specified sleepTime
# argument which has more precedence
defaultSleepTime = None

#===============================================================================
# Support AppDataObject classes
#===============================================================================
class SimpleBarrierApp(BarrierAppDataObject):
    def run(self):
        pass

class SleepAndCopyApp(BarrierAppDataObject):
    """
    A simple application consumer that sleeps between 0 and 4 seconds (or the
    specified amount of time) and then fully copies the contents of the
    DataObject it consumes into each of the DataObjects it writes to. If there
    are more than one DataObject being consumed, the individual contents of each
    DataObjects are written into each output.
    """
    def initialize(self, **kwargs):
        super(SleepAndCopyApp, self).initialize(**kwargs)
        global defaultSleepTime
        if kwargs.has_key('sleepTime'):
            self._sleepTime = float(kwargs['sleepTime'])
        else:
            if defaultSleepTime is not None:
                self._sleepTime = defaultSleepTime
            else:
                self._sleepTime = random.SystemRandom().randint(0, 400)/100.

    def run(self):
        time.sleep(self._sleepTime)
        self.copyAll()

    def copyAll(self):
        inputs  = self._inputs.values()
        outputs = self._outputs.values()
        for inputDO in inputs:
            self.copyRecursive(inputDO, outputs)

    def copyRecursive(self, inputDO, outputs):
        if isinstance(inputDO, ContainerDataObject):
            for child in inputDO.children:
                self.copyRecursive(child, outputs)
        else:
            for outputDO in outputs:
                doutils.copyDataObjectContents(inputDO, outputDO)

#===============================================================================
# Methods that create graphs follow. They must have no arguments to be
# recognized as such
#===============================================================================
def testGraphDODriven():
    return _testGraph(ExecutionMode.DO)

def testGraphLuigiDriven():
    return _testGraph(ExecutionMode.EXTERNAL)

def testGraphMixed():
    return _testGraph(None)

def _testGraph(execMode):
    """
    A test graph that looks like this:

        |--> B1 --> C1 --|
        |--> B2 --> C2 --|
    A --|--> B3 --> C3 --| --> D --> E
        |--> .. --> .. --|
        |--> BN --> CN --|

    B and C DOs are InMemorySleepAndCopyApp DOs (see above). D is simply a
    container. A is a socket listener, so we can actually write to it externally
    and watch the progress of the luigi tasks. We give DOs a long lifespan;
    otherwise they will expire and luigi will see it as a failed task (which is
    actually right!)

    If execMode is given we use that in all DOs. If it's None we use a mixture
    of DO/EXTERNAL execution modes.
    """

    aMode = execMode if execMode is not None else ExecutionMode.EXTERNAL
    bMode = execMode if execMode is not None else ExecutionMode.DO
    cMode = execMode if execMode is not None else ExecutionMode.DO
    dMode = execMode if execMode is not None else ExecutionMode.EXTERNAL
    eMode = execMode if execMode is not None else ExecutionMode.EXTERNAL

    a = InMemorySocketListenerDataObject('oid:A', 'uid:A', executionMode=aMode, lifespan=lifespan)
    d =           SumupContainerChecksum('oid:D', 'uid:D', executionMode=dMode, lifespan=lifespan)
    e =               InMemoryDataObject('oid:E', 'uid:E', executionMode=eMode, lifespan=lifespan)
    e.addProducer(d)
    for i in xrange(random.SystemRandom().randint(10, 20)):
        b =    SleepAndCopyApp('oid:B%d' % (i), 'uid:B%d' % (i), executionMode=bMode, lifespan=lifespan)
        c = InMemoryDataObject('oid:C%d' % (i), 'uid:C%d' % (i), executionMode=cMode, lifespan=lifespan)
        a.addConsumer(b)
        b.addOutput(c)
        c.addConsumer(d)
    return a

def test_pg():
    """
    A very simple graph that looks like this:

    A --> B --> C
    """
    island_one = "192.168.1.1:7777"
    island_two = "192.168.1.2:7777"
    dobA = InMemorySocketListenerDataObject('Obj-A', 'Obj-A', lifespan=lifespan)
    dobB = SleepAndCopyApp('Obj-B', 'Obj-B', lifespan=lifespan)
    dobC = InMemoryDataObject('Obj-C', 'Obj-C', lifespan=lifespan)

    dobA.location = island_one
    dobB.location = island_one
    dobC.location = island_two

    dobA.addConsumer(dobB)
    dobB.addOutput(dobC)

    return dobA

def container_pg():
    '''
    Creates the following graph:

         |--> B --> D --|
     A --|              |--> F --> G
         |--> C --> E --|
    '''
    a = InMemorySocketListenerDataObject('A', 'A', lifespan=lifespan)
    b = SleepAndCopyApp('B', 'B', lifespan=lifespan)
    c = SleepAndCopyApp('C', 'C', lifespan=lifespan)
    d = InMemoryDataObject('D', 'D', lifespan=lifespan)
    e = InMemoryDataObject('E', 'E', lifespan=lifespan)
    f = SimpleBarrierApp('F', 'F', lifespan=lifespan)
    g = ContainerDataObject('G', 'G', lifespan=lifespan)
    h = SleepAndCopyApp('H', 'H', lifespan=lifespan)
    i = InMemoryDataObject('I', 'I', lifespan=lifespan)

    # Wire together
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

    return a

def complex_graph():
    """
    This method creates the following graph

    A -----> E --> G --|                         |--> N ------> Q --> S
                       |              |---> L ---|
    B -----------------|--> I --> J --|          |--> O -->|
                       |              |                    |--> R --> T
    C --|              |              |---> M ------> P -->|
        |--> F --> H --|                |
    D --|                         K ----|

    In this example the "leaves" of the graph are R and T, while the
    "roots" are A, B, C, D and I.

    E, F, I, L, M, Q and R are AppDataObjects; A, B, C, D and K are plain DOs
    that listen in a socket. The rest are plain in-memory DOs
    """

    a =  InMemorySocketListenerDataObject('a', 'a', lifespan=lifespan, port=1111)
    b =  InMemorySocketListenerDataObject('b', 'b', lifespan=lifespan, port=1112)
    c =  InMemorySocketListenerDataObject('c', 'c', lifespan=lifespan, port=1113)
    d =  InMemorySocketListenerDataObject('d', 'd', lifespan=lifespan, port=1114)
    k =  InMemorySocketListenerDataObject('k', 'k', lifespan=lifespan, port=1115)
    e, f, i, l, m, q, r    = [   SleepAndCopyApp(uid, uid, lifespan=lifespan) for uid in ['e', 'f', 'i', 'l', 'm', 'q', 'r']]
    g, h, j, n, o, p, s, t = [InMemoryDataObject(uid, uid, lifespan=lifespan) for uid in ['g', 'h', 'j', 'n', 'o', 'p', 's', 't']]

    for plainDO, appDO in [(a,e), (b,i), (c,f), (d,f), (h,i), (j,l), (j,m), (k,m), (n,q), (o,r), (p,r)]:
        plainDO.addConsumer(appDO)
    for appDO, plainDO in [(e,g), (f,h), (i,j), (l,n), (l,o), (m,p), (q,s), (r,t)]:
        appDO.addOutput(plainDO)

    return [a, b, c, d, k]

def mwa_fornax_pg():
    num_coarse_ch = 24
    num_split = 3 # number of time splits per channel
    dob_root = InMemorySocketListenerDataObject("MWA_LTA", "MWA_LTA", lifespan=lifespan)
    dob_root.location = "Pawsey"

    #container
    dob_comb_img_oid = "Combined_image"
    combineImgAppOid = "Combine image"
    dob_comb_img = InMemoryDataObject(dob_comb_img_oid, dob_comb_img_oid, lifespan=lifespan)
    combineImgApp = SleepAndCopyApp(combineImgAppOid, combineImgAppOid)
    combineImgApp.addOutput(dob_comb_img)
    dob_comb_img.location = "f032.fornax"

    for i in range(1, num_coarse_ch + 1):

        stri = "%02d" % i
        oid = "Subband_{0}".format(stri)
        appOid = "Combine_" + oid
        dob_obs = InMemoryDataObject(oid, oid, lifespan=lifespan)
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
            dob_split = InMemoryDataObject(split_oid, split_oid, lifespan=lifespan)
            dob_split.location = dob_obs.location
            dob_ingest.addOutput(dob_split)
            combineSubbandApp.addInput(dob_split)

        oid_rts = "RTS_{0}".format(stri)
        dob_rts = SleepAndCopyApp(oid_rts, oid_rts, lifespan=lifespan)
        dob_rts.location = dob_obs.location
        dob_obs.addConsumer(dob_rts)

        oid_subimg = "Subcube_{0}".format(stri)
        dob_subimg = InMemoryDataObject(oid_subimg, oid_subimg, lifespan=lifespan)
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
    dob_cube = InMemoryDataObject(dob_cube_oid, dob_cube_oid, lifespan=lifespan)
    dob_cube.location = dob_comb_img.location
    adob_concat.addOutput(dob_cube)

    return dob_root

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

    Cal_model_oid = "A-team/Calibrator"
    dob_cal_model = InMemoryDataObject(Cal_model_oid, Cal_model_oid, lifespan=lifespan)
    dob_cal_model.location = "catalogue.Groningen"
    roots.append(dob_cal_model)

    make_source_app_oid = "makesourcedb"
    make_source_app = SleepAndCopyApp(make_source_app_oid, make_source_app_oid)
    make_source_app.location = dob_cal_model.location
    dob_cal_model.addConsumer(make_source_app)

    source_db_oid = "cal source.db"
    dob_source_db = InMemoryDataObject(source_db_oid, source_db_oid, lifespan=lifespan)
    dob_source_db.location = dob_cal_model.location
    make_source_app.addOutput(dob_source_db)

    num_time_slice = 3
    total_num_subband = 8
    num_subb_per_image = 2

    dob_img_dict = dict()
    sb_chunks = chunks(range(1, total_num_subband + 1), num_subb_per_image)
    for k, img_list in enumerate(sb_chunks):
        imger_oid = "AWIMG_SB_{0}~{1}".format(img_list[0], img_list[-1])
        dob_img = SleepAndCopyApp(imger_oid, imger_oid, lifespan=lifespan)
        kk = k + 1
        print k, img_list, kk
        dob_img_dict[kk] = dob_img

    for j in range(1, num_time_slice + 1):
        sli = "T%02d" % j
        for i in range(1, total_num_subband + 1):
            # for LBA, assume each subband should have two beams
            # one for calibration pipeline, the other for target / imaging pipeline
            stri = "%s_SB%02d" % (sli, i)
            time_slice_oid = "Socket_{0}".format(stri)
            dob_time_slice = InMemorySocketListenerDataObject(time_slice_oid, time_slice_oid, lifespan=lifespan)
            # all the same sub-bands (i) will be on the same node regardless of its time slice j
            dob_time_slice.location = "Node%03d.Groningen" % i
            roots.append(dob_time_slice)

            oid_data_writer = "DataWriter_{0}".format(stri)
            dob_ingest = SleepAndCopyApp(oid_data_writer, oid_data_writer, lifespan=lifespan)
            dob_ingest.location = dob_time_slice.location
            dob_time_slice.addConsumer(dob_ingest)

            # assume a single measuremenset has multiple beams
            oid = "CalNTgt.MS_{0}".format(stri)
            dob_input = InMemoryDataObject(oid, oid, lifespan=lifespan)
            dob_ingest.addOutput(dob_input)
            dob_input.location = dob_ingest.location

            # For calibration, each time slice is calibrated independently, and
            # the result is an output MS per subband
            oid = "FlgAvg.MS_{0}".format(stri)
            appOid = "NDPPP_" + stri
            dob_flagged = InMemoryDataObject(oid, oid, lifespan=lifespan)
            flagApp = SleepAndCopyApp(appOid, appOid, lifespan=lifespan)
            dob_input.addConsumer(flagApp)
            dob_source_db.addConsumer(flagApp)
            flagApp.addOutput(dob_flagged)
            dob_flagged.location = dob_ingest.location #"C%03d.Groningen" % i

            # solve the gain
            oid = "Gain.TBL_{0}".format(stri)
            appOid = "BBS_" + oid
            dob_param_tbl = InMemoryDataObject(oid, oid, lifespan=lifespan)
            applyCalApp = SleepAndCopyApp(appOid, appOid, lifespan=lifespan)
            dob_flagged.addConsumer(applyCalApp)
            applyCalApp.addOutput(dob_param_tbl)
            dob_param_tbl.location = dob_ingest.location

            # or apply the gain to the dataset
            oid = "CAL.MS_{0}".format(stri)
            appOid = "BBS_" + oid
            dob_calibrated = InMemoryDataObject(oid, oid, lifespan=lifespan)
            applyCalApp = SleepAndCopyApp(appOid, appOid, lifespan=lifespan)
            dob_flagged.addConsumer(applyCalApp)
            dob_param_tbl.addConsumer(applyCalApp)
            applyCalApp.addOutput(dob_calibrated)
            dob_calibrated.location = dob_ingest.location

            img_k = (i - 1) / num_subb_per_image + 1
            dob_img = dob_img_dict[img_k]
            dob_calibrated.addConsumer(dob_img)
            dob_img.location = dob_calibrated.location #overwrite to the last i

        """
        oid = "DIRTY.IMG_{0}".format(stri)
        appOid = "AWIMAGER_" + oid
        dob_dirty_img = InMemoryDataObject(oid, oid, lifespan=lifespan)
        dirtyImagerApp = SleepAndCopyApp(appOid, appOid, lifespan=lifespan)
        dob_calibrated.addConsumer(dirtyImagerApp)
        dirtyImagerApp.addOutput(dob_dirty_img)
        dob_dirty_img.location = dob_ingest.location

        oid = "CLEAN.IMG_{0}".format(stri)
        appOid = "QA_n_AWIMAGER_" + oid
        dob_clean_img = InMemoryDataObject(oid, oid, lifespan=lifespan)
        cleanImagerApp =  SleepAndCopyApp(appOid, appOid, lifespan=lifespan)
        dob_dirty_img.addConsumer(cleanImagerApp)
        dob_calibrated.addConsumer(cleanImagerApp)
        cleanImagerApp.addOutput(dob_clean_img)
        dob_clean_img.location = dob_ingest.location

            combineImgApp.addInput(dob_clean_img)
        """
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
    dob_root = InMemorySocketListenerDataObject("FullDataset", "FullDataset", lifespan=lifespan)
    dob_root.location = "Buf01"

    adob_sp_beam = SleepAndCopyApp("SPLT_BEAM", "SPLT_BEAM", lifespan=lifespan)
    adob_sp_beam.location = "Buf01"
    dob_root.addConsumer(adob_sp_beam)

    for i in range(1, num_beam + 1):
        id = i
        dob_beam = InMemoryDataObject("BEAM_{0}".format(id), "BEAM_{0}".format(id), lifespan=lifespan)
        adob_sp_beam.addOutput(dob_beam)
        adob_sp_time = SleepAndCopyApp("SPLT_TIME_{0}".format(id), "SPLT_TIME_{0}".format(id), lifespan=lifespan)
        dob_beam.addConsumer(adob_sp_time)
        for j in range(1, num_time + 1):
            id = "%d-%d" % (i, j)
            dob_time = InMemoryDataObject("TIME_{0}".format(id), "TIME_{0}".format(id), lifespan=lifespan)
            adob_sp_time.addOutput(dob_time)
            adob_sp_freq = SleepAndCopyApp("SPLT_FREQ_{0}".format(id), "SPLT_FREQ_{0}".format(id), lifespan=lifespan)
            dob_time.addConsumer(adob_sp_freq)
            for k in range(1, num_freq + 1):
                id = "%d-%d-%d" % (i, j, k)
                dob_freq = InMemoryDataObject("FREQ_{0}".format(id), "FREQ_{0}".format(id), lifespan=lifespan)
                adob_sp_freq.addOutput(dob_freq)
                adob_sp_facet = SleepAndCopyApp("SPLT_FACET_{0}".format(id), "SPLT_FACET_{0}".format(id), lifespan=lifespan)
                dob_freq.addConsumer(adob_sp_facet)
                for l in range(1, num_facet + 1):
                    id = "%d-%d-%d-%d" % (i, j, k, l)
                    dob_facet = InMemoryDataObject("FACET_{0}".format(id), "FACET_{0}".format(id), lifespan=lifespan)
                    adob_sp_facet.addOutput(dob_facet)

                    adob_ph_rot = SleepAndCopyApp("PH_ROTATN_{0}".format(id), "PH_ROTATN_{0}".format(id), lifespan=lifespan)
                    dob_facet.addConsumer(adob_ph_rot)
                    dob_ph_rot = InMemoryDataObject("PH_ROTD_{0}".format(id), "PH_ROTD_{0}".format(id), lifespan=lifespan)
                    adob_ph_rot.addOutput(dob_ph_rot)
                    adob_sp_stokes = SleepAndCopyApp("SPLT_STOKES_{0}".format(id), "SPLT_STOKES_{0}".format(id), lifespan=lifespan)
                    dob_ph_rot.addConsumer(adob_sp_stokes)

                    adob_w_kernel = SleepAndCopyApp("CACL_W_Knl_{0}".format(id), "CACL_W_Knl_{0}".format(id), lifespan=lifespan)
                    dob_facet.addConsumer(adob_w_kernel)
                    dob_w_knl = InMemoryDataObject("W_Knl_{0}".format(id), "W_Knl_{0}".format(id), lifespan=lifespan)
                    adob_w_kernel.addOutput(dob_w_knl)

                    adob_a_kernel = SleepAndCopyApp("CACL_A_Knl_{0}".format(id), "CACL_A_Knl_{0}".format(id), lifespan=lifespan)
                    dob_facet.addConsumer(adob_a_kernel)
                    dob_a_knl = InMemoryDataObject("A_Knl_{0}".format(id), "A_Knl_{0}".format(id), lifespan=lifespan)
                    adob_a_kernel.addOutput(dob_a_knl)

                    adob_create_kernel = SleepAndCopyApp("CREATE_Knl_{0}".format(id), "CREATE_Knl_{0}".format(id), lifespan=lifespan)
                    dob_w_knl.addConsumer(adob_create_kernel)
                    dob_a_knl.addConsumer(adob_create_kernel)


                    for stoke in stokes:
                        id = "%s-%d-%d-%d-%d" % (stoke, i, j, k, l)
                        #print "id = {0}".format(id)

                        dob_stoke = InMemoryDataObject("STOKE_{0}".format(id), "STOKE_{0}".format(id), lifespan=lifespan)
                        adob_sp_stokes.addOutput(dob_stoke)

                        dob_stoke.addConsumer(adob_create_kernel)


                        dob_aw = InMemoryDataObject("A_{0}".format(id), "A_{0}".format(id), lifespan=lifespan)
                        adob_create_kernel.addOutput(dob_aw)

                        # we do not do loop yet
                        griders = []
                        for m in range(1, num_grid + 1):
                            gid = "%s-%d-%d-%d-%d-%d" % (stoke, i, j, k, l, m)
                            adob_gridding = SleepAndCopyApp("Gridding_{0}".format(gid), "Gridding_{0}".format(gid), lifespan=lifespan)
                            dob_stoke.addConsumer(adob_gridding)
                            dob_gridded_cell = InMemoryDataObject("Grided_Cell_{0}".format(gid), "Grided_Cell_{0}".format(gid), lifespan=lifespan)
                            adob_gridding.addOutput(dob_gridded_cell)
                            griders.append(dob_gridded_cell)
                        adob_gridded_bar = SleepAndCopyApp("GRIDDED_BARRIER_{0}".format(id), "GRIDDED_BARRIER_{0}".format(id), lifespan=lifespan)
                        for grider in griders:
                            grider.addConsumer(adob_gridded_bar)
                        dob_gridded_stoke = InMemoryDataObject("GRIDDED_STOKE_{0}".format(id), "GRIDDED_STOKE_{0}".format(id), lifespan=lifespan)
                        adob_gridded_bar.addOutput(dob_gridded_stoke)

                        FFTers = []
                        for m in range(1, num_grid + 1):
                            gid = "%s-%d-%d-%d-%d-%d" % (stoke, i, j, k, l, m)
                            adob_fft = SleepAndCopyApp("FFT_{0}".format(gid), "FFT_{0}".format(gid), lifespan=lifespan)
                            dob_gridded_stoke.addConsumer(adob_fft)

                            dob_ffted_cell = InMemoryDataObject("FFTed_Cell_{0}".format(gid), "FFTed_Cell_{0}".format(gid), lifespan=lifespan)
                            adob_fft.addOutput(dob_ffted_cell)
                            FFTers.append(dob_ffted_cell)
                        adob_ffted_bar = SleepAndCopyApp("FFTed_BARRIER_{0}".format(id), "FFTed_BARRIER_{0}".format(id), lifespan=lifespan)
                        for ffter in FFTers:
                            ffter.addConsumer(adob_ffted_bar)
                        dob_ffted_stoke = InMemoryDataObject("FFTed_STOKE_{0}".format(id), "FFTed_STOKE_{0}".format(id), lifespan=lifespan)
                        adob_ffted_bar.addOutput(dob_ffted_stoke)

                        cleaners = []
                        for m in range(1, num_grid + 1):
                            gid = "%s-%d-%d-%d-%d-%d" % (stoke, i, j, k, l, m)
                            adob_cleaner = SleepAndCopyApp("DECONV_{0}".format(gid), "DECONV_{0}".format(gid), lifespan=lifespan)
                            dob_ffted_stoke.addConsumer(adob_cleaner)

                            dob_cleaned_cell = InMemoryDataObject("CLEANed_Cell_{0}".format(gid), "CLEANed_Cell_{0}".format(gid), lifespan=lifespan)
                            adob_cleaner.addOutput(dob_cleaned_cell)
                            cleaners.append(dob_cleaned_cell)
                        adob_cleaned_bar = SleepAndCopyApp("CLEANed_BARRIER_{0}".format(id), "CLEANed_BARRIER_{0}".format(id), lifespan=lifespan)
                        for cleaner in cleaners:
                            cleaner.addConsumer(adob_cleaned_bar)
                        dob_decon_stoke = InMemoryDataObject("CLEANed_STOKE_{0}".format(id), "CLEANed_STOKE_{0}".format(id), lifespan=lifespan)
                        adob_cleaned_bar.addOutput(dob_decon_stoke)

                        adob_create_prod = SleepAndCopyApp("CRT-PROD_{0}".format(id), "CRT-PROD_{0}".format(id), lifespan=lifespan)
                        dob_decon_stoke.addConsumer(adob_create_prod)

                        dob_prod = InMemoryDataObject("PRODUCT_{0}".format(id), "PRODUCT_{0}".format(id), lifespan=lifespan)
                        adob_create_prod.addOutput(dob_prod)


    return dob_root

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
        dob_obs = InMemorySocketListenerDataObject(oid, oid, lifespan=lifespan, port=1110+i)
        dob_obs.location = "{0}.aws-ec2.sydney".format(i)
        roots.append(dob_obs)

        for j in range(1, num_subb + 1):
            app_oid = "mstransform_{0}_{1}".format(stri, "%02d" % j)
            adob_split = SleepAndCopyApp(app_oid, app_oid, lifespan=lifespan)
            adob_split.location = dob_obs.location
            dob_obs.addConsumer(adob_split)

            dob_sboid = "Split_{0}_{1}~{2}MHz".format(stri,
                                                      start_freq + subband_width * j,
                                                      start_freq + subband_width * (j + 1))
            dob_sb = InMemoryDataObject(dob_sboid, dob_sboid, lifespan=lifespan)
            dob_sb.location = dob_obs.location
            adob_split.addOutput(dob_sb)

            subband_dict[j].append(dob_sb)

    for j, v in subband_dict.items():
        oid = "Subband_{0}~{1}MHz".format(start_freq + subband_width * j,
                                          start_freq + subband_width * (j + 1))
        appOid = "Combine_" + oid
        dob = InMemoryDataObject(oid, oid)
        dob.location = "{0}.aws-ec2.sydney".format(j % num_obs)
        subbandCombineAppDo = SleepAndCopyApp(appOid, appOid, lifespan=lifespan)
        subbandCombineAppDo.addOutput(dob)
        for dob_sb in v:
            subbandCombineAppDo.addInput(dob_sb)

        app_oid = oid.replace("Subband_", "Clean_")
        adob_clean = SleepAndCopyApp(app_oid, app_oid, lifespan=lifespan)
        adob_clean.location = dob.location
        dob.addConsumer(adob_clean)

        img_oid = oid.replace("Subband_", "Image_")
        dob_img = InMemoryDataObject(img_oid, img_oid, lifespan=lifespan)
        dob_img.location = dob.location
        adob_clean.addOutput(dob_img)
        img_list.append(dob_img)

    #container + app that produces it
    combineAppDo = SleepAndCopyApp('Combine', 'Combine')
    dob_comb_img_oid = "Combined_image"
    dob_comb_img = InMemoryDataObject(dob_comb_img_oid, dob_comb_img_oid, lifespan=lifespan)
    dob_comb_img.location = "10.1.1.100:7777"
    combineAppDo.addOutput(dob_comb_img)
    for dob_img in img_list:
        combineAppDo.addInput(dob_img)

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

    dob_cube = InMemoryDataObject(dob_cube_oid, dob_cube_oid, lifespan=lifespan)
    dob_cube.location = dob_comb_img.location
    adob_concat.addOutput(dob_cube)

    return roots

def listGraphFunctions():
    """
    Returns a generator that iterates over the names of the functions of this
    module that return a DataObject graph. Such functions are recognized because
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
    print 'Functions eligible for returning graphs:'
    for name in listGraphFunctions():
        print "\t%s" % (name)
