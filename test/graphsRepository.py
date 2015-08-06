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
        for outputDO in self._outputs.values():
            outputDO.setCompleted()

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
        for outputDO in outputs:
            outputDO.setCompleted()

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
    e.producer = d
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
