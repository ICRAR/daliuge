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
import random
import time

from dfms import doutils
from dfms.data_object import AppConsumer, InMemoryDataObject, \
    InMemorySocketListenerDataObject, ContainerDataObject, ContainerAppConsumer
from dfms.ddap_protocol import ExecutionMode
from dfms.events.event_broadcaster import ThreadedEventBroadcaster,\
    LocalEventBroadcaster
from _collections import defaultdict
import inspect

"""
A modules that contains several functions returning different "physical graphs",
at this moment represented simply by a number of DataObjects interconnected.

Graph-creator methods must accept no arguments, and must return the starting
point(s) for the graph they create.

All the graphs created in this repository have (up to now) a corresponding
test method in the test_luigi module, where it is automatically verified that
the graph can actually be executed with luigi.
"""

__all__ = ['testGraphDODriven', 'testGraphLuigiDriven', 'testGraphMixed']

# All DOs created in the methods of this module have a lifespan of half an hour.
# This is to allow sufficient time for users to feed data into the initial
# DOs (often a SocketListener DO) and let the graph execute
lifespan = 1800

# All the SleepAndCopyApp DOs below are created by default with a sleeping time
# of up to 4 seconds (randomly generated). If a specific sleeping time needs to
# be used instead (e.g., during automatic tests) the following variable can be
# changed. This value can still be overridden by the per-DO specified sleepTime
# argument which has more precedence
defaultSleepTime = None

#===============================================================================
# Support AppConsumer classes
#===============================================================================
class SleepAndCopyApp(AppConsumer):
    """
    A simple application consumer that sleeps between 0 and 4 seconds (or the
    specified amount of time) and then fully copies the contents of the
    DataObject it consumes into itself. If the DataObject being consumed is a
    ContainerDataObject, the individual contents of the children DataObjects are
    written into this one.
    The copy is two-ways recursive: if the DO being consumed is a Container its
    children are recursively read and their contents concatenated into this DO.
    If this DO is a Container (i.e., a ContainerAppConsumer) the contents of the
    DO being consumed are copied to each of its children.
    """
    def appInitialize(self, *args, **kwargs):
        global defaultSleepTime
        if hasattr(kwargs, 'sleepTime'):
            self._sleepTime = float(kwargs['sleepTime'])
        else:
            if defaultSleepTime is not None:
                self._sleepTime = defaultSleepTime
            else:
                self._sleepTime = random.SystemRandom().randint(0, 400)/100.

    def run(self, dataObject):
        time.sleep(self._sleepTime)
        self.copyRecursive(dataObject)
        if isinstance(self, ContainerAppConsumer):
            for c in self.children: c.setCompleted()
        else:
            self.setCompleted()

    def copyRecursive(self, dataObject):
        if isinstance(dataObject, ContainerDataObject):
            for c in dataObject.children:
                self.copyRecursive(c)
        else:
            if isinstance(self, ContainerAppConsumer):
                for c in self.children:
                    doutils.copyDataObjectContents(dataObject, c)
            else:
                doutils.copyDataObjectContents(dataObject, self)

class InMemorySleepAndCopyApp(SleepAndCopyApp, InMemoryDataObject):
    pass

class SleepAndCopyContainerApp(SleepAndCopyApp, ContainerAppConsumer):
    pass

#===============================================================================
# Methods that create graphs follow, exposed via __all__
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
    A --|--> B3 --> C3 --| --> D
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

    bc = ThreadedEventBroadcaster()
    a = InMemorySocketListenerDataObject('oid:A', 'uid:A', bc, executionMode=aMode, lifespan=lifespan)
    d = ContainerDataObject('oid:D', 'uid:D', bc, executionMode=dMode, lifespan=lifespan)
    for i in xrange(random.SystemRandom().randint(10, 20)):
        b = InMemorySleepAndCopyApp('oid:B%d' % (i), 'uid:B%d' % (i), bc, executionMode=bMode, lifespan=lifespan)
        c = InMemorySleepAndCopyApp('oid:C%d' % (i), 'uid:C%d' % (i), bc, executionMode=cMode, lifespan=lifespan)
        a.addConsumer(b)
        b.addConsumer(c)
        d.addChild(c)
    return a

def test_pg():
    """
    A very simple graph that looks like this:

    A --> B --> C
    """
    eb = LocalEventBroadcaster()
    island_one = "192.168.1.1:7777"
    island_two = "192.168.1.2:7777"
    dobA = InMemorySocketListenerDataObject('Obj-A', 'Obj-A', eb, lifespan=lifespan)
    dobB = InMemorySleepAndCopyApp('Obj-B', 'Obj-B', eb, lifespan=lifespan)
    dobC = InMemorySleepAndCopyApp('Obj-C', 'Obj-C', eb, lifespan=lifespan)

    dobA.location = island_one
    dobB.location = island_one
    dobC.location = island_two

    dobA.addConsumer(dobB)
    dobB.addConsumer(dobC)

    return dobA

def container_pg():
    '''
    Creates the following graph:

           |--> A --|
     one --|        | --> C --> D
           |--> B --|
    '''
    eb = LocalEventBroadcaster()
    island_one = "192.168.1.1:7777"
    island_two = "192.168.1.2:7777"
    dob_one =  InMemorySocketListenerDataObject('Obj-one', 'Obj-one', eb, lifespan=lifespan)
    dobA = InMemorySleepAndCopyApp('Obj-A', 'Obj-A', eb, lifespan=lifespan)
    dobB = InMemorySleepAndCopyApp('Obj-B', 'Obj-B', eb, lifespan=lifespan)
    dobC = ContainerDataObject('Obj-C', 'Obj-C', eb, lifespan=lifespan)
    dobD = InMemorySleepAndCopyApp('Obj-D', 'Obj-D', eb, lifespan=lifespan)

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

def complex_pg():
    return _complex_pg(False)

def complex2_pg():
    return _complex_pg(True)

def _complex_pg(useContainerApps):
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

    se = LocalEventBroadcaster()

    if useContainerApps:
        parentType   = SleepAndCopyContainerApp
        childrenType = InMemoryDataObject
        op = lambda lhs, rhs: lhs.addChild(rhs)
    else:
        parentType   = InMemorySleepAndCopyApp
        childrenType = InMemorySleepAndCopyApp
        op = lambda lhs, rhs: lhs.addConsumer(rhs)

    a =  InMemorySocketListenerDataObject('oid:A', 'uid:A', se, lifespan=lifespan, port=1111)
    b =  InMemorySocketListenerDataObject('oid:B', 'uid:B', se, lifespan=lifespan, port=1112)
    c =  InMemorySocketListenerDataObject('oid:C', 'uid:C', se, lifespan=lifespan, port=1113)
    d =  InMemorySocketListenerDataObject('oid:D', 'uid:D', se, lifespan=lifespan, port=1114)
    e =           InMemorySleepAndCopyApp('oid:E', 'uid:E', se, lifespan=lifespan)
    f =               ContainerDataObject('oid:F', 'uid:F', se, lifespan=lifespan)
    g =               ContainerDataObject('oid:G', 'uid:G', se, lifespan=lifespan)
    h =           InMemorySleepAndCopyApp('oid:H', 'uid:H', se, lifespan=lifespan)
    i =  InMemorySocketListenerDataObject('oid:I', 'uid:I', se, lifespan=lifespan, port=1115)
    j =                        parentType('oid:J', 'uid:J', se, lifespan=lifespan)
    k =               ContainerDataObject('oid:K', 'uid:K', se, lifespan=lifespan)
    l =           InMemorySleepAndCopyApp('oid:L', 'uid:L', se, lifespan=lifespan)
    m =                      childrenType('oid:M', 'uid:M', se, lifespan=lifespan)
    n =                      childrenType('oid:N', 'uid:N', se, lifespan=lifespan)
    o =           InMemorySleepAndCopyApp('oid:O', 'uid:O', se, lifespan=lifespan)
    p =           InMemorySleepAndCopyApp('oid:P', 'uid:P', se, lifespan=lifespan)
    q =               ContainerDataObject('oid:Q', 'uid:Q', se, lifespan=lifespan)

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

def mwa_fornax_pg():
    num_coarse_ch = 24
    num_split = 3 # number of time splits per channel
    se = ThreadedEventBroadcaster()
    dob_root = InMemorySocketListenerDataObject("MWA_LTA", "MWA_LTA", se, lifespan=lifespan)
    dob_root.location = "Pawsey"

    #container
    dob_comb_img_oid = "Combined_image"
    dob_comb_img = ContainerDataObject(dob_comb_img_oid, dob_comb_img_oid, se, lifespan=lifespan)
    dob_comb_img.location = "f032.fornax"

    for i in range(1, num_coarse_ch + 1):
        stri = "%02d" % i
        oid = "Subband_{0}".format(stri)
        dob_obs = ContainerDataObject(oid, oid, se, lifespan=lifespan)
        dob_obs.location = "f%03d.fornax" % i

        oid_ingest = "NGAS_{0}".format(stri)
        dob_ingest = InMemorySleepAndCopyApp(oid_ingest, oid_ingest, se, lifespan=lifespan)
        dob_ingest.location = "f%03d.fornax:7777" % i

        dob_root.addConsumer(dob_ingest)

        for j in range(1, num_split + 1):
            strj = "%02d" % j
            split_oid = "Subband_{0}_Split_{1}".format(stri, strj)
            dob_split = InMemorySleepAndCopyApp(split_oid, split_oid, se, lifespan=lifespan)
            dob_split.location = dob_obs.location
            dob_ingest.addConsumer(dob_split)
            dob_obs.addChild(dob_split)

        oid_rts = "RTS_{0}".format(stri)
        dob_rts = InMemorySleepAndCopyApp(oid_rts, oid_rts, se, lifespan=lifespan)
        dob_rts.location = dob_obs.location
        dob_obs.addConsumer(dob_rts)

        oid_subimg = "Subcube_{0}".format(stri)
        dob_subimg = InMemorySleepAndCopyApp(oid_subimg, oid_subimg, se, lifespan=lifespan)
        dob_subimg.location = dob_obs.location
        dob_rts.addConsumer(dob_subimg)
        dob_comb_img.addChild(dob_subimg)

    #concatenate all images
    adob_concat_oid = "Concat_image"
    adob_concat = InMemorySleepAndCopyApp(adob_concat_oid, adob_concat_oid, se, lifespan=lifespan)
    adob_concat.location = dob_comb_img.location
    dob_comb_img.addConsumer(adob_concat)

    # produce cube
    dob_cube_oid = "Cube_30.72MHz"
    dob_cube = InMemorySleepAndCopyApp(dob_cube_oid, dob_cube_oid, se, lifespan=lifespan)
    dob_cube.location = dob_comb_img.location
    adob_concat.addConsumer(dob_cube)

    return dob_root


def chiles_pg():

    total_bandwidth = 480
    num_obs = 8 # the same as num of data island
    subband_width = 60 # MHz
    num_subb = total_bandwidth / subband_width
    subband_dict = defaultdict(list) # for corner turning
    img_list = []
    start_freq = 940
    eb = ThreadedEventBroadcaster()

    # this should be removed
    dob_root = InMemorySocketListenerDataObject("JVLA", "JVLA", eb, lifespan=lifespan)
    dob_root.location = "NRAO"

    for i in range(1, num_obs + 1):
        stri = "%02d" % i
        oid = "Obs_day_{0}".format(stri)
        dob_obs = InMemorySleepAndCopyApp(oid, oid, eb, lifespan=lifespan)
        dob_obs.location = "{0}.aws-ec2.sydney".format(i)
        dob_root.addConsumer(dob_obs)
        for j in range(1, num_subb + 1):
            app_oid = "mstransform_{0}_{1}".format(stri, "%02d" % j)
            adob_split = InMemorySleepAndCopyApp(app_oid, app_oid, eb, lifespan=lifespan)
            adob_split.location = dob_obs.location
            dob_obs.addConsumer(adob_split)

            dob_sboid = "Split_{0}_{1}~{2}MHz".format(stri,
                                                      start_freq + subband_width * j,
                                                      start_freq + subband_width * (j + 1))
            dob_sb = InMemorySleepAndCopyApp(dob_sboid, dob_sboid, eb, lifespan=lifespan)
            dob_sb.location = dob_obs.location
            adob_split.addConsumer(dob_sb)

            subband_dict[j].append(dob_sb)

    for j, v in subband_dict.items():
        oid = "Subband_{0}~{1}MHz".format(start_freq + subband_width * j,
                                          start_freq + subband_width * (j + 1))
        dob = ContainerDataObject(oid, oid, eb)
        dob.location = "{0}.aws-ec2.sydney".format(j % num_obs)
        for dob_sb in v:
            dob.addChild(dob_sb)

        app_oid = oid.replace("Subband_", "Clean_")
        adob_clean = InMemorySleepAndCopyApp(app_oid, app_oid, eb, lifespan=lifespan)
        adob_clean.location = dob.location
        dob.addConsumer(adob_clean)

        img_oid = oid.replace("Subband_", "Image_")
        dob_img = InMemorySleepAndCopyApp(img_oid, img_oid, eb, lifespan=lifespan)
        dob_img.location = dob.location
        adob_clean.addConsumer(dob_img)
        img_list.append(dob_img)

    #container
    dob_comb_img_oid = "Combined_image"
    dob_comb_img = ContainerDataObject(dob_comb_img_oid, dob_comb_img_oid, eb, lifespan=lifespan)
    dob_comb_img.location = "10.1.1.100:7777"
    for dob_img in img_list:
        dob_comb_img.addChild(dob_img)

    #concatenate all images
    adob_concat_oid = "Concat_image"
    adob_concat = InMemorySleepAndCopyApp(adob_concat_oid, adob_concat_oid, eb, lifespan=lifespan)
    adob_concat.location = dob_comb_img.location
    dob_comb_img.addConsumer(adob_concat)

    # produce cube
    dob_cube_oid = "Cube_Day{0}~{1}_{2}~{3}MHz".format(1,
                                                       num_obs,
                                                       start_freq,
                                                       start_freq + total_bandwidth)

    dob_cube = InMemorySleepAndCopyApp(dob_cube_oid, dob_cube_oid, eb, lifespan=lifespan)
    dob_cube.location = dob_comb_img.location
    adob_concat.addConsumer(dob_cube)

    return dob_root

def listGraphFunctions():
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