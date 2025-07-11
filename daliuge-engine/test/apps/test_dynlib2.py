#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2020
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
import functools
import io
import os
import time
import unittest

from dlg import droputils
from dlg.apps.dynlib import DynlibApp, DynlibStreamApp, DynlibProcApp
from dlg.ddap_protocol import DROPRel, DROPLinkType, DROPStates
from dlg.data.drops.data_base import NullDROP
from dlg.data.drops.memory import InMemoryDROP

from test.dlg_engine_testutils import NMTestsMixIn

from .setp_up import build_shared_library
from ..manager import test_dm

_libname = "dynlib_example2"
_libfname = "libdynlib_example2.so"
_libpath = os.path.join(os.path.dirname(__file__), _libfname)
print_stats = False
bufsize = 10 * 1024 * 1024


@unittest.skipUnless(
    build_shared_library(_libname, _libpath),
    "Example dynamic library not available",
)
class DynlibAppTest(unittest.TestCase):
    def test_simple_batch_copy(self):
        self._test_simple_copy(False)

    def test_simple_stream_copy(self):
        self._test_simple_copy(True)

    def _test_simple_copy(self, streaming):
        """
        Checks that the following graph works, both in streaming and batch mode:

        A ----> B ----> C
           |       +--> D
           |
           +--> E ----> F
                   +--> G

        Both B and E use the same dynamically loaded library and work with their
        input A to copy the inputs into their outputs.
        """

        # Build the graph
        a = (NullDROP if streaming else InMemoryDROP)("a", "a")
        b, e = (
            (DynlibStreamApp if streaming else DynlibApp)(
                x, x, lib=_libpath, print_stats=print_stats, bufsize=bufsize
            )
            for x in ("b", "e")
        )
        c, d, f, g = (InMemoryDROP(x, x) for x in ("c", "d", "f", "g"))
        for app, outputs in (b, (c, d)), (e, (f, g)):
            (app.addStreamingInput if streaming else app.addInput)(a)
            for o in outputs:
                app.addOutput(o)

        # ~100 MBs of data should be copied over from a to c and d via b, etc
        data = os.urandom(1024 * 1024) * 100
        reader = io.BytesIO(data)
        with droputils.DROPWaiterCtx(self, (c, d, f, g), 10):
            if streaming:
                # Write the data in chunks so we actually exercise multiple calls
                # to the data_written library call
                for datum in iter(
                    functools.partial(reader.read, 1024 * 1024), b""
                ):
                    a.write(datum)
            else:
                a.write(data)
            a.setCompleted()

        for drop in (c, d, f, g):
            drop_data = droputils.allDropContents(drop)
            self.assertEqual(
                len(data),
                len(drop_data),
                "Data from %r is not what we wanted :(" % (drop,),
            )
            self.assertEqual(data, drop_data)

    def test_cancel_dynlibprocapp(self):
        """Checks that we can cancel a long-running dynlib proc app"""

        a = DynlibProcApp("a", "a", lib=_libpath, sleep_seconds=10)
        # we don't really end up using it, so it can be anything
        dummy_rpc_endpoint = (None, None)
        a.rpc_endpoint = dummy_rpc_endpoint
        with droputils.DROPWaiterCtx(self, (), timeout=0):
            a.async_execute()

        time.sleep(1)
        t0 = time.time()
        a.cancel()
        self.assertLess(
            time.time() - t0,
            1,
            "Cancelled dynlibprocapp in less than a second",
        )
        self.assertEqual(DROPStates.CANCELLED, a.status)


class IntraNMMixIng(NMTestsMixIn):
    # Indicate which particular application should the test use
    app = None

    def test_input_in_remote_nm(self):
        """
        A test similar in spirit to TestDM.test_runGraphOneDOPerDom, but where
        application B is a DynlibApp. This makes sure that DynlibApps work fine
        across Node Managers.

        NM #1      NM #2
        =======    =============
        | A --|----|-> B --> C |
        =======    =============
        """
        g1 = [
            {
                "oid": "A",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
            }
        ]
        g2 = [
            {
                "oid": "B",
                "categoryType": "Application",
                "dropclass": self.app,
                "lib": _libpath,
                "print_stats": print_stats,
                "bufsize": bufsize,
            },
            {
                "oid": "C",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "producers": ["B"],
            },
        ]
        rels = [DROPRel("A", DROPLinkType.INPUT, "B")]
        a_data = os.urandom(32)
        self._test_runGraphInTwoNMs(g1, g2, rels, a_data, a_data)

    def test_output_in_remote_nm(self):
        """
        Like the above, but with this graph. In this case the output (instead of
        the input) is in a remote Node Manager.

        NM #1            NM #2
        =============    =======
        | A --> B --|----|-> C |
        =============    =======
        """
        g1 = [
            {
                "oid": "A",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "consumers": ["B"],
            },
            {
                "oid": "B",
                "categoryType": "Application",
                "dropclass": self.app,
                "lib": _libpath,
                "print_stats": print_stats,
                "bufsize": bufsize,
            },
        ]
        g2 = [
            {
                "oid": "C",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
            }
        ]
        rels = [DROPRel("B", DROPLinkType.PRODUCER, "C")]
        a_data = os.urandom(32)
        self._test_runGraphInTwoNMs(g1, g2, rels, a_data, a_data)

    def test_multiple_inputs_in_remote_nm(self):
        """Like the above, but with this graph. In this case two inputs are
        located in a remote Node Manager.

        NM #1      NM #2
        =======    ===============
        | A --|----|-|           |
        |     |    | |-> C --> D |
        | B --|----|-|           |
        =======    ===============
        """
        g1 = [
            {
                "oid": "A",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
            },
            {
                "oid": "B",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
            },
        ]
        g2 = [
            {
                "oid": "C",
                "categoryType": "Application",
                "dropclass": self.app,
                "lib": _libpath,
                "print_stats": print_stats,
                "bufsize": bufsize,
            },
            {
                "oid": "D",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "producers": ["C"],
            },
        ]
        rels = [
            DROPRel("A", DROPLinkType.INPUT, "C"),
            DROPRel("B", DROPLinkType.INPUT, "C"),
        ]
        a_data = os.urandom(32)
        self._test_runGraphInTwoNMs(
            g1,
            g2,
            rels,
            a_data,
            a_data * 2,
            root_oids=("A", "B"),
            leaf_oid="D",
        )


@unittest.skipUnless(
    build_shared_library(_libname, _libpath),
    "Example dynamic library not available",
)
class IntraNMDynlibAppTest(IntraNMMixIng, unittest.TestCase):
    app = "dlg.apps.dynlib.DynlibApp"


@unittest.skipUnless(
    build_shared_library(_libname, _libpath),
    "Example dynamic library not available",
)
class IntraNMDynlibProcAppTest(IntraNMMixIng, unittest.TestCase):
    app = "dlg.apps.dynlib.DynlibProcApp"

    def test_crashing_dynlib(self):
        """Like test_multiple_inputs_in_remote_nm, but C crashes"""
        g1 = [
            {
                "oid": "A",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
            },
            {
                "oid": "B",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
            },
        ]
        g2 = [
            {
                "oid": "C",
                "categoryType": "Application",
                "dropclass": self.app,
                "lib": _libpath,
                "print_stats": print_stats,
                "bufsize": bufsize,
                "crash_and_burn": True,
            },
            {
                "oid": "D",
                "categoryType": "Data",
                "dropclass": "dlg.data.drops.memory.InMemoryDROP",
                "producers": ["C"],
            },
        ]
        rels = [
            DROPRel("A", DROPLinkType.INPUT, "C"),
            DROPRel("B", DROPLinkType.INPUT, "C"),
        ]
        a_data = os.urandom(32)
        self._test_runGraphInTwoNMs(
            g1,
            g2,
            rels,
            a_data,
            None,
            root_oids=("A", "B"),
            leaf_oid="D",
            expected_failures=("C", "D"),
        )


@unittest.skipUnless(
    build_shared_library(_libname, _libpath),
    "Example dynamic library not available",
)
class TestExceptionRaised(unittest.TestCase):
    def test_exception_print_stats(self):
        """
        Pass in bad data for the dynlib
        """
        with self.assertRaises(TypeError) as context:
            _ = DynlibApp(
                "a",
                "a",
                lib=_libpath,
                print_stats="print_stats",
                bufsize=bufsize,
            )

        self.assertTrue(
            "print_stats should be a Boolean or Int" in str(context.exception)
        )

    def test_exception_bufsize(self):
        """
        Pass in bad data for the dynlib
        """
        with self.assertRaises(TypeError) as context:
            _ = DynlibApp(
                "a",
                "a",
                lib=_libpath,
                print_stats=print_stats,
                bufsize="bufsize",
            )

        self.assertTrue("bufsize should be an Int" in str(context.exception))

    def test_exception_memory_error(self):
        """
        Pass in bad data for the dynlib
        """
        with self.assertRaises(MemoryError) as context:
            dynlib_app = DynlibApp(
                "a",
                "a",
                lib=_libpath,
                print_stats=print_stats,
                bufsize=pow(2, 50),
            )
            dynlib_app.run()

        self.assertTrue(
            "Couldn't allocate memory for read/write buffer"
            in str(context.exception)
        )
