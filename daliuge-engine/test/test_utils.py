#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2016
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
import json
import os
import tempfile
import unittest
import zlib

from dlg import utils


class TestUtils(unittest.TestCase):
    def test_zlib_uncompressed_stream(self):
        fname = tempfile.mktemp()
        with open(fname, "wb") as f:
            f.write(zlib.compress(b"abc"))

        # Read parts from the beginning
        for b, n in ((b"abc", 3), (b"ab", 2), (b"a", 1)):
            with open(fname, "rb") as f:
                s = utils.ZlibUncompressedStream(f)
                self.assertEqual(b, s.read(n))

        # Read whole contents by successive calls
        with open(fname, "rb") as f:
            s = utils.ZlibUncompressedStream(f)
            self.assertEqual(b"a", s.read(1))
            self.assertEqual(b"b", s.read(1))
            self.assertEqual(b"c", s.read(1))
            self.assertEqual(0, s.buflen)

        os.remove(fname)

        # Try now with bigger data sizes
        for size in [2**x + y for x in range(3, 18) for y in (-1, 0, 1)]:
            original_bytes = os.urandom(size)
            compressed_bytes = zlib.compress(original_bytes)

            # Try first with whole reads
            compressed_stream = io.BytesIO(compressed_bytes)
            uncompressed_stream = utils.ZlibUncompressedStream(
                compressed_stream
            )
            b = uncompressed_stream.read()
            self.assertEqual(size, len(b))
            self.assertEqual(original_bytes, b)

            # Now read little by little
            read_size = min(size // 4, 1024)
            b = b""
            compressed_stream = io.BytesIO(compressed_bytes)
            uncompressed_stream = utils.ZlibUncompressedStream(
                compressed_stream
            )
            for u in iter(
                functools.partial(uncompressed_stream.read, read_size), b""
            ):
                b += u
            self.assertEqual(size, len(b))
            self.assertEqual(original_bytes, b)

    def test_zlib_compressed_stream_writer(self):
        compressed_ref = zlib.compress(b"abcd")

        # Read parts from the beginning
        for x in range(1, len(compressed_ref)):
            bytesio = io.BytesIO(b"abcd")
            s = utils.ZlibCompressedStream(bytesio)
            compressed = s.read(x)
            self.assertEqual(x, len(compressed))
            self.assertEqual(compressed_ref[0:x], compressed[0:x])

        # Try now with bigger data sizes
        for size in [2**x + y for x in range(3, 18) for y in (-1, 0, 1)]:
            original_bytes = os.urandom(size)
            compressed_bytes = zlib.compress(original_bytes)

            # Try first with whole reads
            uncompressed_stream = io.BytesIO(original_bytes)
            compressed_stream = utils.ZlibCompressedStream(uncompressed_stream)
            b = compressed_stream.read()
            self.assertEqual(
                len(compressed_bytes),
                len(b),
                "Incorrect size when compressing %d bytes" % (size),
            )
            self.assertEqual(compressed_bytes, b)

            # Now read little by little
            read_size = min(size // 4, 1024)
            uncompressed_stream = io.BytesIO(original_bytes)
            compressed_stream = utils.ZlibCompressedStream(uncompressed_stream)
            b = b""
            for c in iter(
                functools.partial(compressed_stream.read, read_size), b""
            ):
                b += c
            self.assertEqual(
                len(compressed_bytes),
                len(b),
                "Incorrect size when compressing %d bytes" % (size),
            )
            self.assertEqual(compressed_bytes, b)

    def test_zlib_streams_combined_randombytes(self):
        self._test_zlib_streams_combined(os.urandom)

    def test_zlib_streams_combined_zerobytes(self):
        self._test_zlib_streams_combined(lambda n: b"0" * n)

    def _test_zlib_streams_combined(self, gen_bytes):
        sizes = [2**x + y for x in range(1, 18) for y in (-1, 0, 1)]
        for size in sizes:
            original_bytes = gen_bytes(size)

            # Read the whole thing
            original_stream = io.BytesIO(original_bytes)
            compressed_stream = utils.ZlibCompressedStream(original_stream)
            uncompressed_stream = utils.ZlibUncompressedStream(
                compressed_stream
            )
            b = uncompressed_stream.read()
            self.assertEqual(size, len(b))
            self.assertEqual(original_bytes, b)

            # There's nothing left now to read
            for x in range(10):
                self.assertEqual(0, len(uncompressed_stream.read(100)))

            # Read with given number of bytes
            for n in (1, len(original_bytes) // 2, len(original_bytes)):
                these_bytes = original_bytes[0:n]

                original_stream = io.BytesIO(these_bytes)
                compressed_stream = utils.ZlibCompressedStream(original_stream)
                uncompressed_stream = utils.ZlibUncompressedStream(
                    compressed_stream
                )

                b = uncompressed_stream.read(n)
                self.assertEqual(n, len(b))
                self.assertEqual(these_bytes, b)

                # There's nothing left now to read
                for x in range(10):
                    self.assertEqual(0, len(uncompressed_stream.read(100)))

    def test_json_stream_simple_sequence(self):
        for s in ([0], [{}], ["a"], [{"oid": "A", "categoryType": "Data"}]):
            stream = utils.JSONStream(s)
            self.assertEqual(s, json.loads(stream.read(100).decode("utf8")))

    def test_json_stream_sequences(self):
        ref = [1, 2, 3]
        objects_list = [1, 2, 3]
        objects_tuple = (1, 2, 3)

        def objects_gen():
            yield 1
            yield 2
            yield 3

        for objects in (objects_list, objects_tuple, objects_gen()):
            stream = utils.JSONStream(objects)
            self.assertSequenceEqual(
                ref, json.loads(stream.read(100).decode("latin1"))
            )
            self.assertEqual(0, len(stream.read(100).decode("latin1")))

    def test_json_stream_simpleobj(self):
        sessionId = "some_id"
        for obj in (1, {"a": 2}, "b", {"sessionId": sessionId}):
            stream = utils.JSONStream(obj)
            self.assertEqual(
                obj, json.loads(stream.read(100).decode("latin1"))
            )
            self.assertEqual(0, len(stream.read(100).decode("latin1")))

    def test_get_dlg_root(self):
        # It should obey the DLG_ROOT environment variable
        old = os.environ.get("DLG_ROOT", None)
        os.environ["DLG_ROOT"] = tempfile.mkdtemp()
        self.assertEqual(utils.getDlgDir(), os.environ["DLG_ROOT"])
        os.rmdir(os.environ["DLG_ROOT"])
        if old:
            os.environ["DLG_ROOT"] = old
        else:
            del os.environ["DLG_ROOT"]

