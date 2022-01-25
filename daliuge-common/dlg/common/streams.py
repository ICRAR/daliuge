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
"""Common stream utilities"""
import json
import types
import zlib


class ZlibCompressedStream(object):
    """
    An object that takes a input of uncompressed stream and returns a compressed version of its
    contents when .read() is read.
    """

    def __init__(self, content):
        self.content = content
        self.compressor = zlib.compressobj()
        self.buf = []
        self.buflen = 0
        self.exhausted = False

    def readall(self):

        if not self.compressor:
            return b""

        content = self.content
        response = []
        compressor = self.compressor

        blocksize = 8192
        uncompressed = content.read(blocksize)
        while True:
            if not uncompressed:
                break
            compressed = compressor.compress(uncompressed)
            response.append(compressed)
            uncompressed = content.read(blocksize)

        response.append(compressor.flush())
        self.compressor = None
        return b"".join(response)

    def read(self, n=-1):

        if n <= 0:
            return self.readall()

        if self.buflen >= n:
            data = b"".join(self.buf)
            self.buf = [data[n:]]
            self.buflen -= n
            return data[:n]

        # Dump contents of previous buffer
        response = []
        written = 0
        if self.buflen:
            written += self.buflen
            data = b"".join(self.buf)
            response.append(data)
            self.buf = []
            self.buflen = 0

        compressor = self.compressor
        if not compressor:
            return b"".join(response)

        while True:

            decompressed = self.content.read(n)
            if not decompressed:
                compressed = compressor.flush()
                compressor = self.compressor = None
            else:
                compressed = compressor.compress(decompressed)

            if compressed:
                size = len(compressed)

                # If we have more compressed bytes than we need we write only
                # those needed to get us to n.
                to_write = min(n - written, size)
                if to_write:
                    response.append(compressed[:to_write])
                    written += to_write

                # The rest of the unwritten compressed bytes go into our internal
                # buffer
                if written == n:
                    self.buf.append(compressed[to_write:])
                    self.buflen += size - to_write

            if written == n or not compressor:
                break

        return b"".join(response)


class JSONStream(object):
    def __init__(self, objects):
        if isinstance(objects, (list, tuple, types.GeneratorType)):
            self.objects = enumerate(objects)
            self.isiter = True
        else:
            self.objects = objects
            self.isiter = False

        self.buf = []
        self.buflen = 0
        self.nreads = 0

    def read(self, n=-1):

        if n == -1:
            raise ValueError("n must be positive")

        if self.buflen >= n:
            self.buflen -= n
            data = b"".join(self.buf)
            self.buf = [data[n:]]
            return data[:n]

        written = 0
        response = []

        # Dump contents of previous buffer
        if self.buflen:
            data = b"".join(self.buf)
            written += self.buflen
            response.append(data)
            self.buf = []
            self.buflen = 0

        if self.nreads and not self.isiter:
            return b"".join(response)
        self.nreads += 1

        while True:

            if self.isiter:
                try:
                    i, obj = next(self.objects)
                    json_out = b"[" if i == 0 else b","
                    json_out += json.dumps(obj).encode("latin1")
                except StopIteration:
                    json_out = b"]"
                    self.isiter = False  # not nice, but prevents more reads
            else:
                json_out = json.dumps(self.objects).encode("latin1")

            if json_out:
                size = len(json_out)

                # If we have more decompressed bytes than we need we write only
                # those needed to get us to n.
                to_write = min(n - written, size)
                if to_write:
                    response.append(json_out[0:to_write])
                    written += to_write

                # The rest of the unwritten decompressed bytes go into our internal
                # buffer
                if written == n:
                    self.buf.append(json_out[to_write:])
                    self.buflen += size - to_write

            if written == n:
                break

            if not self.isiter:
                break

        return b"".join(response)
