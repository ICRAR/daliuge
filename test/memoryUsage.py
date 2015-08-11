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
A small module that measures the average memory consumption of different
DataObject types. It was initially developed to address PRO-234.
"""
import sys
import psutil
from dfms import data_object

def measure(n, DOtype):
    """
    Create `n` DataObjects of type `DOtype` and measure how much memory does the
    program use at the beginning and the end of the process. It returns the
    total amount of memory used by all instances, and the average size of each
    of them, both amounts in bytes
    """
    p = psutil.Process()
    mem1 = p.memory_info()[0]
    dos = []
    for i in xrange(n):
        uid = str(i)
        dos.append(DOtype(uid, uid))
    mem2 = p.memory_info()[0]

    diff   = mem2 - mem1
    doSize = diff / float(n)
    return diff, doSize

if __name__ == '__main__':

    if len(sys.argv) < 3:
        sys.stderr.write("Usage: %s #iter doType\n" % (sys.argv[0]))
        sys.stderr.write("\n")
        sys.stderr.write("Example: %s 50 FileDataObject\n" % (sys.argv[0]))
        sys.exit()

    n = int(sys.argv[1])
    dotype = sys.argv[2]
    dotype = getattr(data_object, dotype)
    total, avg = measure(n, dotype)
    print "%d bytes used by %d %ss (%.2f bytes per DO)" % (total, n, dotype.__name__, avg)