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
'''
A small, httplib-only dependent flavor of the NGAS "read" and "write" methods.
Installing the NGAS client libraries is still a not-so-trivial exercise, and
while not all DALiuGE installations have the NGAS client libraries available we
still need to access NGAS from time to time.

@author: rtobar
'''

import six.moves.http_client as httplib  # @UnresolvedImport
import six.moves.urllib.request as urlrequest
import logging

logger = logging.getLogger(__name__)


def open(host, fileId, port=7777, timeout=None):
    url = 'http://%s:%d/RETRIEVE?file_id=%s' % (host, port, fileId)
    logger.debug("Issuing RETRIEVE request: %s" % (url))
    conn = urlrequest.urlopen(url)
    return conn

def retrieve(host, fileId, port=7777, timeout=None):
    """
    Retrieve the given fileId from the NGAS server located at `host`:`port`

    This method returns a file-like object that supports the `read` operation,
    and over which `close` must be invoked once no more data is read from it.
    """
    url = 'http://%s:%d/RETRIEVE?file_id=%s' % (host, port, fileId)
    logger.debug("Issuing RETRIEVE request: %s" % (url))
    conn = urlrequest.urlopen(url)
    if conn.status != httplib.OK:
        raise Exception("Error while RETRIEVE-ing %s from %s:%d: %d %s" % (fileId, host, port, conn.status, conn.msg))
    return conn

def beginArchive(host, fileId, port=7777, timeout=0, length=-1, mimeType='application/octet-stream'):
    """
    Opens a connecting to the NGAS server located at `host`:`port` and sends out
    the request for archiving the given `fileId`.

    This method returns the HTTP connection object, over which subsequential
    calls to `send` must be made with the chunks of data that need to be stored.
    Once all the data has been sent, the `finishArchive` method of this module
    should be invoked to check that all went well with the archiving.
    """
    conn = httplib.HTTPConnection(host, port, timeout=timeout)
    conn.putrequest('POST', '/QARCHIVE?filename=' + fileId)
    conn.putheader('Content-Type', mimeType)
    if length is not None and length >= 0:
        conn.putheader('Content-Length', length)
        # defer endheaders NGAS requires Content-Length
        conn.endheaders()
    return conn

def finishArchive(conn, fileId):
    """
    Checks that an archiving started by `beginArchive` went on successfully.
    """
    response = conn.getresponse()
    if response.status != httplib.OK:
        raise Exception("Error while QARCHIVE-ing %s to %s:%d: %d %s" % (fileId, conn.host, conn.port, response.status, response.msg))
