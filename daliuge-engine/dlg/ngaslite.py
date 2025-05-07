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
A small, httplib-only dependent flavor of the NGAS "read" and "write" methods.
Installing the NGAS client libraries is still a not-so-trivial exercise, and
while not all DALiuGE installations have the NGAS client libraries available we
still need to access NGAS from time to time.

@author: rtobar
"""

import http.client
import logging
import requests
from xml.dom.minidom import parseString

logger = logging.getLogger(f"dlg.{__name__}")


def open( # pylint: disable=redefined-builtin
    host,
    fileId,
    port=7777,
    timeout=5,
    mode=1,
    mimeType="application/octet-stream",
    length=-1,
    chunksize=65536,
):
    logger.debug(
        "Opening NGAS drop %s with mode %d and length %s", fileId, mode, length
    )
    if mode == 1:
        return retrieve(host, fileId, port=port, timeout=timeout, chunksize=chunksize)
    elif mode == 0:
        return beginArchive(
            host, fileId, port=port, timeout=timeout, mimeType=mimeType, length=length
        )
    else:
        # just return the status for that file_id
        stat = fileStatus(host, port, fileId, timeout=timeout)
        return stat


def retrieve(host, fileId, port=7777, timeout=None, chunksize=65536):
    """
    Retrieve the given fileId from the NGAS server located at `host`:`port`

    This method returns a file-like object that supports the `read` operation,
    and over which `close` must be invoked once no more data is read from it.
    """
    scheme = "http" if port != 443 else "https"
    url = "%s://%s:%d/RETRIEVE?file_id=%s" % (scheme, host, port, fileId)
    logger.debug("Issuing RETRIEVE request: %s", url)
    resp = requests.request("GET", url, stream=True, timeout=timeout)
    # conn = urllib.request.urlopen(url)
    if resp.status_code != http.HTTPStatus.OK:
        raise Exception(
            "Error while RETRIEVE-ing %s from %s:%d: %d %s"
            % (fileId, host, port, resp.status_code, resp.msg)
        )
    read_gen = resp.iter_content(chunksize)
    return read_gen


def beginArchive(
    host, fileId, port=7777, timeout=5, length=-1, mimeType="application/octet-stream"
):
    """
    Opens a connecting to the NGAS server located at `host`:`port` and sends out
    the request for archiving the given `fileId`.

    This method returns the HTTP connection object, over which subsequential
    calls to `send` must be made with the chunks of data that need to be stored.
    Once all the data has been sent, the `finishArchive` method of this module
    should be invoked to check that all went well with the archiving.
    """
    logger.debug(
        "Issuing ARCHIVE for file %s request to: http://%s:%d", fileId, host, port
    )
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    conn.putrequest("POST", "/QARCHIVE?filename=" + fileId)
    conn.putheader("Content-Type", mimeType)
    if length is not None and length >= 0:
        conn.putheader("Content-Length", length)
        # defer endheaders NGAS requires Content-Length
        conn.endheaders()
    else:
        logger.warning("Data size for %s unkown. Caching it first", fileId)
    return conn


def finishArchive(conn, fileId):
    """
    Checks that an archiving started by `beginArchive` went on successfully.
    """
    response = conn.getresponse()
    if response.status != http.HTTPStatus.OK:
        raise Exception(
            "Error while QARCHIVE-ing %s to %s:%d: %d %s"
            % (fileId, conn.host, conn.port, response.status, response.msg)
        )


def fileStatus(host, port, fileId, timeout=10):
    """
    Return the status as a dictionary for the file_id given
    TODO: This needs to use file_version as well, else it might
    return a value for a previous version.
    """
    scheme = "http" if port != 443 else "https"
    url = "%s://%s:%d/STATUS?file_id=%s" % (scheme, host, port, fileId)
    logger.debug("Issuing STATUS request: %s", url)
    try:
        conn = requests.request("GET", url, timeout=timeout)
        # conn = urllib.request.urlopen(url, timeout=timeout)
    except ConnectionError as e:
        raise FileNotFoundError from e
    if conn.status_code != http.HTTPStatus.OK:
        raise Exception(
            "Error while getting STATUS %s from %s:%d: %d %s"
            % (fileId, host, port, conn.status_code, conn.text)
        )
    dom = parseString(conn.content.decode())
    stat = dict(
        dom.getElementsByTagName("NgamsStatus")[0]
        .getElementsByTagName("DiskStatus")[0]
        .getElementsByTagName("FileStatus")[0]
        .attributes.items()
    )
    logger.debug("Returning status: %s", stat)
    return stat
