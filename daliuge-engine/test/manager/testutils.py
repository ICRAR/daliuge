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
import http.client
import json

from dlg import utils
import codecs


def _get(url, port):
    conn = http.client.HTTPConnection("localhost", port, timeout=3)
    conn.request("GET", "/api" + url)
    return conn.getresponse(), conn


def get(test, url, port):
    res, conn = _get(url, port)
    test.assertEqual(http.HTTPStatus.OK, res.status)
    jsonRes = json.load(codecs.getreader("utf-8")(res))
    res.close()
    conn.close()
    return jsonRes


def post(test, url, port, content=None, mimeType=None):
    conn = http.client.HTTPConnection("localhost", port, timeout=3)
    headers = {mimeType or "Content-Type": "application/json"} if content else {}
    conn.request("POST", "/api" + url, content, headers)
    res = conn.getresponse()
    test.assertEqual(http.HTTPStatus.OK, res.status)
    conn.close()


def delete(test, url, port):
    conn = http.client.HTTPConnection("localhost", port, timeout=3)
    conn.request("DELETE", "/api" + url)
    res = conn.getresponse()
    test.assertEqual(http.HTTPStatus.OK, res.status)
    conn.close()


class terminating(object):
    """
    A context manager that makes sure a process always exits.
    """

    def __init__(self, proc, timeout):
        self.proc = proc
        self.timeout = timeout

    def __enter__(self):
        return self.proc

    def __exit__(self, typ, val, traceback):
        utils.terminate_or_kill(self.proc, self.timeout)
        return False
