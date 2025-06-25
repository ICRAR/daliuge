#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2019
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
import json
import logging
import os
import time

from .. import droputils
from .. import constants
from ..manager.client import BaseDROPManagerClient
from ..manager.session import SessionStates
import itertools


logger = logging.getLogger(f"dlg.{__name__}")


class _StatusDumper(BaseDROPManagerClient):
    """A client that dumps the graph status each time a session is queried"""

    def __init__(self, *args, **kwargs):
        self.dump_path = kwargs.pop("dump_path")
        super(_StatusDumper, self).__init__(*args, **kwargs)

    def _dump_session_status(self, session_id):
        wgs = {
            "ssid": session_id,
            "gs": self.graph_status(session_id),
            "ts": "%.3f" % time.time(),
        }
        with open(self.dump_path, "a") as fs:
            json.dump(wgs, fs)
            fs.write(os.linesep)

    def sessions(self):
        sessions = super(_StatusDumper, self).sessions()
        for session in sessions:
            self._dump_session_status(session["sessionId"])
        return sessions

    def session(self, sessionId):
        session = super(_StatusDumper, self).session(sessionId)
        self._dump_session_status(sessionId)
        return session


def _is_end_state(session_status):
    return session_status in (SessionStates.FINISHED, SessionStates.CANCELLED)


def _get_client(host, port, timeout, status_dump_path=None):
    kwargs = {"host": host, "port": port, "timeout": timeout}
    clazz = BaseDROPManagerClient
    if status_dump_path:
        clazz = _StatusDumper
        kwargs["dump_path"] = status_dump_path
    return clazz(**kwargs)


def _session_status(session):
    def _get(status):
        if isinstance(status, dict):
            status = [_get(s) for s in status.values()]
            if isinstance(status[0], list):
                status = list(itertools.chain(*status))
        return status

    x = _get(session["status"])
    return x


def _session_finished(session):
    session_status = _session_status(session)
    logger.debug("Session status: %r", session_status)
    if isinstance(session_status, int):
        return _is_end_state(session_status)
    return all(_is_end_state(s) for s in session_status)


def monitor_sessions(
    session_id=None,
    poll_interval=10,
    host="localhost",
    port=constants.ISLAND_DEFAULT_REST_PORT,
    timeout=30,
    status_dump_path=None,
):
    """
    Monitors the execution status of all sessions (or the single session
    specified by `session_id`) by polling `host`:`port`, and returns when they
    all have finished executing.
    """
    client = _get_client(host, port, timeout, status_dump_path)
    if session_id:
        while True:
            session = client.session(session_id)
            if _session_finished(session):
                return _session_status(session)
            time.sleep(poll_interval)
    else:
        while True:
            sessions = client.sessions()
            if all(_session_finished(s) for s in sessions):
                return {s["sessionId"]: _session_status(s) for s in sessions}
            time.sleep(poll_interval)


def monitor_sessions_repro(
    session_id=None,
    poll_interval=10,
    host="localhost",
    port=constants.ISLAND_DEFAULT_REST_PORT,
    timeout=60,
    status_dump_path=None,
):
    """
    Very similar to monitoring execution status of all (or one) session specified by `session_id`
    by polling `host`:`port`, and returns when they all have finalized their reproducibility data.
    """
    client = _get_client(host, port, timeout, status_dump_path)
    if session_id:
        while True:
            repro_status = client.session_repro_status(session_id)
            if repro_status:
                return True
            time.sleep(poll_interval)
    else:
        while True:
            sessions = client.sessions()
            if all(client.session_repro_status(s) for s in sessions):
                return {s["sessionId"]: s["repro"] for s in sessions}
            time.sleep(poll_interval)


def fetch_reproducibility(
    session_id=None,
    poll_interval=10,
    host="localhost",
    port=constants.ISLAND_DEFAULT_REST_PORT,
    timeout=60,
):
    """
    Fetches the final graph and associated reproducibility information for `session_id`.
    """
    if session_id is None:
        return {}
    client = _get_client(host, port, timeout)
    while True:
        repro_data = client.session_repro_data(session_id)
        if repro_data is not None:
            return repro_data
        time.sleep(poll_interval)


def submit(
    pg,
    host="localhost",
    port=constants.ISLAND_DEFAULT_REST_PORT,
    timeout=30,
    skip_deploy=False,
    session_id=None,
):
    """
    Submits a physical graph for execution. Additionally it waits until the
    graph has finished executing.
    """
    client = _get_client(host, port, timeout)
    session_id = session_id or "%f" % (time.time())
    completed_uids = droputils.get_roots(pg)
    with client:
        client.create_session(session_id)
        logger.info("Session %s created", session_id)
        client.append_graph(session_id, pg)
        logger.info("Graph for session %s appended", session_id)
        if not skip_deploy:
            client.deploy_session(session_id, completed_uids=completed_uids)
            logger.info("Session %s deployed", session_id)
    return session_id
