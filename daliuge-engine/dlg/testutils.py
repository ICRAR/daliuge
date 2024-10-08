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
from __future__ import annotations
import threading

from dlg import constants
from dlg.manager.composite_manager import DataIslandManager, MasterManager
from dlg.manager.node_manager import NodeManager
from dlg.manager.rest import NMRestServer, CompositeManagerRestServer
from dlg.utils import portIsOpen


class ManagerInfo(object):
    def __init__(self, manager, server, thread, test):
        self.manager = manager
        self.server = server
        self.thread = thread
        self.test = test

    def __enter__(self):
        pass

    def __exit__(self, *_args, **_kwargs):
        self.stop()

    def stop(self):
        self.server.stop()
        self.thread.join()
        self.manager.shutdown()
        self.test.assertFalse(self.thread.is_alive())


class ManagerStarter(object):
    def _start_manager_in_thread(
        self, port, manager_class, rest_class, *manager_args, **manager_kwargs
    ):
        manager = manager_class(*manager_args, **manager_kwargs)
        server = rest_class(manager)
        thread = threading.Thread(target=server.start, args=("localhost", port))
        thread.start()
        self.assertTrue(portIsOpen("localhost", port, 5))
        return ManagerInfo(manager, server, thread, self)

    def start_nm_in_thread(self,
                           port=constants.NODE_DEFAULT_REST_PORT,
                           events_port=constants.NODE_DEFAULT_EVENTS_PORT,
                           rpc_port=constants.NODE_DEFAULT_RPC_PORT):
        return self._start_manager_in_thread(
            port, NodeManager, NMRestServer, False, rpc_port, events_port)

            # port, NodeManager, NMRestServer, False, rpc_port, events_port)
    def start_dim_in_thread(
        self,
        nm_hosts: list[str] = None,
        port=constants.ISLAND_DEFAULT_REST_PORT,
    ):
        if not nm_hosts:
            nm_hosts = [f"localhost:{constants.NODE_DEFAULT_REST_PORT}"]
        return self._start_manager_in_thread(
            port, DataIslandManager, CompositeManagerRestServer, nm_hosts
        )

    def start_mm_in_thread(
        self,
        nm_hosts: list[str] = None,
        port=constants.MASTER_DEFAULT_REST_PORT,
    ):
        if not nm_hosts:
            nm_hosts = [f"localhost:{constants.ISLAND_DEFAULT_REST_PORT}"]
        return self._start_manager_in_thread(
            port, MasterManager, CompositeManagerRestServer, nm_hosts
        )
