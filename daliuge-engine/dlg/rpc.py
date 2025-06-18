#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2017
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
RPC support for DALiuGE

This module contains all client and server RPC classes for all the different
technologies we support.
"""

import collections
import dataclasses
import logging
import queue
import threading

import gevent
import zerorpc

from dlg.manager.manager_data import Node

from dlg import utils

logger = logging.getLogger(f"dlg.{__name__}")


class RPCObject(object):
    """Base class for all RCP clients and server"""

    def start(self):
        self.rpc_running = True

    def shutdown(self):
        self.rpc_running = False


class RPCClientBase(RPCObject):
    """Base class for all RPC clients"""

    def get_drop_attribute(self, hostname, port, session_id, uid, name):

        # hostname = hostname.split(":")[0]

        logger.debug(
            "Getting attribute %s for drop %s of session %s at %s:%d",
            name,
            uid,
            session_id,
            hostname,
            port,
        )
        # hostname = hostname.split(":")[0]

        client, closer = self.get_rpc_client(hostname, port)

        # The remote method receives the same client used to inspect the remote
        # object, and it closes it when the method is not used anymore
        class remote_method(object):
            def __del__(self):
                closer()

            def __call__(self, *args):
                return client.call_drop(session_id, uid, name, *args)

        # Shortcut to avoid extra calls
        known_methods = ()
        # known_methods = ('open', 'read', 'write', 'close')
        closeit = False
        try:
            if name in known_methods or client.has_method(session_id, uid, name):
                return remote_method()
            closeit = True
            return client.get_drop_property(session_id, uid, name)
        finally:
            if closeit:
                closer()


class RPCServerBase(RPCObject):
    """Base class for all RPC server"""

    def __init__(self, host, port):
        self._rpc_host = host.split(":")[0]
        self._rpc_port = port


class ZeroRPCClient(RPCClientBase):
    """ZeroRPC client support"""

    request = collections.namedtuple("request", "method args queue")
    response = collections.namedtuple("response", "value is_exception")

    def __init__(self, *args, **kwargs):
        super(ZeroRPCClient, self).__init__(*args, **kwargs)
        self._zrpcclients = {}
        self._zrpcclientthreads = []
        self._own_context = False
        logger.debug("RPC Client created")

    def __del__(self):
        if self._own_context and self._context:
            self._context.term()

    def start(self):
        super(ZeroRPCClient, self).start()
        if not hasattr(self, "_context"):
            self._context = zerorpc.Context()
            self._own_context = True

        # One per remote host
        self._zrpcclient_acquisition_lock = threading.Lock()
        self._zrpcclients = {}
        self._zrpcclientthreads = []

    def shutdown(self):
        super(ZeroRPCClient, self).shutdown()
        for t in self._zrpcclientthreads:
            t.join(10)
            if t.is_alive():
                logger.warning("ZeroRPC client thread %s is still alive", t.name)
        if self._own_context:
            self._context.term()
            self._context = None

    def get_client_for_endpoint(self, host, port):

        # host = host.split(":")[0]
        if isinstance(host, Node):
            endpoint = (host.host, port)
        else:
            endpoint = (host, port)

        with self._zrpcclient_acquisition_lock:
            if endpoint in self._zrpcclients:
                return self._zrpcclients[endpoint]

            # We start the new client on its own thread so it uses gevent, etc.
            # In this thread we create simply enqueue requests
            req_queue = queue.Queue()
            tname_tpl, args = "zrpc(%s:%d)", (host, port)
            t = threading.Thread(
                target=self.run_zrpcclient,
                args=(host, port, req_queue),
                name=tname_tpl % args,
            )
            t.start()

            class QueueingClient(object):
                def __make_call(self, method, *args):
                    res_queue = queue.Queue()
                    request = ZeroRPCClient.request(method, args, res_queue)
                    req_queue.put(request)
                    x = res_queue.get()
                    if x.is_exception:
                        raise x.value
                    return x.value

                def call_drop(self, session_id, uid, name, *args):
                    return self.__make_call("call_drop", session_id, uid, name, *args)

                def get_drop_property(self, session_id, uid, name):
                    return self.__make_call("get_drop_property", session_id, uid, name)

                def has_method(self, session_id, uid, name):
                    return self.__make_call("has_method", session_id, uid, name)

            client = QueueingClient()
            self._zrpcclients[endpoint] = client
            self._zrpcclientthreads.append(t)
            return client

    def run_zrpcclient(self, host, port, req_queue):
        if isinstance(host, Node):
            host = host.host  # split(":")[0]
        client = zerorpc.Client("tcp://%s:%d" % (host, port), context=self._context)

        forwarder = gevent.spawn(self.forward_requests, req_queue, client)
        gevent.joinall([forwarder])

        logger.info("Closing %s:%d ZeroRPC client", host, port)
        client.close()

    def forward_requests(self, req_queue, client):
        while self.rpc_running:
            try:
                req = req_queue.get_nowait()
                gevent.spawn(self.queue_request, client, req)
            except queue.Empty:
                gevent.sleep(0.005)

    def process_response(self, req, async_response):
        try:
            x = ZeroRPCClient.response(async_response.get_nowait(), False)
        except gevent.Timeout as e:
            raise RuntimeError("Timed out on AsyncResult.get_nowait") from e
        except RuntimeError as e:
            x = ZeroRPCClient.response(e, True)
        req.queue.put(x)

    def queue_request(self, client, req):
        # Pass "async" in a dictionary; 3.7+ fails because it's a keyword
        async_result = client.__call__(req.method, *req.args, **{"async": True})
        async_result.rawlink(lambda x: self.process_response(req, x))

    def get_rpc_client(self, hostname, port):
        # hostname = hostname.split(":")[0]
        client = self.get_client_for_endpoint(hostname, port)
        # No closing function since clients are long-lived
        return client, lambda: None


class ZeroRPCServer(RPCServerBase):
    """ZeroRPC server support"""

    @classmethod
    def create_context(cls):
        # This import can take a long time in big HPC deployments
        return zerorpc.Context()

    @property
    def rpc_endpoint(self):
        return self._rpc_host, self._rpc_port

    def start(self):
        super(ZeroRPCServer, self).start()

        # Starts the single-threaded ZeroRPC server for RPC requests
        timeout = 30
        server_started = threading.Event()
        self._zrpcserverthread = threading.Thread(
            target=self.run_zrpcserver,
            name="ZeroRPC server",
            args=(self._rpc_host, self._rpc_port, server_started),
        )
        self._zrpcserverthread.start()
        if not server_started.wait(timeout):
            raise Exception(
                "ZeroRPC server didn't start within %d seconds" % (timeout,)
            )

    def run_zrpcserver(self, host, port, server_started):
        # Use context provided by subclass
        self._zrpcserver = zerorpc.Server(self, context=self._context)
        # zmq needs an address, not a hostname
        endpoint = "tcp://%s:%d" % (utils.zmq_safe(host), port)
        logger.debug("Trying to bind ZeroRPC to %s", endpoint)
        self._zrpcserver.bind(endpoint)
        logger.info("Listening for RPC requests via ZeroRPC on %s", endpoint)
        server_started.set()

        runner = gevent.spawn(self._zrpcserver.run)
        stopper = gevent.spawn(self.stop_zrpcserver)
        gevent.joinall([runner, stopper])
        logger.info("ZeroRPC server finished")

    def stop_zrpcserver(self):
        while self.rpc_running:
            gevent.sleep(0.01)
        logger.info(
            "Closing ZeroRPC server on tcp://%s:%d",
            utils.zmq_safe(self._rpc_host),
            self._rpc_port,
        )
        self._zrpcserver.close()

    def shutdown(self):
        super(ZeroRPCServer, self).shutdown()
        self._zrpcserverthread.join()


RPCServer, RPCClient = ZeroRPCServer, ZeroRPCClient


@dataclasses.dataclass(frozen=True)
class ProxyInfo:
    """Information needed to create a DropProxy"""

    hostname: str
    port: int
    session_id: str
    uid: str

    @classmethod
    def from_data_drop(cls, drop):
        if isinstance(drop, DropProxy):
            return drop.proxy_info

        # TODO: we can't use the RPC endpoint's host directly here, as that
        #       indicates the address the different servers *bind* to
        #       (and, for example, can be 0.0.0.0)
        assert drop.rpc_endpoint
        rpc_host, rpc_port = drop.rpc_endpoint
        rpc_host = utils.to_externally_contactable_host(rpc_host, prefer_local=True)
        return cls(rpc_host, rpc_port, drop.dlg_session_id, drop.uid)

    def __repr__(self):
        return (
            f"<ProxyInfo {self.uid}, session {self.session_id} "
            f"@{self.hostname}:{self.port}>"
        )


class DropProxy(object):
    """
    A proxy to a remote drop.

    It forwards attribute requests and procedure calls through the given RPC client.
    """

    def __init__(self, rpc_client, proxy_info: ProxyInfo):
        self.rpc_client = rpc_client
        self._proxy_info: ProxyInfo = proxy_info
        logger.debug("Created %r", self)

    def handleEvent(self, evt):
        pass

    def __getattr__(self, name):
        if name == "uid":
            return self._proxy_info.uid
        elif name in ("inputs", "streamingInputs", "outputs", "consumers", "producers"):
            return []
        return self.rpc_client.get_drop_attribute(
            self._proxy_info.hostname,
            self._proxy_info.port,
            self._proxy_info.session_id,
            self.uid,
            name,
        )

    def __repr__(self):
        return f"<DropProxy with {self._proxy_info}"

    @property
    def proxy_info(self) -> ProxyInfo:
        """
        Get ProxyInfo
        :return:  the proxy info associated with the object
        """
        return self._proxy_info
