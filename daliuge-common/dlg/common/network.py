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
import contextlib
import errno
import logging
import socket
import time

logger = logging.getLogger(f"dlg.{__name__}")


def check_port(host, port, timeout=0, checking_open=True, return_socket=False):
    """
    Checks that the port specified by ``host``:``port`` is either open or
    closed (depending on the value of ``checking_open``) within a given
    ``timeout``.
    When checking for an open port, this method will keep trying to connect to
    it either until the given ``timeout`` has expired or until the socket is
    found open. When checking for a closed port this method will keep trying to
    connect to it until the connection is unsuccessful, or until the ``timeout``
    expires.
    Additionally, if some ``data`` is passed and the method is ``checking_open``
    then ``data`` will be written to the socket if it connects successfully.

    This method returns ``True`` if the port was found on the expected state
    within the time limit, and ``False`` otherwise.
    """

    if return_socket and not checking_open:
        raise ValueError("If return_socket is True then checking_open must be True")

    start = time.time()
    while True:
        try:

            # Calculate the timeout used during this cycle
            # If we're past the timeout we have failed already
            thisTimeout = None
            if timeout is not None and timeout != 0:
                thisTimeout = timeout - (time.time() - start)
                if thisTimeout <= 0:
                    return False

            # Create the socket and try to connect, sending data if required
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            if return_socket:
                ret = s
            else:
                ret = True

            # socket is closed always on error, conditionally otherwise
            try:
                s.settimeout(thisTimeout)
                s.connect((host, port))
                if not return_socket:
                    s.close()
            except socket.error as e:
                s.close()
                raise e
            except OSError: # pylint: disable=duplicate-except
                # logger.debug("Error opening connection!!")
                logger.error("Unable to connect to %s:%s", host, port)
                return not checking_open

            # Success if we were checking for an open port!
            if checking_open:
                return ret

            # Otherwise keep trying until we find the socket closed
            time.sleep(0.1)
            continue

        except socket.timeout:
            logger.debug(
                "Timed out while trying to connect to %s:%d with timeout of %f [s]",
                host,
                port,
                thisTimeout,
            )
            return not checking_open

        except socket.gaierror:
            logger.debug("Endpoint service %s:%d not known", host, port)
            return not checking_open

        except socket.error as e:

            # If the connection becomes suddenly closed from the server-side.
            # We assume that it's not re-opening any time soon
            if e.errno == errno.ECONNRESET:
                logger.debug(
                    "Connection closed by %s:%d, assuming it will stay closed",
                    host,
                    port,
                )
                if not return_socket:
                    return not checking_open
                raise

            # The port is closed
            elif e.errno == errno.ECONNREFUSED:

                if not checking_open:
                    return True

                # Keep trying because we're checking if it's open
                if timeout is not None:
                    if time.time() - start > timeout:
                        logger.debug(
                            "Refused connection to %s:%d for more than %f seconds",
                            host,
                            port,
                            timeout,
                        )
                        if not return_socket:
                            return False
                        raise

                time.sleep(0.1)
                continue

            # Any other error should be raised
            raise


def portIsClosed(host, port, timeout):
    """
    Checks if a given ``host``/``port`` is closed, with a given ``timeout``.
    """
    return check_port(host, port, timeout=timeout, checking_open=False)


def portIsOpen(host, port, timeout=0):
    """
    Checks if a given ``host``/``port`` is open, with a given ``timeout``.
    """
    return check_port(host, port, timeout=timeout, checking_open=True)


def connect_to(host, port, timeout=None):
    """
    Connects to ``host``:``port`` within the given timeout and return the
    connected socket. If no connection could be established a socket.timeout
    error is raised
    """
    s = check_port(host, port, timeout=timeout, return_socket=True)
    if s is False:
        raise socket.timeout()
    return s


def write_to(host, port, data, timeout=5):
    """
    Connects to ``host``:``port`` within the given timeout and write the given
    piece of ``data`` into the connected socket.
    """
    sock = connect_to(host, port, timeout=timeout)
    with contextlib.closing(sock):
        sock.send(data)
