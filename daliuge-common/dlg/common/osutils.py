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
"""Common OS utilities"""
import logging
import math
import time

logger = logging.getLogger(f"dlg.{__name__}")


def terminate_or_kill(proc, timeout):
    """
    Terminates a process and waits until it has completed its execution within
    the given timeout. If the process is still alive after the timeout it is
    killed.
    """
    ecode = proc.poll()
    if ecode is not None:
        logger.info("Process %d already exited with code %d", proc.pid, ecode)
        return
    logger.info("Terminating %d", proc.pid)
    proc.terminate()
    wait_or_kill(proc, timeout)


def wait_or_kill(proc, timeout, period=0.1):
    waitLoops = 0
    max_loops = math.ceil(timeout / period)
    while proc.poll() is None and waitLoops < max_loops:
        time.sleep(period)
        waitLoops += 1

    kill9 = waitLoops == max_loops
    if kill9:
        logger.warning(
            "Killing %d by brute force after waiting %.2f [s], BANG! :-(",
            proc.pid,
            timeout,
        )
        proc.kill()
    proc.wait()
