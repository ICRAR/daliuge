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
This package contains the modules implementing the core functionality of
the system.
"""


# It is of utmost importance that this is the *first* thing we do any time
# we load daliuge up. Most modules in daliuge that have a logger create it at
# import time, so we need to perform this setup before doing any other imports
def setup_logger_class():

    # Doc building is failing due with "ValueError: Empty module name" in
    # "from . import drop" further below, which I can't easily explain.
    # Just skip this setup in RTD for the time being
    import os

    if os.environ.get("READTHEDOCS", None) == "True":
        return

    import logging

    # To avoid 'No handlers could be found for logger' messages during testing
    logging.getLogger(__name__).addHandler(logging.NullHandler())

    # Use our own logger class, which knows about the currently executing app
    class _DlgLogger(logging.Logger):
        def makeRecord(self, *args, **kwargs):
            record = super(_DlgLogger, self).makeRecord(*args, **kwargs)

            from .. import drop
            from ..manager import session

            # Try to get the UID of the drop ultimately in charge of sending this
            # log record, if any
            try:
                drop = drop.track_current_drop.tlocal.drop
            except AttributeError:
                drop = None
            try:
                drop_uid = drop.humanKey if drop else ""
            except AttributeError:
                drop_uid = ""

            # Do the same with the session_id, which can be found via the drop (if any)
            # or checking if there is a session currently executing something
            session_id = ""
            if drop and hasattr(drop, "dlg_session_id"):
                session_id = drop.dlg_session_id
            else:
                try:
                    session_id = session.track_current_session.tlocal.session.sessionId
                except AttributeError:
                    pass
            record.drop_uid = drop_uid
            record.session_id = session_id
            return record

    logging.setLoggerClass(_DlgLogger)


setup_logger_class()
del setup_logger_class

from dlg.common.version import git_version as __git_version__
from dlg.common.version import version as __version__
from ..apps import get_include_dir
from ..dask_emulation import delayed
