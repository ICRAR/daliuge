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

from .version import git_version as __git_version__
from .version import version as __version__
from .apps import get_include_dir
from .dask_emulation import delayed

import logging

# To avoid 'No handlers could be found for logger' messages during testing
logging.getLogger(__name__).addHandler(logging.NullHandler())

# Use our own logger class, which knows about the currently executing app
class _DlgLogger(logging.Logger):
    def makeRecord(self, *args, **kwargs):
        record = super(_DlgLogger, self).makeRecord(*args, **kwargs)

        from . import drop
        from .manager import session

        # Try to get the UID of the drop ultimately in charge of sending this
        # log record, if any
        try:
            drop = drop.track_current_drop.tlocal.drop
        except AttributeError:
            drop = None
        drop_uid = drop.uid if drop else ''

        # Do the same with the session_id, which can be found via the drop (if any)
        # or checking if there is a session currently executing something
        session_id = ''
        if drop and hasattr(drop, '_dlg_session') and drop._dlg_session:
            session_id = drop._dlg_session.sessionId
        else:
            try:
                session_id = session.track_current_session.tlocal.session.sessionId
            except AttributeError:
                pass
        record.drop_uid = drop_uid
        record.session_id = session_id
        return record

logging.setLoggerClass(_DlgLogger)
del logging