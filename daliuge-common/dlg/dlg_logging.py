#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2025
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
Logging facilities for DALiuGE runtime environment.
"""

import logging

PREFIX = "dlg"
USER = 25

def setup_logger_class():

    # Doc building is failing due with "ValueError: Empty module name" in
    # "from . import drop" further below, which I can't easily explain.
    # Just skip this setup in RTD for the time being
    import os

    if os.environ.get("READTHEDOCS", None) == "True":
        return

    class _DlgLogger(logging.Logger):
        """
        Logging subclass that adds information to the log record and creates a "USER"
        log level.

        This exists in the daliuge-common to ensure that the USER logging is available
        both across all modules.

        Note: Moving this to dlg-engine prevents the USER log-level from being run within
        subprocesses (for example, when deployed using the start_dlg_cluster).
        """

        def makeRecord(self, *args, **kwargs):

            record = super(_DlgLogger, self).makeRecord(*args, **kwargs)

            try:
                from dlg import drop
                from dlg.manager import session

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
            except ImportError:
                pass

            finally:
                return record


        def user(self, message, *args, **kwargs):
            """
            Operator-specific error messages
            :param self:
            :param message:
            :param args:
            :param kwargs:
            :return:
            """
            if self.isEnabledFor(USER):
                self._log(USER, message, args, **kwargs)


    logging.addLevelName(USER, "USER")
    # To avoid 'No handlers could be found for logger' messages during testing
    logging.getLogger(__name__).addHandler(logging.NullHandler())

    # Use our own logger class, which knows about the currently executing app
    logging.setLoggerClass(_DlgLogger)
    logging.addLevelName(USER, "USER")
    # logging.user = _DlgLogger.user
    logging.USER = USER # CREATE
