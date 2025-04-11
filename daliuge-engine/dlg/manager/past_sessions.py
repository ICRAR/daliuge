#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2024
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
Module that contains functionality for managing and manipulating previous sessions that have been run. 

Currently, this module only supports sessions with history stored on the _local_ filesystem. 
"""

from pathlib import Path

class PastSessionManager:
    """
    A utility class that 'manages' past sessions that have been run on the current compute
    node.

    """
    def __init__(self, work_dir: str, allow_no_graphs=False):
        self._work_dir = Path(work_dir)
        self._allow_no_graphs = allow_no_graphs

    def past_sessions(self, excluded_sessions: list = None) -> list[Path]:
        """
        Return the list of past_sessions that exists in the current working directory.

        If excluded_sessions is provided, they will be removed from the returned list.
        This is useful to remove duplications in past sessions and current session
        history when the current sessions are still tracked by the Manager.

        :params: excluded_sessions, list of sessions we want to exclude from returned
        sessions.
        """

        return [
            path for path in self._work_dir.iterdir()
            if (path.is_dir()
                and path.name not in excluded_sessions
                and self._is_session_dir(path))
        ]


    @staticmethod
    def _is_session_dir(session: Path, expected_ext=".graph") -> bool:
        """
        Inspect each file in 'session' and check if there is a file with
        'expected_suffix', which is a required file for counting that directory as a
        session directory.

        expected_ext is intended to avoid listing sessions for which there is no re-run
        or reproducibility information.
        
        :param: session, Path: a directory containing session information. 
        :param: expected_suffix: expected file extension of a file that we expect to see 
                in the session directory. If a file with this extension does not exist, 
                then this is not a 'valid' directory and we do not wish to add it to the 
                list of past sessions. 
        """

        return any(expected_ext in f.name for f in session.iterdir())


