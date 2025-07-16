#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2016
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
Module containing common exceptions used throughout Daliuge.
In particular DaliugeException should be the root of all our exceptions.
"""


class DaliugeException(Exception):
    """
    The parent of all exceptions thrown by Daliuge
    """


class InvalidDropException(DaliugeException):
    """
    An exception thrown when a Drop is created with a set of invalid arguments.
    """

    def __init__(self, drop, reason):
        DaliugeException.__init__(self, drop, reason)
        if isinstance(drop, (list, tuple)):
            self.oid, self.uid = drop
        else:
            self.oid = drop.oid
            self.uid = drop.uid
        self.reason = reason
        self.msg = "InvalidDropException <Drop %s / %s>: %s" % (
            self.uid,
            self.oid,
            self.reason,
        )

    def __str__(self, *args, **kwargs):
        return self.msg


class InvalidRelationshipException(DaliugeException):
    """
    An exception thrown when a relationship between two Drops has been
    instructed but is invalid in nature.
    """

    def __init__(self, rel, reason):
        DaliugeException.__init__(self, rel, reason)
        self.rel = rel
        self.reason = reason
        self.msg = "InvalidRelationshipException <%r>: %s" % (self.rel, self.reason)

    def __str__(self, *args, **kwargs):
        return self.msg


class InvalidGraphException(DaliugeException):
    """
    An exception thrown when an invalid graph, or part of a graph, is given to
    Daliuge.
    """

class DropChecksumException(DaliugeException):
    """
    An exception thrown when a DROP does not pass a checksum check
    """
    def __init__(self, drop):
        DaliugeException.__init__(self, drop)
        self.drop = drop
        self.msg = "DropChecksumException <%r> checksum: %s" % (self.drop, drop.checksum)

    def __str__(self, *args, **kwargs):
        return self.msg


class NoDropException(DaliugeException):
    """
    An exception thrown when a Drop UID is pointing to a non-existing Drop
    """

    def __init__(self, drop_uid, reason=None):
        DaliugeException.__init__(self, drop_uid, reason)
        self._drop_uid = drop_uid
        self._reason = reason

    def __str__(self):
        ret = "NoDropException <drop_uid: %s>" % (self._drop_uid)
        if self._reason:
            ret += ". Reason: %s" % (self._reason)
        return ret


class NoSessionException(DaliugeException):
    """
    An exception thrown when a session ID is pointing to a non-existing session
    """

    def __init__(self, session_id, reason=None):
        DaliugeException.__init__(self, session_id, reason)
        self._session_id = session_id
        self._reason = reason

    def __str__(self):
        ret = "NoSessionException <session_id: %s>" % (self._session_id)
        if self._reason:
            ret += ". Reason: %s" % (self._reason)
        return ret

    @property
    def session_id(self):
        return self._session_id


class SessionAlreadyExistsException(DaliugeException):
    """
    An exception thrown when a session ID is pointing to an existing session
    but is meant to be used as the ID of a new session.
    """

    def __init__(self, session_id, reason=None):
        DaliugeException.__init__(self, session_id, reason)
        self._session_id = session_id
        self._reason = reason

    def __str__(self):
        ret = "SessionAlreadyExistsException <session_id: %s>" % (self._session_id)
        if self._reason:
            ret += ". Reason: %s" % (self._reason)
        return ret

    @property
    def session_id(self):
        return self._session_id


class InvalidSessionState(DaliugeException):
    """
    An exception thrown when an operation is requested on a session that is not
    in the expected state for that operation.
    """


class SubManagerException(DaliugeException):
    """
    An exception thrown by composite drop managers when there was an error
    invoking one of their underlying drop managers.
    """
