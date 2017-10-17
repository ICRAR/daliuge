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
Module containing the base interface for all DROP managers.
"""
import abc


class DROPManager(object):
    """
    Base class for all DROPManagers.

    A DROPManager, as the name states, manages the creation and execution of
    DROPs. In order to support parallel DROP graphs execution, a DROPManager
    separates them into "sessions".

    Sessions follow a simple lifecycle:
     * They are created in the PRISTINE status
     * One or more graph specifications are appended to them, which can also
       be linked together, building up the final graph specification. While
       building the graph the session is in the BUILDING status.
     * Once all graph specifications have been appended and linked together,
       the graph is deployed, meaning that the DROPs are effectively created.
       During this process the session transitions between the DEPLOYING and
       RUNNING states.
     * One all DROPs contained in a session have transitioned to COMPLETED (or
       ERROR, if there has been an error during the execution) the session moves
       to FINISHED.

    Graph specifications are currently accepted in the form of a list of
    dictionaries, where each dictionary is a DROP specification. A DROP
    specification in turn consists on key/value pairs in the dictionary which
    state the type of DROP, some key parameters, and instance-specific
    parameters as well used to create the DROP.
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def createSession(self, sessionId):
        """
        Creates a session on this DROPManager with id `sessionId`. A session
        represents an isolated DROP graph execution.
        """

    @abc.abstractmethod
    def getSessionStatus(self, sessionId):
        """
        Returns the status of the session `sessionId`.
        """

    @abc.abstractmethod
    def addGraphSpec(self, sessionId, graphSpec):
        """
        Adds a graph specification `graphSpec` (i.e., a description of the DROPs
        that should be created) to the current graph specification held by
        session `sessionId`.
        """

    @abc.abstractmethod
    def getGraphStatus(self, sessionId):
        """
        Returns the status of the graph being executed in session `sessionId`.
        """

    @abc.abstractmethod
    def getGraph(self, sessionId):
        """
        Returns a specification of the graph currently held by session
        `sessionId`.
        """

    @abc.abstractmethod
    def deploySession(self, sessionId, completedDrops=[]):
        """
        Deploys the graph specification held by session `sessionId`, effectively
        creating all DROPs, linking them together, and moving those whose UID
        is in `completedDrops` to the COMPLETED state.
        """

    @abc.abstractmethod
    def destroySession(self, sessionId):
        """
        Destroys the session `sessionId`
        """

    @abc.abstractmethod
    def getSessionIds(self):
        """
        Returns the IDs of the sessions currently held by this DROPManager.
        """

    @abc.abstractmethod
    def getGraphSize(self, sessionId):
        """
        Returns the number of drops contained in the physical graph attached
        to ``sessionId``.
        """