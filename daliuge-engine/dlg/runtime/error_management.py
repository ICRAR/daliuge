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
Functions and data structures to improve user-facing debugging and manage execptions
cleanly during runtime.
"""

import dill
import pickle
import logging
import dlg.exceptions as ex

from enum import Enum, auto


class ErrorCode(Enum):
    """
    Enum for Errors that occur during runtime execution on the engine.
    We use `Enum` for the auto-incremementer, and because we can use the autodoc feature
    of Sphinx to generate
    """

    DROP_ERROR = 100
    """
    An error occured during DROP initialization that was not expected. Please 
    double-check your current graph and attempt to resolve the issue based on the 
    debugging tips for DROP_ERROR codes below. 
    
    If none of the fixes address your isse, please consider opening an issue on our GitHub
    repository `here <https://github.com/ICRAR/daliuge/issues/new/choose>`_.
    """

    INCOMPLETE_DROP_SPEC = auto()
    """
    The DROP being initalized was not complete. This means that key parameters in the 
    have not been filled. If you are using a PyFuncApp, double-check you have filled in 
    both the ``func_name`` parameter if you are filling in the ``func_code`` parameter 
    too.
    """

    BAD_IMPORT = auto()
    """
    There was an issue importing the module. Consider:
    
    - Reviewing the current environment
    - Reviewing the spelling
    - Ensuring there are no missing dependencies
    """

    ENCODING_ERROR = auto()
    """
    There was an issue with reading/writing data in your drop.
    This is likely caused by using the wrong 'encoding' option when configuring
    your graph in EAGLE. Please ensure that: 
        
        - The OutputPort/InputPort on your Apps are using the same encoding
        - The Encoding matches the Type (e.g. String types are using UTF-8 encoding)
        - The DataDROP matches the right type/encoding (e.g. Using a File for its path is 
          using 'path' encoding'). 
        
    These changes will need to be added to the EAGLE graph. 
    """

    PATH_ERROR = auto()
    """
    There was an issue with the path set for this DROP, which may becaused by the 
    following: 
    
        - The file does not exist and/or you have set check_filepath_exists to be 
          True.
        - The directory does not exist at runtime
        - An environment variable that is being referred to has not been set and cannot 
          be expanded
        
    Please review the DataDROP parameter values for this DROP.    
    """

    BASH_COMMAND_FAILED = auto()
    """
    There was an issue running your BashShellApp DROP. This may be caused by: 
    
        - The application cannot find the program you are running (e.g. cannot find the
          script that is being referred to)
        - The arguments applied to the command line are incorrectly formatted; for 
          example, incorrect flags, or the wrong redirect was used. 
    """

    MEMORY_DROP_TYPE_ERROR = auto()
    """
    There was an issue reading/writing to a MemoryDROP. This is likely caused by the 
    PyData type being inconsistent with the data that is actually being written. 
    
    Make sure that if you are expecting string data to be stored in the memory drop, 
    that the type dropdown box is set to "String"; if it is not string, set it to an 
    alternative type. 
    """

    GRAPH_ERROR = 200
    """
    An error has occured during graph execution that was not expected. Please 
    double-check your current graph and attempt to resolve the issue based on the 
    debugging tips for SESSION_ERROR codes below. 

    If none of the fixes address your isse, please consider opening an issue on our GitHub
    repository `here <https://github.com/ICRAR/daliuge/issues/new/choose>`_.
    """

    INVALID_GRAPH_CONFIGURATION = auto()
    """
    The graph has failed to be loaded by the DALiuGE Engine. This can be caused by number 
    of issues: 

        - A python module that your graph relies on is not currently accessible in the 
          virtual environment in which DALiuGE is being run. 
        - The graph is missing key arguments - please check the JSON is complete.
    """

    SESSION_ERROR = 300
    """
    An error has occured during your session that was not expected. Please 
    double-check your current graph and attempt to resolve the issue based on the 
    debugging tips for SESSION_ERROR codes below. 
    
    If none of the fixes address your isse, please consider opening an issue on our GitHub
    repository `here <https://github.com/ICRAR/daliuge/issues/new/choose>`_ and 
    attaching the log files.
    """

    SESSION_STATE_ERROR = auto()
    """
    The session has been put in a state is was not expecting. This could be the 
    result of a synchronisation issue between the managers; consider restarting them 
    and re-running your session. 
    """

    SESSION_MISSING_ERROR = auto()
    """
    A session was not found that was requested. This typically occurs when restarting 
    the Node Manager(s) when a Data Island Manager is still running. 
    
    If a Node Manager was not restarted, please reveiw the logs and if the issue 
    persists, consider opening an issue on our GitHub repository `here 
    <https://github.com/ICRAR/daliuge/issues/new/choose>`_ and 
    attaching the log files.
    """

    REMOTE_SESSION_RUNTIME_ERROR = auto()
    """
    An error occured during the execution of a remote session. At the moment, 
    known causes of this error are: 
        
        - A SlurmClient PyFunc app has wait = True and submit = False. Submit must be 
          set to True if you want to `wait` for the remote subgraph to deploy. 
    """

    @property
    def doc_url(self) -> str:
        log_message = f"Error [{self.value}] - {self.name}"
        path = (f"https://daliuge--344.org.readthedocs.build/page/debugging/errors"
                   f".html"
                f"#{__name__}.{str(self)}")
        # if os.environ.get('READTHEDOCS', None) == 'True':
        # path =  f"https://daliuge.readthedocs.org/page/errors.html#{self.name}"
        href = f"<a href={path} target='_blank' rel='noopener noreferrer'>{path}</a>'"
        return f"{log_message} occured: Please review potential issues at\n {href}"


EXCEPTION_MAP = {

    #################################################################
    # Custom DALiUGE Exceptions
    # 100 - Exceptions that inheret generic DropException
    # 200 - Exceptions that inheret GraphException
    # 300 - Exceptions that inheret SessionException
    #################################################################

    ex.InvalidDropException: ErrorCode.DROP_ERROR,
    ex.BadModuleException: ErrorCode.BAD_IMPORT,
    ex.InvalidEncodingException: ErrorCode.ENCODING_ERROR,
    ex.InvalidPathException: ErrorCode.PATH_ERROR,
    ex.IncompleteDROPSpec: ErrorCode.INCOMPLETE_DROP_SPEC,
    ex.BashAppRuntimeError: ErrorCode.BASH_COMMAND_FAILED,
    ex.MemoryDROPTypeError: ErrorCode.MEMORY_DROP_TYPE_ERROR,
    ex.InvalidGraphException: ErrorCode.GRAPH_ERROR,
    ex.IncompleteGraphError: ErrorCode.INVALID_GRAPH_CONFIGURATION,
    ex.InvalidRelationshipException: ErrorCode.INVALID_GRAPH_CONFIGURATION,
    ex.InvalidSessionException: ErrorCode.SESSION_ERROR,
    ex.InvalidSessionState: ErrorCode.SESSION_STATE_ERROR,
    ex.NoSessionException: ErrorCode.SESSION_MISSING_ERROR,
    ex.RemoteSessionRuntimeException: ErrorCode.REMOTE_SESSION_RUNTIME_ERROR,
    ###################################################################
    # Standard errors that we may not catch with specific exceptions
    # based on where they might occur in the code, _but_ where we
    # are confident the cause can be resolved using our error debugging
    ###################################################################

    pickle.PickleError: ErrorCode.ENCODING_ERROR,
    dill.PickleError: ErrorCode.ENCODING_ERROR
}


def intercept_error(e: Exception):
    """
    Intercept DALiuGEExceptions during App/DataDROP runtime.

    Some exceptions we want to report and continue on running the session;
    other exceptions will leave us in an unmanageable state and thus we need
    to re-raise them to an alternative error to promote session cancellation
    prematurely.

    :param: e: The exception we have intercepted
    """

    logger = logging.getLogger(f"dlg.{__name__}")
    if type(e) != ex.ErrorManagerCaughtException:
        errorno = EXCEPTION_MAP.get(type(e), ErrorCode.DROP_ERROR)
        if hasattr(e, "reason"):
            logger.error(e.reason)
        logger.warning(errorno.doc_url)
    raise ex.ErrorManagerCaughtException from e


def manage_session_failure(func):
    """
    @decorator
    Intercept a session failure that has occured and set the session as cancelled.

    This is used to stop us in a situation where the session is in an incongruous state
    (e.g. a graph has failed to build mid-session) and we have no logic to manage this 
    in a safe way. If we detect this sort of exception has occured, we 'clean up' by 
    cancelling the session and reporting the error. 
    
    :param func: The function to which we have added this dectorator
    :return:
    """
    def manage_session(self, *args, **kwargs):
        """
        Attempt to run the decorated function 
        :param self: The session object we expect this to run within 
        :param args: 
        :param kwargs: 
        :return: If successful, the result of `func`.
        """
        try:
            return func(self, *args, **kwargs)
        except ex.InvalidSessionState:
            # sessionId = kwargs.get('sessionId')
            if args:
                session = self.sessions[next(iter(args))]
                if session:
                    session.cancel()
                return

    return manage_session
