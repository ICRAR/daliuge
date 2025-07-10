#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2014
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
Module containing the core DROP classes.
"""
import ast
import inspect
import logging
import os
import threading
import time
import re
import sys
from abc import ABCMeta

from dlg.common.reproducibility.constants import (
    ReproducibilityFlags,
    REPRO_DEFAULT,
    rmode_supported,
    ALL_RMODES,
)
from dlg.common.reproducibility.reproducibility import common_hash
from merklelib import MerkleTree

from .ddap_protocol import (
    ExecutionMode,
    ChecksumTypes,
    DROPLinkType,
    DROPPhases,
    DROPStates,
    DROPRel,
)
from dlg.event import EventFirer, EventHandler
from dlg.exceptions import InvalidDropException, InvalidRelationshipException

DEFAULT_INTERNAL_PARAMETERS = {
    "dropclass",
    "category",
    "storage",
    "fields",
    "streaming",
    "persist",
    "rank",
    "loop_ctx",
    "weight",
    "iid",
    "consumers",
    "config_data",
    "mode",
    "group_end",
    "applicationArgs",
    "reprodata",
}

if sys.version_info >= (3, 8):
    pass
from dlg.utils import (
    object_tracking,
    getDlgVariable,
    truncateUidToKey,
)
from dlg.meta import (
    dlg_float_param,
    dlg_int_param,
    dlg_list_param,
    dlg_string_param,
    dlg_enum_param,
    dlg_bool_param,
    dlg_dict_param,
)

# Opt into using per-drop checksum calculation
checksum_disabled = "DLG_DISABLE_CHECKSUM" in os.environ
try:
    from crc32c import crc32c  # pylint: disable=unused-import

    _checksumType = ChecksumTypes.CRC_32C
except (ModuleNotFoundError, ImportError):
    from binascii import crc32 # pylint: disable=unused-import

    _checksumType = ChecksumTypes.CRC_32

logger = logging.getLogger(f"dlg.{__name__}")


class ListAsDict(list):
    """A list that adds drop UIDs to a set as they get appended to the list"""

    def __init__(self, my_set):
        self.set = my_set

    def append(self, drop):
        super(ListAsDict, self).append(drop)
        self.set.add(drop.uid)


track_current_drop = object_tracking("drop")


# ===============================================================================
# DROP classes follow
# ===============================================================================


class AbstractDROP(EventFirer, EventHandler):
    """
    Base class for all DROP implementations.

    A DROP is a representation of a piece of data. DROPs are created,
    written once, potentially read many times, and they finally potentially
    expire and get deleted. Subclasses implement different storage mechanisms
    to hold the data represented by the DROP.

    If the data represented by this DROP is written *through* this object
    (i.e., calling the ``write`` method), this DROP will keep track of the
    data's size and checksum. If the data is written externally, the size and
    checksum can be fed into this object for future reference.

    DROPs can have consumers attached to them. 'Normal' consumers will
    wait until the DROP they 'consume' (their 'input') moves to the
    COMPLETED state and then will consume it, most typically by opening it
    and reading its contents, but any other operation could also be performed.
    How the consumption is triggered depends on the producer's `executionMode`
    flag, which dictates whether it should trigger the consumption itself or
    if it should be manually triggered by an external entity. On the other hand,
    streaming consumers receive the data that is written into its input
    *as it gets written*. This mechanism is driven always by the DROP that
    acts as a streaming input. Apart from receiving the data as it gets
    written into the DROP, streaming consumers are also notified when the
    DROPs moves to the COMPLETED state, at which point no more data should
    be expected to arrive at the consumer side.

    DROPs' data can be expired automatically by the system after the DROP has
    transitioned to the COMPLETED state if they are created by a DROP Manager.
    Expiration can either be triggered by an interval relative to the creation
    time of the DROP (via the `lifespan` keyword), or by specifying that the
    DROP should be expired after all its consumers have finished (via the
    `expireAfterUse` keyword). These two methods are mutually exclusive. If none
    is specified no expiration occurs.
    """

    # This ensures that:
    #  - This class cannot be instantiated
    #  - Subclasses implement methods decorated with @abstractmethod
    __metaclass__ = ABCMeta

    # Matcher used to validate environment_variable_syntax
    _env_var_matcher = re.compile(r"\$[A-z|\d]+\..+")
    _dlg_var_matcher = re.compile(r"\$DLG_.+")

    _known_locks = ("_finishedProducersLock", "_refLock")
    _known_rlocks = ("_statusLock",)

    def __getstate__(self):
        state = self.__dict__.copy()
        for attr_name in AbstractDROP._known_locks + AbstractDROP._known_rlocks:
            del state[attr_name]
        del state["_listeners"]
        return state

    def __setstate__(self, state):
        for attr_name in AbstractDROP._known_locks:
            state[attr_name] = threading.Lock()
        for attr_name in AbstractDROP._known_rlocks:
            state[attr_name] = threading.RLock()

        self.__dict__.update(state)

        import collections

        self._listeners = collections.defaultdict(list)

    @track_current_drop
    def __init__(self, oid, uid, **kwargs):
        """
        Creates a DROP. The only mandatory argument are the Object ID
        (`oid`) and the Unique ID (`uid`) of the new object (see the `self.oid`
        and `self.uid` methods for more information). Any extra arguments must
        be keyed, and will be processed either by this method, or by the
        `initialize` method.

        This method should not be overwritten by subclasses. For any specific
        initialization logic, the `initialize` method should be overwritten
        instead. This allows us to move to the INITIALIZED state only after any
        specific initialization has occurred in the subclasses.
        """

        super(AbstractDROP, self).__init__()

        if "_dlg_session_id" not in kwargs and "dlg_session_id" in kwargs:
            kwargs["_dlg_session_id"] = kwargs["dlg_session_id"]
        self._extract_attributes(**kwargs)

        # Copy it since we're going to modify it
        kwargs = dict(kwargs)
        # So far only these three are mandatory
        self._oid = str(oid)
        self._uid = str(uid)

        # Set log_level for this drop to level provided
        self._log_level = self._popArg(
            kwargs, "log_level", logging.getLevelName(logger.getEffectiveLevel())
        )
        if self._log_level == "" or self._log_level == logging.getLevelName(
                logging.NOTSET
        ):
            self._log_level = logging.getLevelName(logger.getEffectiveLevel())
        self._global_log_level = logging.getLevelName(logger.getEffectiveLevel())

        # The physical graph drop type. This is determined
        # by the drop category when generating the drop spec
        self._type = self._popArg(kwargs, "categoryType", None)

        # The ID of the session owning this drop, if any
        # In most real-world situations this attribute will be set, but in
        # general it cannot be assumed it will (e.g., unit tests create drops
        # directly outside the context of a session).
        self._dlg_session_id = self._popArg(kwargs, "dlg_session_id", "")

        # A simple name that the Drop might receive
        # This is usually set in the Logical Graph Editor,
        # but is not necessarily always there
        self.name = self._popArg(kwargs, "name", self._oid)

        # The key of this drop in the original Logical Graph
        # This information might or might not be present depending on how the
        # physical graph was generated (or if this drop is being created as part
        # of a graph, to begin with), so we default it to an empty value
        self.lg_key = self._popArg(kwargs, "lg_key", "")

        # 1-to-N relationship: one DROP may have many consumers and producers.
        # The potential consumers and producers are always AppDROPs instances
        # We keep the UIDs in a set for O(1) "x in set" operations
        # Obviously the normal way of doing this is using a dictionary, but
        # for the time being and while testing the integration with TBU's ceda
        # library we need to expose a list.
        self._consumers_uids = set()
        self._consumers = ListAsDict(self._consumers_uids)
        self._producers_uids = set()
        self._producers = ListAsDict(self._producers_uids)

        # Set holding the state of the producers that have finished their
        # execution. Once all producers have finished, this DROP moves
        # itself to the COMPLETED state
        self._finishedProducers = []
        self._finishedProducersLock = threading.Lock()

        # Streaming consumers are objects that consume the data written in
        # this DROP *as it gets written*, and therefore don't have to
        # wait until this DROP has moved to COMPLETED.
        # An object cannot be a streaming consumers and a 'normal' consumer
        # at the same time, although this rule is imposed simply to enforce
        # efficiency (why would a consumer want to consume the data twice?) and
        # not because it's technically impossible.
        # See comment above in self._consumers/self._producers for separate set
        # with uids
        self._streamingConsumers_uids = set()
        self._streamingConsumers = ListAsDict(self._streamingConsumers_uids)

        self._refCount = 0
        self._refLock = threading.Lock()
        self._location = None
        self._parent = None
        self._status = None
        self._statusLock = threading.RLock()

        # Current and target phases.
        # Phases represent the resiliency of data. An initial phase of PLASMA
        # is set on each DROP representing its lack of non-transient storage
        # support. A target phase is also set to hint the Data Lifecycle Manager
        # about the level of resilience that this DROP should achieve.
        self._phase = DROPPhases.PLASMA
        self._targetPhase = self._popArg(kwargs, "targetPhase", DROPPhases.GAS)

        # Calculating the checksum and maintaining the data size internally
        # implies that the data represented by this DROP is written
        # *through* this DROP. This might not always be the case though,
        # since data could be written externally and the DROP simply be
        # moved to COMPLETED at the end of the process. In this case we return a
        # None checksum and size (when requested), signaling that we don't have
        # this information.
        # Note also that the setters of these two properties also allow to set
        # a value on them, but only if they are None
        self._checksum = None
        self._checksumType = None
        self._size = None

        # Recording runtime reproducibility information is handled via MerkleTrees
        # Switching on the reproducibility level will determine what information is recorded.
        self._committed = False
        self._merkleRoot = None
        self._merkleTree = None
        self._merkleData = []
        self._reproducibility = REPRO_DEFAULT

        # The DataIO instance we use in our write method. It's initialized to
        # None because it's lazily initialized in the write method, since data
        # might be written externally and not through this DROP
        self._wio = None

        # DataIO objects used for reading.
        # Instead of passing file objects or more complex data types in our
        # open/read/close calls we use integers, mainly because Pyro doesn't
        # handle file types and other classes (like StringIO) well, but also
        # because it requires less transport.
        # TODO: Make these threadsafe, no lock around them yet
        self._rios = {}

        self._humanKey = self._popArg(
            kwargs, "humanReadableKey", truncateUidToKey(self._uid)
        )
        # The execution mode.
        # When set to DROP (the default) the graph execution will be driven by
        # DROPs themselves by firing and listening to events, and reacting
        # accordingly by executing themselves or moving to the COMPLETED state.
        # When set to EXTERNAL, DROPs do no react to these events, and remain
        # in the state they currently are. In this case an external entity must
        # listen to the events and decide when to trigger the execution of the
        # applications.
        self._executionMode = self._popArg(kwargs, "executionMode", ExecutionMode.DROP)

        # The physical node where this DROP resides.
        # This piece of information is mandatory when submitting the physical
        # graph via the DataIslandManager, but in simpler scenarios such as
        # tests or graph submissions via the NodeManager it might be
        # missing.
        self._node = self._popArg(kwargs, "node", None)

        # The host representing the Data Island where this DROP resides
        # This piece of information is mandatory when submitting the physical
        # graph via the MasterManager, but in simpler scenarios such as tests or
        # graphs submissions via the DataIslandManager or NodeManager it might
        # missing.
        self._dataIsland = self._popArg(kwargs, "island", None)

        # DROP expiration.
        # Expiration can be time-driven or usage-driven, which are mutually
        # exclusive methods. If time-driven, a relative lifespan is assigned to
        # the DROP.
        # Expected lifespan for this object, used by to expire them
        if "lifespan" in kwargs and "expireAfterUse" in kwargs:
            raise InvalidDropException(
                self,
                "%r specifies both `lifespan` and `expireAfterUse`"
                "but they are mutually exclusive" % (self,),
            )

        self._expireAfterUse = self._popArg(kwargs, "expireAfterUse", False)
        self._expirationDate = -1
        if not self._expireAfterUse:
            lifespan = float(self._popArg(kwargs, "lifespan", -1))
            if lifespan != -1:
                self._expirationDate = time.time() + lifespan

        # Expected data size, used to automatically move the DROP to COMPLETED
        # after successive calls to write()
        self._expectedSize = -1
        if "expectedSize" in kwargs and kwargs["expectedSize"]:
            self._expectedSize = int(kwargs.pop("expectedSize"))

        # No DROP should be persisted unless stated otherwise; used for replication
        self._persist: bool = self._popArg(kwargs, "persist", False)
        # If DROP should be persisted, don't expire (delete) it.
        if self._persist:
            self._expireAfterUse = False

        # Flag to control whether a call to skip blocks until the
        # last producer is finished. This is useful for data drops capturing
        # the output of multiple branches. Default is false, meaning that
        # the first call to skip will close the drop and also skip the following
        # drops.
        self.block_skip = self._popArg(kwargs, "block_skip", False)

        # Useful to have access to all EAGLE parameters without a prior knowledge
        self._parameters = dict(kwargs)
        self.autofill_environment_variables()
        kwargs.update(self._parameters)
        # Sub-class initialization; mark ourselves as INITIALIZED after that
        self.initialize(**kwargs)
        self._status = (
            DROPStates.INITIALIZED
        )  # no need to use synchronised self.status here

    _members_cache = {}

    def _get_members(self):
        cls = self.__class__
        if cls not in AbstractDROP._members_cache:
            members = [
                (name, val)
                for c in cls.__mro__[:-1]
                for name, val in vars(c).items()
                if not (inspect.isfunction(val) or isinstance(val, property))
            ]
            # logger.info("GOT MEMBEEEERS: %r", members)

            AbstractDROP._members_cache[cls] = members
        return AbstractDROP._members_cache[cls]

    def _extract_attributes(self, **kwargs):
        """
        Extracts component and app params then assigns them to class instance attributes.
        Component params take pro
        """

        def get_param_value(attr_name, default_value):

            if attr_name in kwargs:
                return kwargs.get(attr_name)
            elif "applicationArgs" in kwargs and attr_name in kwargs["applicationArgs"] and (
                kwargs["applicationArgs"].get(attr_name).usage not in [
                    "InputPort",
                    "OutputPort",
                    "InputOutput",
                    ]
                ):
                    return kwargs["applicationArgs"].get(attr_name).value
            else:
                return default_value

        # Take a class dlg defined parameter class attribute and create an instanced attribute on object
        for attr_name, member in self._get_members():
            if isinstance(member, dlg_float_param):
                value = get_param_value(attr_name, member.default_value)
                if value is not None and value != "":
                    value = float(value)
            elif isinstance(member, dlg_bool_param):
                value = get_param_value(attr_name, member.default_value)
                if value is not None and value != "":
                    value = bool(value)
            elif isinstance(member, dlg_int_param):
                value = get_param_value(attr_name, member.default_value)
                if value is not None and value != "":
                    value = int(value)
            elif isinstance(member, dlg_string_param):
                value = get_param_value(attr_name, member.default_value)
                if value is not None and value != "":
                    value = str(value)
            elif isinstance(member, dlg_enum_param):
                value = get_param_value(attr_name, member.default_value)
                if value is not None and value != "":
                    value = member.cls(value)
            elif isinstance(member, dlg_list_param):
                value = get_param_value(attr_name, member.default_value)
                if isinstance(value, str):
                    if value == "":
                        value = []
                    else:
                        value = ast.literal_eval(value)
                if value is not None and not isinstance(value, list):
                    raise TypeError(
                        f"dlg_list_param {attr_name} is not a list. Type is {type(value)}"
                    )
            elif isinstance(member, dlg_dict_param):
                value = get_param_value(attr_name, member.default_value)
                if isinstance(value, str):
                    if value == "":
                        value = {}
                    else:
                        value = ast.literal_eval(value)
                if value is not None and not isinstance(value, dict):
                    raise TypeError(
                        "dlg_dict_param {} is not a dict. It is a {}".format(
                            attr_name, type(value)
                        )
                    )
            else:
                continue
            setattr(self, attr_name, value)

    def _popArg(self, kwargs, key, default):
        """
        Pops the specified key arg from kwargs else returns the default
        """
        if key not in kwargs:
            logger.debug("Defaulting %s to %s in %r", key, str(default), self)
        return kwargs.pop(key, default)

    def __hash__(self):
        return hash(self._uid)

    def __repr__(self):
        return "<%s oid=%s, uid=%s>" % (
            self.__class__.__name__,
            self.oid,
            self.uid,
        )

    def initialize(self, **kwargs):
        """
        Performs any specific subclass initialization.

        `kwargs` contains all the keyword arguments given at construction time,
        except those used by the constructor itself. Implementations of this
        method should make sure that arguments in the `kwargs` dictionary are
        removed once they are interpreted so they are not interpreted by
        accident by another method implementations that might reside in the call
        hierarchy (in the case that a subclass implementation calls the parent
        method implementation, which is usually the case).
        """

    def autofill_environment_variables(self):
        """
        Runs through all parameters here, fetching those which match the env-var syntax when
        discovered.
        """
        for param_key, param_val in self.parameters.items():
            if self._env_var_matcher.fullmatch(str(param_val)):
                self.parameters[param_key] = self.get_environment_variable(param_val)
            if self._dlg_var_matcher.fullmatch(str(param_val)):
                self.parameters[param_key] = getDlgVariable(param_val)

    def get_environment_variable(self, key: str):
        """
        Expects keys of the form $store_name.var_name
        $store_name.var_name.sub_var_name will query store_name for var_name.sub_var_name
        """
        if self._dlg_var_matcher.fullmatch(key):
            return getDlgVariable(key)
        if len(key) < 2 or key[0] != "$":
            # Reject malformed entries
            return key
        key_edit = key[1:]
        env_var_ref, env_var_key = (
            key_edit.split(".")[0],
            ".".join(key_edit.split(".")[1:]),
        )
        env_var_drop = None
        for producer in self._producers:
            if producer.name == env_var_ref:
                env_var_drop = producer
        if env_var_drop is not None:  # TODO: Check for KeyValueDROP interface support
            ret_val = env_var_drop.get(env_var_key)
            if ret_val is None:
                return key
            return ret_val
        else:
            return key

    def get_environment_variables(self, keys: list):
        """
        Expects multiple instances of the single key form
        """
        return_values = []
        for key in keys:
            # TODO: Accumulate calls to the same env_var_store to save communication
            return_values.append(self.get_environment_variable(key))
        return return_values

    @property
    def merkleroot(self):
        return self._merkleRoot

    @property
    def reproducibility_level(self):
        return self._reproducibility

    @reproducibility_level.setter
    def reproducibility_level(self, new_flag):
        if type(new_flag) != ReproducibilityFlags:
            raise TypeError("new_flag must be a reproducibility flag enum.")
        elif rmode_supported(new_flag):  # TODO: Support custom checkers for repro-level
            self._reproducibility = new_flag
            if new_flag == ReproducibilityFlags.ALL:
                self._committed = False
                self._merkleRoot = {rmode.name: None for rmode in ALL_RMODES}
                self._merkleTree = {rmode.name: None for rmode in ALL_RMODES}
                self._merkleData = {rmode.name: [] for rmode in ALL_RMODES}
            elif self._committed:
                # Current behaviour, set to un-committed again after change
                self._committed = False
                self._merkleRoot = None
                self._merkleTree = None
                self._merkleData = []
        else:
            raise NotImplementedError("new_flag %d is not supported" % new_flag.value)

    def generate_rerun_data(self):
        """
        Provides a serailized list of Rerun data.
        At runtime, Rerunning only requires execution success or failure.
        :return: A dictionary containing rerun values
        """
        return {"status": self._status}

    def generate_repeat_data(self):
        """
        Provides a list of Repeat data.
        At runtime, repeating, like rerunning only requires execution success or failure.
        :return: A dictionary containing runtime exclusive repetition values.
        """
        return {"status": self._status}

    def generate_recompute_data(self):
        """
        Provides a dictionary containing recompute data.
        At runtime, recomputing, like repeating and rerunning, by default, only shows success or failure.
        We anticipate that any further implemented behaviour be done at a lower class.
        :return: A dictionary containing runtime exclusive recompute values.
        """
        return {"status": self._status}

    def generate_reproduce_data(self):
        """
        Provides a list of Reproducibility data (specifically).
        The default behaviour is to return nothing. Per-class behaviour is to be achieved by overriding this method.
        :return: A dictionary containing runtime exclusive reproducibility data.
        """
        return {}

    def generate_replicate_sci_data(self):
        """
        Provides a list of scientific replication data.
        This is by definition a merging of both reproduction and rerun data
        :return: A dictionary containing runtime exclusive scientific replication data.
        """
        res = {}
        res.update(self.generate_rerun_data())
        res.update(self.generate_reproduce_data())
        return res

    def generate_replicate_comp_data(self):
        """
        Provides a list of computational replication data.
        This is by definition a merging of both reproduction and recompute data
        :return: A dictionary containing runtime exclusive computational replication data.
        """
        res = {}
        recomp_data = self.generate_recompute_data()
        if recomp_data is not None:
            res.update(self.generate_recompute_data())
        res.update(self.generate_reproduce_data())
        return res

    def generate_replicate_total_data(self):
        """
        Provides a list of total replication data.
        This is by definition a merging of reproduction and repetition data
        :return: A dictionary containing runtime exclusive total replication data.
        """
        res = {}
        res.update(self.generate_repeat_data())
        res.update(self.generate_reproduce_data())
        return res

    def generate_merkle_data(self):
        """
        Provides a serialized summary of data as a list.
        Fields constitute a single entry in this list.
        Wraps several methods dependent on this DROPs reproducibility level
        Some of these are abstract.
        :return: A dictionary of elements constituting a summary of this drop
        """
        if self._reproducibility is ReproducibilityFlags.NOTHING:
            return {}
        elif self._reproducibility is ReproducibilityFlags.RERUN:
            return self.generate_rerun_data()
        elif self._reproducibility is ReproducibilityFlags.REPEAT:
            return self.generate_repeat_data()
        elif self._reproducibility is ReproducibilityFlags.RECOMPUTE:
            return self.generate_recompute_data()
        elif self._reproducibility is ReproducibilityFlags.REPRODUCE:
            return self.generate_reproduce_data()
        elif self._reproducibility is ReproducibilityFlags.REPLICATE_SCI:
            return self.generate_replicate_sci_data()
        elif self._reproducibility is ReproducibilityFlags.REPLICATE_COMP:
            return self.generate_replicate_comp_data()
        elif self._reproducibility is ReproducibilityFlags.REPLICATE_TOTAL:
            return self.generate_replicate_total_data()
        elif self._reproducibility is ReproducibilityFlags.ALL:
            return {
                ReproducibilityFlags.RERUN.name: self.generate_rerun_data(),
                ReproducibilityFlags.REPEAT.name: self.generate_repeat_data(),
                ReproducibilityFlags.RECOMPUTE.name: self.generate_recompute_data(),
                ReproducibilityFlags.REPRODUCE.name: self.generate_reproduce_data(),
                ReproducibilityFlags.REPLICATE_SCI.name: self.generate_replicate_sci_data(),
                ReproducibilityFlags.REPLICATE_COMP.name: self.generate_replicate_comp_data(),
                ReproducibilityFlags.REPLICATE_TOTAL.name: self.generate_replicate_total_data(),
            }
        else:
            raise NotImplementedError("Currently other levels are not in development.")

    def commit(self):
        """
        Generates the MerkleRoot of this DROP
        Should only be called once this DROP is completed.
        """
        if not self._committed:
            #  Generate the MerkleData
            self._merkleData = self.generate_merkle_data()
            if self._reproducibility == ReproducibilityFlags.ALL:
                for rmode in ALL_RMODES:
                    self._merkleTree[rmode.name] = MerkleTree(
                        self._merkleData[rmode.name].items(), common_hash
                    )
                    self._merkleRoot[rmode.name] = self._merkleTree[
                        rmode.name
                    ].merkle_root
            else:
                # Fill MerkleTree, add data and set the MerkleRoot Value
                self._merkleTree = MerkleTree(self._merkleData.items(), common_hash)
                self._merkleRoot = self._merkleTree.merkle_root
                # Set as committed
            self._committed = True
        else:
            logger.debug("Trying to re-commit DROP %s, cannot overwrite.", self)

    @property
    def oid(self):
        """
        The DROP's Object ID (OID). OIDs are unique identifiers given to
        semantically different DROPs (and by consequence the data they
        represent). This means that different DROPs that point to the same
        data semantically speaking, either in the same or in a different
        storage, will share the same OID.
        """
        return self._oid

    @property
    def uid(self):
        """
        The DROP's Unique ID (UID). Unlike the OID, the UID is globally
        different for all DROP instances, regardless of the data they
        point to.
        """
        return self._uid

    @property
    def type(self):
        return self._type

    @property
    def executionMode(self):
        """
        The execution mode of this DROP. If `ExecutionMode.DROP` it means
        that this DROP will automatically trigger the execution of all its
        consumers. If `ExecutionMode.EXTERNAL` it means that this DROP
        will *not* trigger its consumers, and therefore an external entity will
        have to do it.
        """
        return self._executionMode

    def handleInterest(self, drop):
        """
        Main mechanism through which a DROP handles its interest in a
        second DROP it isn't directly related to.

        A call to this method should be expected for each DROP this
        DROP is interested in. The default implementation does nothing,
        but implementations are free to perform any action, such as subscribing
        to events or storing information.

        At this layer only the handling of such an interest exists. The
        expression of such interest, and the invocation of this method wherever
        necessary, is currently left as a responsibility of the entity creating
        the DROPs. In the case of a Session in a DROPManager for
        example this step would be performed using deployment-time information
        contained in the dropspec dictionaries held in the session.
        """

    def _fire(self, eventType: str, **kwargs):
        """
        Delivers an event of `eventType` to all interested listeners.

        All the key-value pairs contained in `attrs` are set as attributes of
        the event being sent. On top of that, the `uid` and `oid` attributes are
        also added, carrying the uid and oid of the current DROP, respectively.
        """
        kwargs["oid"] = self.oid
        kwargs["uid"] = self.uid
        kwargs["session_id"] = self._dlg_session_id
        kwargs["name"] = self.name
        kwargs["lg_key"] = self.lg_key
        self._fireEvent(eventType, **kwargs)

    @property
    def phase(self):
        """
        This DROP's phase. The phase indicates the resilience of a DROP.
        """
        return self._phase

    @phase.setter
    def phase(self, phase):
        self._phase = phase

    @property
    def targetPhase(self):
        return self._targetPhase

    @property
    def expirationDate(self):
        return self._expirationDate

    @property
    def expireAfterUse(self):
        return self._expireAfterUse

    @property
    def size(self):
        """
        The size of the data pointed by this DROP. Its value is
        automatically calculated if the data was actually written through this
        DROP (using the `self.write()` method directly or indirectly). In
        the case that the data has been externally written, the size can be set
        externally after the DROP has been moved to COMPLETED or beyond.
        """
        return self._size

    @size.setter
    def size(self, size):
        if self._size is not None:
            raise RuntimeError(
                "The size of DROP %s is already calculated, cannot overwrite with new value"
                % (self)
            )
        if self.status in [DROPStates.INITIALIZED, DROPStates.WRITING]:
            raise RuntimeError(
                "DROP %s is still not fully written, cannot manually set a size yet"
                % (self)
            )
        self._size = size

    @property
    def persist(self):
        """
        Whether this DROP should be considered persisted after completion
        """
        return self._persist

    @property
    def status(self):
        """
        The current status of this DROP.
        """
        with self._statusLock:
            return self._status

    @status.setter
    def status(self, value):
        with self._statusLock:
            # if we are already in the state that is requested then do nothing
            if value == self._status:
                return
            self._status = value

        self._fire("status", status=value)

    @property
    def parent(self):
        """
        The DROP that acts as the parent of the current one. This
        parent/child relationship is created by ContainerDROPs, which are
        a specific kind of DROP.
        """
        return self._parent

    @parent.setter
    @track_current_drop
    def parent(self, parent):
        if self._parent and parent:
            logger.warning(
                "A parent is already set in %r, overwriting with new value",
                self.oid,
            )
        if parent:
            prevParent = self._parent
            self._parent = parent  # a parent is a container
            if hasattr(parent, "addChild") and self not in parent.children:
                try:
                    parent.addChild(self)
                except InvalidRelationshipException:
                    self._parent = prevParent

    def get_consumers_nodes(self):
        """
        Gets the physical node address(s) of the consumer of this drop.
        """
        return [cons.node for cons in self._consumers] + [
            cons.node for cons in self._streamingConsumers
        ]

    @property
    def consumers(self):
        """
        The list of 'normal' consumers held by this DROP.

        :see: `self.addConsumer()`
        """
        return self._consumers[:]

    @track_current_drop
    def addConsumer(self, consumer, back=True):
        """
        Adds a consumer to this DROP.

        Consumers are normally (but not necessarily) AppDROPs that get
        notified when this DROP moves into the COMPLETED or ERROR states.
        This is done by firing an event of type `dropCompleted` to which the
        consumer subscribes to.

        This is one of the key mechanisms by which the DROP graph is
        executed automatically. If AppDROP B consumes DROP A, then
        as soon as A transitions to COMPLETED B will be notified and will
        probably start its execution.
        """

        # An object cannot be a normal and streaming consumer at the same time,
        # see the comment in the __init__ method
        cuid = consumer.uid
        if cuid in self._streamingConsumers_uids:
            raise InvalidRelationshipException(
                DROPRel(consumer, DROPLinkType.CONSUMER, self),
                "Consumer already registered as a streaming consumer",
            )

        # Add if not already present
        # Add the reverse reference too automatically
        if cuid in self._consumers_uids:
            return
        # logger.debug("Adding new consumer %r to %r", consumer.oid, self.oid)
        self._consumers.append(consumer)

        # Subscribe the consumer to events sent when this DROP moves to
        # COMPLETED. This way the consumer will be notified that its input has
        # finished.
        # This only happens if this DROP's execution mode is 'DROP'; otherwise
        # an external entity will trigger the execution of the consumer at the
        # right time
        if self.executionMode == ExecutionMode.DROP:
            self.subscribe(consumer, "dropCompleted")

        # Automatic back-reference
        if back and hasattr(consumer, "addInput"):
            logger.debug("Adding back %r as input of %r", self.oid, consumer)
            consumer.addInput(self, False)

        # Add reproducibility subscription
        self.subscribe(consumer, "reproducibility")

    @property
    def producers(self):
        """
        The list of producers that write to this DROP

        :see: `self.addProducer()`
        """
        return self._producers[:]

    @track_current_drop
    def addProducer(self, producer, back=True):
        """
        Adds a producer to this DROP.

        Producers are AppDROPs that write into this DROP; from the
        producers' point of view, this DROP is one of its many outputs.

        When a producer has finished its execution, this DROP will be
        notified via the self.producerFinished() method.
        """

        # Don't add twice
        puid = producer.uid
        if puid in self._producers_uids:
            return

        self._producers.append(producer)

        # Automatic back-reference
        if back and hasattr(producer, "addOutput"):
            producer.addOutput(self, False)

    @track_current_drop
    def handleEvent(self, e):
        """
        Handles the arrival of a new event. Events are delivered from those
        objects this DROP is subscribed to.
        """
        if e.type == "producerFinished":
            self.producerFinished(e.uid, e.status)
        elif e.type == "reproducibility":
            self.dropReproComplete(e.uid, e.reprodata)

    @track_current_drop
    def producerFinished(self, uid, drop_state):
        """
        Method called each time one of the producers of this DROP finishes
        its execution. Once all producers have finished this DROP moves to the
        COMPLETED state (or to ERROR if one of the producers is on the ERROR
        state).

        This is one of the key mechanisms through which the execution of a
        DROP graph is accomplished. If AppDROP A produces DROP
        B, as soon as A finishes its execution B will be notified and will move
        itself to COMPLETED.
        """

        finished = False
        with self._finishedProducersLock:
            self._finishedProducers.append(drop_state)
            nFinished = len(self._finishedProducers)
            nProd = len(self._producers)
            self._refCount -= 1

            if nFinished > nProd:
                raise RuntimeError(
                    "More producers finished that registered in DROP %r: %d > %d"
                    % (self, nFinished, nProd)
                )
            elif nFinished == nProd:
                finished = True

        if finished:
            logger.debug("All producers finished for DROP %r,%s", self, uid)

            # decided that if any producer fails then fail the data drop
            if DROPStates.ERROR in self._finishedProducers:
                self.setError()
            else:
                self.setCompleted()

    def dropReproComplete(self, uid, reprodata):
        """
        Callback invoved when a DROP with UID `uid` has finishing processing its reproducibility information.
        Importantly, this is independent of that drop being completed.
        """
        #  TODO: Perform some action

    @property
    def streamingConsumers(self):
        """
        The list of 'streaming' consumers held by this DROP.

        :see: `self.addStreamingConsumer()`
        """
        return self._streamingConsumers[:]

    @track_current_drop
    def addStreamingConsumer(self, streamingConsumer, back=True):
        """
        Adds a streaming consumer to this DROP.

        Streaming consumers are AppDROPs that receive the data written
        into this DROP *as it gets written*, and therefore do not need to
        wait until this DROP has been moved to the COMPLETED state.
        """

        # An object cannot be a normal and streaming streamingConsumer at the same time,
        # see the comment in the __init__ method
        scuid = streamingConsumer.uid
        if scuid in self._consumers_uids:
            raise InvalidRelationshipException(
                DROPRel(streamingConsumer, DROPLinkType.STREAMING_CONSUMER, self),
                "Consumer is already registered as a normal consumer",
            )

        # Add if not already present
        if scuid in self._streamingConsumers_uids:
            return
        logger.debug(
            "Adding new streaming streaming consumer for %r: %s",
            self,
            streamingConsumer,
        )
        self._streamingConsumers.append(streamingConsumer)

        # Automatic back-reference
        if back and hasattr(streamingConsumer, "addStreamingInput"):
            streamingConsumer.addStreamingInput(self, False)

        # Subscribe the streaming consumer to events sent when this DROP moves
        # to COMPLETED. This way the streaming consumer will be notified that
        # its input has finished
        # This only happens if this DROP's execution mode is 'DROP'; otherwise
        # an external entity will trigger the execution of the consumer at the
        # right time
        if self.executionMode == ExecutionMode.DROP:
            self.subscribe(streamingConsumer, "dropCompleted")

        # Add reproducibility subscription
        self.subscribe(streamingConsumer, "reproducibility")

    def completedrop(self):
        """
        Builds final reproducibility data for this drop and fires a 'dropComplete' event.
        This should be called once a drop is finished in success or error
        :return:
        """
        self.commit()
        reprodata = {"data": self._merkleData, "merkleroot": self.merkleroot}
        self._fire(eventType="reproducibility", reprodata=reprodata)
        if logger.getEffectiveLevel() != logging.getLevelName(self._global_log_level):
            logger.warning(
                "log-level after %s.%s: %s",
                self.name,
                self._humanKey,
                logging.getLevelName(logger.getEffectiveLevel()),
            )

    @track_current_drop
    def setError(self):
        """
        Moves this DROP to the ERROR state.
        """

        if self.status in (DROPStates.CANCELLED, DROPStates.SKIPPED):
            return

        self._closeWriters()

        logger.info("Moving %r to ERROR", self)
        self.status = DROPStates.ERROR

        # Signal our subscribers that the show is over
        self._fire(eventType="dropCompleted", status=DROPStates.ERROR)
        self.completedrop()

    def _setCompletedStateCheck(self) -> bool:
        """
        Checks DROP state to identify conflics before setting it to completed
        """
        status = self.status
        if status == DROPStates.CANCELLED:
            return False
        elif status == DROPStates.SKIPPED:
            self._fire("dropCompleted", status=status)
            return False
        elif status == DROPStates.COMPLETED:
            logger.warning("%r already in COMPLETED state", self)
            return False
        elif status not in [DROPStates.INITIALIZED, DROPStates.WRITING]:
            raise Exception(
                "%r not in INITIALIZED or WRITING state (%s), cannot setComplete()"
                % (self, self.status)
            )
        return True

    @track_current_drop
    def setCompleted(self):
        """
        Moves this DROP to the COMPLETED state. This can be used when not all the
        expected data has arrived for a given DROP, but it should still be moved
        to COMPLETED, or when the expected amount of data held by a DROP
        is not known in advanced.
        """
        if not self._setCompletedStateCheck():
            return
        try:
            self._closeWriters()
        except AttributeError as exp:
            logger.debug(exp)

        logger.debug("Moving %r to COMPLETED", self)
        self.status = DROPStates.COMPLETED
        # Signal our subscribers that the show is over
        self._fire(eventType="dropCompleted", status=DROPStates.COMPLETED)
        self.completedrop()

    def isCompleted(self):
        """
        Checks whether this DROP is currently in the COMPLETED state or not
        """
        # Mind you we're not accessing _status, but status. This way we use the
        # lock in status() to access _status
        return self.status == DROPStates.COMPLETED

    def cancel(self):
        """Moves this drop to the CANCELLED state closing any writers we opened"""
        if self.status in [DROPStates.INITIALIZED, DROPStates.WRITING]:
            self._closeWriters()
            self.status = DROPStates.CANCELLED
        self.completedrop()

    def skip(self):
        """
        Moves this drop to the SKIPPED state closing any writers we opened.

        If the drop has more than one producer it will block until the first
        producer is skipped or all producers are completed. If the block_skip
        flag is set the drop will block a skipping chain until the last producer
        is completed or also skipped. This is useful for branches inside loops
        to allow alternate paths for each iteration. For a branch to terminate
        a loop the flag needs to be false (default).
        """
        if self.status in [DROPStates.INITIALIZED, DROPStates.WRITING]:
            if not self.block_skip or (
                    len(self.producers) == 1
                    or (len(self.producers) > 1 and self._refCount == 1)
            ):
                self._closeWriters()
                self.status = DROPStates.SKIPPED
                self.completedrop()
            else:
                self._refCount -= 1

    @property
    def node(self):
        return self._node

    @property
    def dataIsland(self):
        return self._dataIsland

    @property
    def parameters(self):
        return self._parameters

    @property
    def dlg_session_id(self) -> str:
        """
        Get the session id for the session to which this DROP is assigned.
        """
        return self._dlg_session_id

    @property
    def humanKey(self) -> str:
        """
        Get the Human Readable Key for this DROP
        """
        return self._humanKey

    @property
    def log_level(self):
        """
        Get the log level for this DROP
        """
        return self._log_level


# Dictionary mapping 1-to-many DROPLinkType constants to the corresponding methods
# used to append a a DROP into a relationship collection of another
# (e.g., one uses `addConsumer` to add a DROPLinkeType.CONSUMER DROP into
# another)
LINKTYPE_1TON_APPEND_METHOD = {
    DROPLinkType.CONSUMER: "addConsumer",
    DROPLinkType.STREAMING_CONSUMER: "addStreamingConsumer",
    DROPLinkType.INPUT: "addInput",
    DROPLinkType.STREAMING_INPUT: "addStreamingInput",
    DROPLinkType.OUTPUT: "addOutput",
    DROPLinkType.CHILD: "addChild",
    DROPLinkType.PRODUCER: "addProducer",
}

# Same as above, but for N-to-1 relationships, in which case we indicate not a
# method but a property
LINKTYPE_NTO1_PROPERTY = {DROPLinkType.PARENT: "parent"}

LINKTYPE_1TON_BACK_APPEND_METHOD = {
    DROPLinkType.CONSUMER: "addInput",
    DROPLinkType.STREAMING_CONSUMER: "addStreamingInput",
    DROPLinkType.INPUT: "addConsumer",
    DROPLinkType.STREAMING_INPUT: "addStreamingConsumer",
    DROPLinkType.OUTPUT: "addProducer",
    DROPLinkType.CHILD: "setParent",
    DROPLinkType.PRODUCER: "addOutput",
}

LINKTYPE_NTO1_BACK_APPEND_METHOD = {DROPLinkType.PARENT: "addChild"}
