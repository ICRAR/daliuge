from abc import ABC, abstractmethod
from collections import OrderedDict
from concurrent.futures import Future
from typing import List, Callable
import logging
import math
import threading

from dlg.drop import track_current_drop
from dlg.drop_loaders import load_dill
from dlg.data.drops.container import ContainerDROP
from dlg.data.drops.data_base import DataDROP
from dlg.ddap_protocol import (
    AppDROPStates,
    DROPLinkType,
    DROPStates,
    DROPRel,
)
from dlg.exceptions import InvalidDropException, InvalidRelationshipException

from dlg.meta import (
    dlg_int_param,
)

logger = logging.getLogger(f"dlg.{__name__}")


class DropRunner(ABC):
    """An executor for `run()`-ing an AppDROP"""

    @abstractmethod
    def run_drop(self, app_drop: "AppDROP") -> Future:
        """Executes `app_drop.run()`, returning a future with the result."""
        raise NotImplementedError


class SyncDropRunner(DropRunner):
    """
    A simple pool-like object that creates a new thread for each invocation.
    """

    def run_drop(self, app_drop: "AppDROP") -> Future:
        """Run drop synchronously."""
        future = Future()

        try:
            res = app_drop.run()
            future.set_result(res)
        except Exception as e: # pylint: disable=broad-exception-caught
            future.set_exception(e)

        return future


def run_on_daemon_thread(func: Callable, *args, **kwargs) -> Future:
    """Runs a callable on a daemon thread, meaning it will be
    ungracefully terminated if the process ends."""
    future = Future()

    def thread_target():
        try:
            res = func(*args, **kwargs)
            future.set_result(res)
        except Exception as e: # pylint: disable=broad-exception-caught
            future.set_exception(e)

    t = threading.Thread(target=thread_target)
    t.daemon = True
    t.start()

    return future


_SYNC_DROP_RUNNER = SyncDropRunner()

class InstanceLogHandler(logging.Handler):
    """Custom handler to store logs in-memory per object instance."""
    def __init__(self, log_storage):
        super().__init__()
        self.log_storage = log_storage

    def emit(self, record):
        """Store log messages in the instance's log storage.

         :param record: The log string we want to add to the log storage

         .. note: We are not interested in actually emitting the log;
             we are just interested in extracting and storing Record metadata
        """

        exc = f"{str(record.exc_text)}" if record.exc_text else ""
        # msg = str(record.message).replace("\n", "<br>")
        msg = (f"<pre>{record.message.encode('utf-8').decode('unicode_escape')}\n"
               f"{exc}</pre>")
        try:
            rec_time = record.asctime
        except AttributeError:
            rec_time = ""
        self.log_storage.append({ "time":rec_time,
            "Level": record.levelname,
            "Module": record.name,
            "Function/Method": record.funcName,
            "Line #": record.lineno,
            "Message": msg,
        })

class DROPLogFilter(logging.Filter):
    def __init__(self, uid: str, humanKey: str):
        super().__init__()
        self.uid = uid
        self.humanKey = humanKey

    def filter(self, record):
        uid = getattr(record, "drop_uid", None)
        return uid == self.uid or uid == self.humanKey



# ===============================================================================
# AppDROP classes follow
# ===============================================================================


class AppDROP(ContainerDROP):
    """
    An AppDROP is a DROP representing an application that reads data
    from one or more DataDROPs (its inputs), and writes data onto one or more
    DataDROPs (its outputs).

    AppDROPs accept two different kind of inputs: "normal" and "streaming"
    inputs. Normal inputs are DataDROPs that must be on the COMPLETED state
    (and therefore their data must be fully written) before this application is
    run, while streaming inputs are DataDROPs that feed chunks of data into
    this application as the data gets written into them.

    This class contains two methods that need to be overwritten by
    subclasses: `dropCompleted`, invoked when input DataDROPs move to
    COMPLETED, and `dataWritten`, invoked with the data coming from streaming
    inputs.

    How and when applications are executed is completely up to the app component
    developer, and is not enforced by this base class. Some applications might need
    to be run at `initialize` time, while other might start during the first invocation
    of `dataWritten`. A common scenario anyway is to start an application only
    after all its inputs have moved to COMPLETED (implying that none of them is
    an streaming input); for these cases see the `BarrierAppDROP`.
    """

    def __getstate__(self):
        state = super().__getstate__()
        del state["_drop_runner"]
        return state

    def __setstate__(self, state):
        super().__setstate__(state)
        self._drop_runner = _SYNC_DROP_RUNNER

    def initialize(self, **kwargs):
        super(AppDROP, self).initialize(**kwargs)

        # Inputs and Outputs are the DROPs that get read from and written
        # to by this AppDROP, respectively. An input DROP will see
        # this AppDROP as one of its consumers, while an output DROP
        # will see this AppDROP as one of its producers.
        #
        # Input and output objects are later referenced by their *index*
        # (relative to the order in which they were added to this object)
        # Therefore we use an ordered dict to keep the insertion order.

        self._inputs = OrderedDict()
        self._outputs = OrderedDict()
        self._inputs_names = OrderedDict()
        self._outputs_names = OrderedDict()

        # Same as above, only that these correspond to the 'streaming' version
        # of the consumers
        self._streamingInputs = OrderedDict()

        # An AppDROP has a second, separate state machine indicating its
        # execution status.
        self._execStatus = AppDROPStates.NOT_RUN

        # by default run drops synchronously
        self._drop_runner: DropRunner = _SYNC_DROP_RUNNER

        self.log_storage = []

        self.logger = logging.getLogger(f"{__class__}.{self.uid}")
        instance_handler = InstanceLogHandler(self.log_storage)
        instance_handler.addFilter(DROPLogFilter(self.uid, self._humanKey))
        fmt = ("%(asctime)-15s [%(levelname)5.5s] "
               "%(name)s#%(funcName)s:%(lineno)s %(message)s")
        fmt = logging.Formatter(fmt)
        instance_handler.setFormatter(fmt)

        # Attach instance-specific handler
        logging.root.addHandler(instance_handler)

        # Ensure logs still propagate to the root logger
        logger.propagate = True

    @track_current_drop
    def addInput(self, inputDrop, back=True):
        uid = inputDrop.uid
        if uid not in self._inputs:
            self._inputs[uid] = inputDrop
            if back:
                inputDrop.addConsumer(self, False)

    @property
    def inputs(self) -> List[DataDROP]:
        """
        The list of inputs set into this AppDROP
        """
        return list(self._inputs.values())

    @inputs.setter
    def inputs(self, inputs):
        self._inputs = inputs

    @track_current_drop
    def addOutput(self, outputDrop: DataDROP, back=True):
        if outputDrop is self:
            raise InvalidRelationshipException(
                DROPRel(outputDrop, DROPLinkType.OUTPUT, self),
                "Cannot add an AppConsumer as its own output",
            )
        uid = outputDrop.uid
        if uid not in self._outputs:
            self._outputs[uid] = outputDrop

            if back:
                outputDrop.addProducer(self, False)

            # Subscribe the output DROP to events sent by this AppDROP when it
            # finishes its execution.
            self.subscribe(outputDrop, "producerFinished")

    @property
    def outputs(self) -> List[DataDROP]:
        """
        The list of outputs set into this AppDROP
        """
        return list(self._outputs.values())

    @outputs.setter
    def outputs(self, outputs):
        self._outputs = outputs

    def addStreamingInput(self, streamingInputDrop, back=True):
        if streamingInputDrop not in self._streamingInputs.values():
            uid = streamingInputDrop.uid
            self._streamingInputs[uid] = streamingInputDrop
            if back:
                streamingInputDrop.addStreamingConsumer(self, False)

    @property
    def streamingInputs(self) -> List[DataDROP]:
        """
        The list of streaming inputs set into this AppDROP
        """
        return list(self._streamingInputs.values())

    def _generateNamedPorts(self, ports):
        """
        Generates a named mapping of ports to data drops. Can only be called during run().
        """
        port_type = {"inputs": "InputPort", "outputs": "OutputPort"}
        named_ports: OrderedDict[str, DataDROP] = OrderedDict()
        port_dict = self.__getattribute__(f"_{ports}")
        if (
            ports in self.parameters
            and len(self.parameters[ports]) > 0
            and isinstance(self.parameters[ports][0], dict)
        ):
            for i in range(len(port_dict)):
                key = list(self.parameters[ports][i].values())[0]
                value = port_dict[list(self.parameters[ports][i].keys())[0]]
                if key not in named_ports:
                    named_ports[key] = value
                else:
                    if isinstance(named_ports[key], list):
                        named_ports[key].append(value)
                    else:
                        named_ports[key] = [named_ports[key], value]
        elif (
            ports in self.parameters
            and len(self.parameters[ports]) > 0
            and isinstance(self.parameters[ports], list)
        ):
            # This enables the gather to work
            return {}
        elif "applicationArgs" in self.parameters:
            for key, field in self.parameters["applicationArgs"].items():
                if field["usage"] in [port_type[ports], "InputOutput"]:
                    named_ports[field["name"]] = field
        return named_ports

    def handleEvent(self, e):
        """
        Handles the arrival of a new event. Events are delivered from those
        objects this DROP is subscribed to.
        """
        if e.type == "dropCompleted":
            self.dropCompleted(e.uid, e.status)

    def dropCompleted(self, uid, drop_state):
        """
        Callback invoked when the DROP with UID `uid` (which is either a
        normal or a streaming input of this AppDROP) has moved to the
        COMPLETED or ERROR state. By default no action is performed.
        """

    def dataWritten(self, uid, data):
        """
        Callback invoked when `data` has been written into the DROP with
        UID `uid` (which is one of the streaming inputs of this AppDROP).
        By default no action is performed
        """

    @property
    def execStatus(self):
        """
        The execution status of this AppDROP
        """
        return self._execStatus

    @execStatus.setter
    def execStatus(self, execStatus):
        if self._execStatus == execStatus:
            return
        self._execStatus = execStatus
        self._fire("execStatus", execStatus=execStatus)

    def _notifyAppIsFinished(self):
        """
        Method invoked by subclasses when the execution of the application is
        over. Subclasses must make sure that both the status and execStatus
        properties are set to their correct values correctly before invoking
        this method.
        """
        is_error = self._execStatus == AppDROPStates.ERROR
        if is_error:
            self.status = DROPStates.ERROR
        else:
            self.status = DROPStates.COMPLETED
        logger.debug(
            "Moving %r to %s",
            self.name,
            "FINISHED" if not is_error else "ERROR",
        )
        self._fire("producerFinished", status=self.status, execStatus=self.execStatus)
        self.completedrop()

    def cancel(self):
        """Moves this application drop to its CANCELLED state"""
        super(AppDROP, self).cancel()
        self.execStatus = AppDROPStates.CANCELLED

    def skip(self):
        """Moves this application drop to its SKIPPED state"""
        super().skip()

        prev_execStatus = self.execStatus
        self.execStatus = AppDROPStates.SKIPPED
        for o in self._outputs.values():
            o.skip()

        logger.debug("Moving %s to SKIPPED", self.__repr__())
        if prev_execStatus in [AppDROPStates.NOT_RUN]:
            self._fire(
                "producerFinished",
                status=self.status,
                execStatus=self.execStatus,
            )

    @property
    def drop_runner(self):
        """
        Getter for the drop runner
        """
        return self._drop_runner

    @drop_runner.setter
    def drop_runner(self, runner):
        self._drop_runner = runner

    def getLogs(self):
        """
        :return: Return the logs stored in the logging handler
        """

        return self.log_storage


class InputFiredAppDROP(AppDROP):
    """
    An InputFiredAppDROP accepts no streaming inputs and waits until a given
    amount of inputs (called *effective inputs*) have moved to COMPLETED to
    execute its 'run' method, which must be overwritten by subclasses. This way,
    this application allows to continue the execution of the graph given a
    minimum amount of inputs being ready. The transitions of subsequent inputs
    to the COMPLETED state have no effect.

    Normally only one call to the `run` method will happen per application.
    However users can override this by specifying a different number of tries
    before finally giving up.

    The amount of effective inputs must be less or equal to the amount of inputs
    added to this application once the graph is being executed. The special
    value of -1 means that all inputs are considered as effective, in which case
    this class acts as a BarrierAppDROP, effectively blocking until all its
    inputs have moved to the COMPLETED, SKIPPED or ERROR state. Setting this
    value to anything other than -1 or the number of inputs, results in
    late arriving inputs to be ignored, even if they would successfully finish.
    This requires careful implementation of the upstream and downstream apps to
    deal with this situation. It is only really useful to control a combination
    of maximum allowed execution time and acceptable number of completed inputs.

    An input error threshold controls the behavior of the application given an
    error in one or more of its inputs (i.e., a DROP moving to the ERROR state).
    The threshold is a value within 0 and 100 that indicates the tolerance
    to erroneous effective inputs, and after which the application will not be
    run but moved to the ERROR state itself instead.
    """

    input_error_threshold = dlg_int_param("Input error threshold (0 and 100)", 0)
    n_effective_inputs = dlg_int_param("Number of effective inputs", -1)
    n_tries = dlg_int_param("Number of tries", 1)

    def initialize(self, **kwargs):
        super(InputFiredAppDROP, self).initialize(**kwargs)
        if "dlg_session_id" in kwargs:
            self.dlg_session_id = kwargs["dlg_session_id"]
        self._completedInputs = []
        self._errorInputs = []
        self._skippedInputs = []

        # Error threshold must be within 0 and 100
        if self.input_error_threshold < 0 or self.input_error_threshold > 100:
            raise InvalidDropException(
                self, "%r: input_error_threshold not within [0,100]: %s" % (self,type(self.input_error_threshold))
            )

        # Amount of effective inputs
        if "n_effective_inputs" not in kwargs:
            raise InvalidDropException(
                self, "%r: n_effective_inputs is mandatory" % (self,)
            )

        if self.n_effective_inputs < -1 or self.n_effective_inputs == 0:
            raise InvalidDropException(
                self,
                "%r: n_effective_inputs must be > 0 or equals to -1" % (self,),
            )

        # Number of tries
        if self.n_tries < 1:
            raise InvalidDropException(
                self, "Invalid n_tries, must be a positive number"
            )

    def addStreamingInput(self, streamingInputDrop, back=True):
        raise InvalidRelationshipException(
            DROPRel(streamingInputDrop, DROPLinkType.STREAMING_INPUT, self),
            "InputFiredAppDROPs don't accept streaming inputs",
        )

    def dropCompleted(self, uid, drop_state):
        super(InputFiredAppDROP, self).dropCompleted(uid, drop_state)

        logger.debug(
            "Received notification from input drop: uid=%s, state=%d",
            uid,
            drop_state,
        )

        # A value of -1 means all inputs
        n_inputs = len(self._inputs)
        n_eff_inputs = self.n_effective_inputs
        if n_eff_inputs == -1:
            n_eff_inputs = n_inputs

        # More effective inputs than inputs, this is a horror
        if n_eff_inputs > n_inputs:
            raise RuntimeError(
                "%r: More effective inputs (%s) than inputs (%d)"
                % (self, self.n_effective_inputs, n_inputs)
            )

        if drop_state == DROPStates.ERROR:
            self._errorInputs.append(uid)
        elif drop_state == DROPStates.COMPLETED:
            self._completedInputs.append(uid)
        elif drop_state == DROPStates.SKIPPED:
            self._skippedInputs.append(uid)
        else:
            raise Exception("Invalid DROP state in dropCompleted: %s" % drop_state)

        error_len = len(self._errorInputs)
        ok_len = len(self._completedInputs)
        skipped_len = len(self._skippedInputs)

        # We have enough inputs to proceed
        if (skipped_len + error_len + ok_len) == n_eff_inputs:
            # calculate the number of errors that have already occurred
            percent_failed = math.floor((error_len / float(n_eff_inputs)) * 100)
            if percent_failed > 0:
                logger.debug(
                    "Error rate on inputs for %r: %d/%d",
                    self,
                    percent_failed,
                    self.input_error_threshold,
                )

            # if we hit the input error threshold then ERROR the drop and move on
            if percent_failed > self.input_error_threshold:
                logger.info(
                    "Error threshold reached on %r, not executing it: %d/%d",
                    self,
                    percent_failed,
                    self.input_error_threshold,
                )

                self.execStatus = AppDROPStates.ERROR
                self.status = DROPStates.ERROR
                self._notifyAppIsFinished()
            elif (not self.block_skip and skipped_len > 0) or (
                self.block_skip and skipped_len == n_eff_inputs):
                self.skip()
            elif self.block_skip and skipped_len < n_eff_inputs:
                self.async_execute()
            else:
                self.async_execute()

    def async_execute(self):
        # TODO Do we need another thread pool for this?
        # Careful, trying to run this on the same threadpool as the
        # DropRunner can cause deadlocks
        return run_on_daemon_thread(self._execute_and_log_exception)

    def _execute_and_log_exception(self):
        try:
            self.execute()
        except Exception: # pylint: disable=broad-exception-caught
            logger.exception("Unexpected exception during drop (%r) execution", self)

    @track_current_drop
    def execute(self, _send_notifications=True):
        """
        Manually trigger the execution of this application.

        This method is normally invoked internally when the application detects
        all its inputs are COMPLETED.
        """

        # TODO: We need to be defined more clearly how the state is set in
        #       applications, for the time being they follow their execState.

        # Run at most self._n_tries if there are errors during the execution
        logger.info("Executing %r", f"{self.name}.{self._humanKey}")
        tries = 0
        drop_state = DROPStates.COMPLETED
        self.execStatus = AppDROPStates.RUNNING
        while tries < self.n_tries:
            try:
                fut = self._drop_runner.run_drop(self)
                fut.result()
                if self.execStatus == AppDROPStates.CANCELLED:
                    return
                self.execStatus = AppDROPStates.FINISHED
                if logger.getEffectiveLevel() != logging.getLevelName(
                    self._global_log_level
                ):
                    logging.getLogger("dlg").setLevel(self._global_log_level)
                    logger.warning(
                        "Setting log-level after execution %s.%s back to %s",
                        self.name,
                        self._humanKey,
                        self._global_log_level,
                    )
                break
            except Exception: # pylint: disable=broad-exception-caught
                if self.execStatus == AppDROPStates.CANCELLED:
                    return
                tries += 1
                logger.exception(
                    "Error while executing %r (try %s/%s)",
                    self,
                    tries,
                    self.n_tries,
                )

        # We gave up running the application, go to error
        if tries == self.n_tries:
            self.execStatus = AppDROPStates.ERROR
            drop_state = DROPStates.ERROR

        self.status = drop_state
        if _send_notifications:
            self._notifyAppIsFinished()

    def _run(self):
        """
        Run this application. It can be safely assumed that at this point all
        the required inputs are COMPLETED.

        This will first set the named input params and then call the run method
        provided by the implementation.
        """
        named_inputs = self._generateNamedPorts("inputs")
        logger.debug("named inputs identified: %s", named_inputs)
        for attr_name in named_inputs:
            if isinstance(named_inputs[attr_name], list) and len(named_inputs[attr_name]) > 1:
                for ni in named_inputs[attr_name]:
                    if ni.status != DROPStates.COMPLETED:
                        continue
                    break # we use the first completed
            else:
                ni = named_inputs[attr_name]
            logger.debug("Identified input: %s", ni.name)
            # Ignore NullDROPs: This is the current work-around to pass-on events
            # In reality we want to check whether the port is an event port, but
            # that is really hard with the current node data structure.
            if  "componentParams" not in ni.parameters or (
                ni.parameters["componentParams"]["dropclass"]["value"] !=
                "dlg.data.drops.data_base.NullDROP" and ni.status == DROPStates.COMPLETED
            ):
                if not hasattr(self, attr_name):
                    self.__setattr__(attr_name, named_inputs[attr_name])
                else:
                    # TODO: need to check for parser before reading
                    self.__setattr__(attr_name, load_dill(ni))
                    logger.debug("Input read: %s",getattr(self, attr_name))
            else:
                logger.warning("None of the inputs COMPLETED, falling back to default value.")


        named_outputs = self._generateNamedPorts("outputs")
        logger.debug("named outputs identified: %s", named_outputs)
        for attr_name in named_outputs:
            if not isinstance(named_outputs[attr_name], list):
                self.__setattr__(attr_name, named_outputs[attr_name])

    # TODO: another thing we need to check
    def exists(self):
        return True



class BarrierAppDROP(InputFiredAppDROP):
    """
    A BarrierAppDROP is an InputFireAppDROP that waits for all its inputs to
    complete, effectively blocking the flow of the graph execution.
    """

    def initialize(self, **kwargs):
        # Blindly override existing value if any
        kwargs["n_effective_inputs"] = -1
        super().initialize(**kwargs)
