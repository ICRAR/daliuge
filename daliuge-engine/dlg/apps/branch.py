from dlg.drop import track_current_drop
from dlg.apps.app_base import BarrierAppDROP
from dlg.exceptions import InvalidDropException


##
# @brief Branch
# @details A conditional branch to control flow
# @par EAGLE_START
# @param category Branch
# @param tag template
# @param input /Object/ApplicationArgument/InputPort/ReadWrite//False/False/Input port
# @param true /Object/ComponentParameter/OutputPort/ReadWrite//False/False/True condition output port
# @param false /Object/ComponentParameter/OutputPort/ReadWrite//False/False/False condition output port
# @param log_level "NOTSET"/Select/ComponentParameter/NoPort/ReadWrite/NOTSET,DEBUG,INFO,WARNING,ERROR,CRITICAL/False/False/Set the log level for this drop
# @param dropclass dlg.apps.branch.BranchAppDrop/String/ComponentParameter/NoPort/ReadOnly//False/False/Application class
# @param base_name branch/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @param execution_time 5/Float/ConstraintParameter/NoPort/ReadOnly//False/False/Estimated execution time
# @param num_cpus 1/Integer/ConstraintParameter/NoPort/ReadOnly//False/False/Number of cores used
# @param group_start False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the start of a group?
# @param input_error_threshold 0/Integer/ComponentParameter/NoPort/ReadWrite//False/False/the allowed failure rate of the inputs (in percent), before this component goes to ERROR state and is not executed
# @param n_tries 1/Integer/ComponentParameter/NoPort/ReadWrite//False/False/Specifies the number of times the 'run' method will be executed before finally giving up
# @par EAGLE_END
class BranchAppDrop(BarrierAppDROP):
    """
    A special kind of application with exactly two outputs. After normal
    execution, the application decides whether a certain condition is met.
    If the condition is met, the first output is considered as COMPLETED,
    while the other is moved to SKIPPED state, and vice-versa.
    """

    @track_current_drop
    def execute(self, _send_notifications=True):
        if len(self._outputs) != 2:
            raise InvalidDropException(
                self,
                f"BranchAppDrops should have exactly 2 outputs, not {len(self._outputs)}",
            )
        BarrierAppDROP.execute(self, _send_notifications=False)
        self.outputs[1 if self.condition() else 0].skip()
        self._notifyAppIsFinished()
