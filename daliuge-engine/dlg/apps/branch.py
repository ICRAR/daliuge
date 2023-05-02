from dlg.drop import track_current_drop
from dlg.apps.app_base import BarrierAppDROP
from dlg.exceptions import InvalidDropException


##
# @brief Branch
# @details A conditional branch to control flow
# @par EAGLE_START
# @param category Branch
# @param tag template
# @param appclass Application Class/dlg.apps.simple.SimpleBranch/String/ComponentParameter/readonly//False/False/Application class
# @param input_parser Input Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Input port parsing technique
# @param output_parser Output Parser/pickle/Select/ApplicationArgument/readwrite/raw,pickle,eval,npy,path,dataurl/False/False/Output port parsing technique
# @param execution_time Execution Time/5/Float/ComponentParameter/readonly//False/False/Estimated execution time
# @param num_cpus No. of CPUs/1/Integer/ComponentParameter/readonly//False/False/Number of cores used
# @param group_start Group start/False/Boolean/ComponentParameter/readwrite//False/False/Is this node the start of a group?
# @param input_error_threshold "Input error rate (%)"/0/Integer/ComponentParameter/readwrite//False/False/the allowed failure rate of the inputs (in percent), before this component goes to ERROR state and is not executed
# @param n_tries Number of tries/1/Integer/ComponentParameter/readwrite//False/False/Specifies the number of times the 'run' method will be executed before finally giving up
# @param dummy0 dummy0//Object/OutputPort/readwrite//False/False/Dummy output port
# @param dummy1 dummy1//Object/OutputPort/readwrite//False/False/Dummy output port
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
