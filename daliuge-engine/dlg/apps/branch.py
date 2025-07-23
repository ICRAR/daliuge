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
import logging

from dlg import droputils
from dlg.apps.pyfunc import PyFuncApp
from dlg.drop import track_current_drop
from dlg.apps.app_base import BarrierAppDROP
from dlg.exceptions import InvalidDropException

logger = logging.getLogger(f"dlg.{__name__}")

from dlg.meta import (
    dlg_float_param,
    dlg_string_param,
    dlg_bool_param,
    dlg_int_param,
    dlg_list_param,
    dlg_dict_param,
    dlg_component,
    dlg_batch_input,
    dlg_batch_output,
    dlg_streaming_input,
)

# @brief Branch
# @details A branch application that copies the input to either the 'true' or the 'false' output depending on the result of
# the provided conditional function. The conditional function can be specified either in-line or as an external function and has
# to return a boolean value.
# The inputs of the application are passed on as arguments to the conditional function. The conditional function needs to return
# a boolean value, but the application will copy the input data to the true or false output, depending on the result of the
# conditional function.
# @par EAGLE_START
# @param category Branch
# @param tag daliuge
# @param func_name condition/String/ComponentParameter/NoPort/ReadWrite//False/False/Python conditional function name. This can also be a valid import path to an importable function.
# @param func_code def condition(x): return (x > 0)/String/ComponentParameter/NoPort/ReadWrite//False/False/Python function code for the branch condition. Modify as required. Note that func_name above needs to match the defined name here.
# @param x /Object/ComponentParameter/InputPort/ReadWrite//False/False/Port carrying the input which is also used in the condition function. Note that the name of the parameter has to match the argument of the condition function.
# @param true  /Object/ComponentParameter/OutputPort/ReadWrite//False/False/If condition is true the input will be copied to this port
# @param false /Object/ComponentParameter/OutputPort/ReadWrite//False/False/If condition is false the input will be copied to this port
# @param log_level "NOTSET"/Select/ComponentParameter/NoPort/ReadWrite/NOTSET,DEBUG,INFO,WARNING,ERROR,CRITICAL/False/False/Set the log level for this drop
# @param dropclass dlg.apps.simple.Branch/String/ComponentParameter/NoPort/ReadOnly//False/False/Application class
# @param base_name simple/String/ComponentParameter/NoPort/ReadOnly//False/False/Base name of application class
# @param execution_time 5/Float/ConstraintParameter/NoPort/ReadOnly//False/False/Estimated execution time
# @param num_cpus 1/Integer/ConstraintParameter/NoPort/ReadOnly//False/False/Number of cores used
# @param group_start False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the start of a group?
# @par EAGLE_END
class Branch(PyFuncApp):
    """
    A branch application that copies the input to either the 'true' or the 'false' output depending on the result of
    the provided conditional function. The conditional function can be specified either in-line or as an external function and has
    to return a boolean value.
    The inputs of the application are passed on as arguments to the conditional function. The conditional function needs to return
    a boolean value, but the application will copy the input data to the true or false output, depending on the result of the
    conditional function.
    """

    bufsize = dlg_int_param("bufsize", 65536)
    result = dlg_bool_param("result", False)

    def write_results(self,result:bool=False):
        """
        Copy the input to the output identified by the condition function.
        """
        if result and isinstance(result, bool):
            self.result = result
        if not self.outputs:
            return

        go_result = str(self.result).lower()
        nogo_result = str(not self.result).lower()

        try:
            nogo_drop = getattr(self, nogo_result)
        except AttributeError:
            logger.error("There is no Drop associated with the False condition; "
                         "a runtime failure has occured.")
            self.setError()
            return
        try:
            go_drop = getattr(self, go_result)
        except AttributeError:
            logger.error("There is no Drop associated with the True condition; "
                         "a runtime failure has occured.")
            self.setError()
            return

        logger.info("Sending skip to port: %s: %s", str(nogo_result), getattr(self,nogo_result))
        nogo_drop.skip()  # send skip to correct branch

        if self.inputs and hasattr(go_drop, "write"):
            droputils.copyDropContents(  # send data to correct branch
                self.inputs[0], go_drop, bufsize=self.bufsize
            )
        else:  # this enables a branch based only on the condition function
            d = pickle.dumps(self.parameters[self.argnames[0]])
            # d = self.parameters[self.argnames[0]]
            if hasattr(go_drop, "write"):
                go_drop.write(d)
