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
import pickle

from dlg import droputils
from dlg.apps.pyfunc import PyFuncApp

logger = logging.getLogger(f"dlg.{__name__}")

from dlg.meta import (
    dlg_bool_param,
    dlg_int_param,
)

##
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
# @param x /Object/ApplicationParameter/InputPort/ReadWrite//False/False/Port carrying the input which is also used in the condition function. Note that the name of the parameter has to match the argument of the condition function.
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

    def _get_drop_from_port(self, result):
        for output in self.outputs:
            for value in self.parameters['outputPorts'].values():
                if value['target_id'] in output.oid:
                    if value['name'] == result:
                        return output
        raise RuntimeError

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

        # if nogo_result == 'true':
        #     raise Exception
        go_drop_oid = next(iter(self._port_names['output'].get(go_result,[])), None)
        nogo_drop_oid = next(iter(self._port_names['output'].get(nogo_result,[])), None)

        go_drop = next(o for o in self.outputs if o.oid == go_drop_oid)
        nogo_drop = next(o for o in self.outputs if o.oid == nogo_drop_oid)

        nogo_drop.skip()  # send skip to correct branch

        if self.inputs and hasattr(go_drop, "write"):
            droputils.copyDropContents(  # send data to correct branch
                    self.x, go_drop, bufsize=self.bufsize
            )
            logger.debug("Sent the following data to correct branch: %s",
                         droputils.allDropContents(self.x))

        else:  # this enables a branch based only on the condition function
            d = pickle.dumps(self.parameters['x'])
            logger.debug("Sending following data to correct branch: %s", self.parameters['x'])
            # d = self.parameters[self.argnames[0]]
            if hasattr(go_drop, "write"):
                go_drop.write(d)