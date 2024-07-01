#
#    ICRAR - International Centre for Radio Astronomy Research 2024
#    (c) UWA - The University of Western Australia
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


from dlg.apps.app_base import BarrierAppDROP
from dlg.data.drops.data_base import DataDROP, logger
from dlg.data.io import DataIO


##
# @brief SubGraphApp
# @par EAGLE_START
# @param category PythonApp
# @param tag daliuge
# @param sleep_time 5/Integer/ApplicationArgument/NoPort/ReadWrite//False/False/The number of seconds to sleep
# @param dropclass dlg.apps.simple.SleepAndCopyApp/String/ComponentParameter/NoPort/ReadOnly//False/False/Application class
# @param execution_time 5/Float/ConstraintParameter/NoPort/ReadOnly//False/False/Estimated execution time
# @param num_cpus 1/Integer/ConstraintParameter/NoPort/ReadOnly//False/False/Number of cores used
# @param group_start False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the start of a group?
# @param input_parser pickle/Select/ComponentParameter/NoPort/ReadWrite/raw,pickle,eval,npy,path,dataurl/False/False/Input port parsing technique
# @param output_parser pickle/Select/ComponentParameter/NoPort/ReadWrite/raw,pickle,eval,npy,path,dataurl/False/False/Output port parsing technique
# @par EAGLE_END
class SubGraphApp(BarrierAppDROP):

    def initialize(self, **kwargs):
        super(SubGraphApp, self).initialize(**kwargs)

    def run(self):
        """
        Use the subgraph data application to get the subgraph data, then use that to
        launch a new session for the engine.
        1. need the 'subgraph' named port
        2. Need to duplicate the input data for the subgraph

        :return:
        """


##
# @brief Memory
# @details In-memory storage of intermediate data products
# @par EAGLE_START
# @param category Memory
# @param tag daliuge
# @param pydata None/String/ApplicationArgument/NoPort/ReadWrite//False/False/Data to be loaded into memory
# @param dummy /Object/ApplicationArgument/InputOutput/ReadWrite//False/False/Dummy port
# @param persist False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Specifies whether this data component contains data that should not be deleted after execution
# @param data_volume 5/Float/ConstraintParameter/NoPort/ReadWrite//False/False/Estimated size of the data contained in this node
# @param group_end False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the end of a group?
# @param streaming False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Specifies whether this data component streams input and output data
# @param dropclass dlg.data.drops.memory.InMemoryDROP/String/ComponentParameter/NoPort/ReadOnly//False/False/Drop class
# @par EAGLE_END
class SubGraphDataDrop(DataDROP):
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


    def initialize(self, **kwargs):
        super(SubGraphDataDrop, self).initialize(**kwargs)

    def getIO(self) -> DataIO:
        """
        Get the SubGraph IO drop
        :return:
        """

        return SubGraphIO()

class SubGraphIO(DataIO):

    def __init__(self, **kwargs):
        super().__init__()

    def _open(self):
        pass


