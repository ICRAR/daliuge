import csv
import json
import math

from dlg.drop import BarrierAppDROP
from dlg.droputils import DROPFile
from dlg.meta import dlg_string_param, \
    dlg_component, dlg_batch_input, dlg_batch_output, dlg_streaming_input


## Produce Config
# @brief Produce Config
# @details A BarrierAppDrop that produces multiple config files suitable for the CallLeap BarrierAppDrop
# @par EAGLE_START
# @param gitrepo $(GIT_REPO)
# @param version $(PROJECT_VERSION)
# @param category PythonApp
# @param[in] param/filePath/filePath//String/readwrite
#     \~English Path to the MS\n
#     \~Chinese \n
#     \~
# @param[in] param/outputFilePath/outputFilePath//String/readwrite
#     \~English Path for output file\n
#     \~Chinese \n
#     \~
# @param[in] param/implementation/Implementation/cpu/String/readwrite
#     \~English The implementation of the LEAP algorithm to use (cpu, cuda)\n
#     \~Chinese \n
#     \~
# @param[in] param/verbosity/verbosity/info/String/readwrite
#     \~English The verbosity of the LEAP logging output (info|debug)\n
#     \~Chinese \n
#     \~
# @param[in] param/appclass/Application Class/leap_nodes.ProduceConfig.ProduceConfig/String/readonly
#     \~English The path to the class that implements this app\n
#     \~Chinese \n
#     \~
# @param[in] port/Directions
#     \~English A CSV file containing directions for calibration
#     \~Chinese \n
#     \~
# @param[out] port/Config
#     \~English A JSON config containing the specification for running an instance of LeapAccelerateCLI
#     \~Chinese \n
#     \~
# @par EAGLE_END

class ProduceConfig(BarrierAppDROP):
    """A BarrierAppDrop that produces multiple config files suitable for the CallLeap BarrierAppDrop"""
    compontent_meta = dlg_component('ProduceConfig', 'Produce Config.',
                                    [dlg_batch_input('binary/*', [])],
                                    [dlg_batch_output('binary/*', [])],
                                    [dlg_streaming_input('binary/*')])

    # read component parameters
    filePath = dlg_string_param('filePath', '')
    outputFilePath = dlg_string_param('outputFilePath', '')
    implementation = dlg_string_param('implementation', 'cpu')
    verbosity = dlg_string_param('verbosity', 'info')


    def initialize(self, **kwargs):
        super(ProduceConfig, self).initialize(**kwargs)


    def run(self):
        # check number of inputs and outputs
        if len(self.inputs) != 1:
            raise Exception("One input is expected by this application")

        # read directions from input 0
        directions = self._readDirections(self.inputs[0])

        # determine number of directions per instance
        numDirectionsPerInstance = float(len(directions)) / float(len(self.outputs))

        startDirectionIndex = 0
        endDirectionIndex = 0

        # split directions
        for i in range(len(self.outputs)):
            endDirectionIndex = int(math.floor((i+1)*numDirectionsPerInstance))

            # split directions
            partDirections = directions[startDirectionIndex:endDirectionIndex]

            # build config
            configJSON = self._createConfig(partDirections)

            # stringify config
            config = json.dumps(configJSON)

            # write config to output
            self.outputs[i].write(config.encode())

            # continue from here in the next iteration
            startDirectionIndex = endDirectionIndex


    def _readDirections(self, inDrop):
        directions = []

        # NOTE: it appears csv.reader() can't use the DROPFile(inDrop) directly,
        #       since DROPFile is not a iterator. Instead, we read the whole
        #       inDrop to a string and pass that to csv.reader()
        with DROPFile(inDrop) as f:
            file_data = f.read()
        if type(file_data) == type(b''):
            file_data = file_data.decode()
        csvreader = csv.reader(file_data.split('\n'))
        for row in csvreader:
            # skip rows with incorrect number of values
            if len(row) != 2:
                continue

            x = float(row[0])
            y = float(row[1])
            directions.append([x,y])

        return directions


    def _createConfig(self, directions):
        return {
            'filePath': self.filePath,
            'outputFilePath': self.outputFilePath,
            'directions': directions,
            'computeImplementation': self.implementation,
            'verbosity': self.verbosity
        }

