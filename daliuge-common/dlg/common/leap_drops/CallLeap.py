import json
import os
import random
import subprocess
import time

from dlg.drop import BarrierAppDROP
from dlg.droputils import DROPFile
from dlg.meta import dlg_component, dlg_batch_input, dlg_batch_output, dlg_streaming_input


## Call Leap
# @brief Call Leap
# @details A BarrierAppDrop that reads a config file, generates a command line for the LeapAccelerateCLI application, and then executes the application
# @par EAGLE_START
# @param gitrepo $(GIT_REPO)
# @param version $(PROJECT_VERSION)
# @param category PythonApp
# @param[in] param/measurementSetFilename/Measurement Set Filename/""/String/readwrite
#     \~English The file from which the input measurement set should be loaded\n
#     \~Chinese \n
#     \~
# @param[in] param/appclass/Application Class/leap_nodes.CallLeap.CallLeap/String/readonly
#     \~English The path to the class that implements this app\n
#     \~Chinese \n
#     \~
# @param[in] port/Config
#     \~English The Config file containing JSON specifying how this instance of LeapAccelerateCLI should be run
#     \~Chinese \n
#     \~
# @param[out] port/Result
#     \~English The output of the LeapAccelerateCLI application (JSON)
#     \~Chinese \n
#     \~
# @par EAGLE_END

class CallLeap(BarrierAppDROP):
    """A BarrierAppDrop that reads a config file, generates a command line for the LeapAccelerateCLI application, and then executes the application"""
    compontent_meta = dlg_component('Call Leap', 'Call Leap.',
                                    [dlg_batch_input('binary/*', [])],
                                    [dlg_batch_output('binary/*', [])],
                                    [dlg_streaming_input('binary/*')])

    # TODO: this measurementSetFilename is not being read by dlg_string_param
    #       hard-coding it for the moment
    measurementSetFilename = "/Users/james/working/leap-accelerate/testdata/1197638568-split.ms"

    DEBUG = True


    def initialize(self, **kwargs):
        super(CallLeap, self).initialize(**kwargs)


    def run(self):
        # check number of inputs and outputs
        if len(self.outputs) != 1:
            raise Exception("One output is expected by this application")
        if len(self.inputs) != 1:
            raise Exception("One input is expected by this application")

        # check that measurement set DIRECTORY exists
        if not os.path.isdir(self.measurementSetFilename):
            raise Exception("Could not find measurement set directory:" + self.measurementSetFilename)

        # read config from input
        config = self._readConfig(self.inputs[0])

        # build command line
        commandLine = [
            'LeapAccelerateCLI',
            '-f', self.measurementSetFilename,
            '-s', str(config['numStations']),
            '-d', str(config['directions']),
            '-a', str(config['autoCorrelation'])
        ]

        if self.DEBUG:
            time.sleep(random.uniform(5,10))
            self.outputs[0].write(json.dumps(commandLine))
        else:
            # call leap
            result = subprocess.run(commandLine, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.outputs[0].write(result.stdout)


    def _readConfig(self, inDrop):
        with DROPFile(inDrop) as f:
            config = json.load(f)
        return config
