
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2015
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
import os
from zipfile import ZipFile

import cwlgen

from dlg import common


#from ..common import dropdict, get_roots
logger = logging.getLogger(__name__)


def create_workflow(pgt, pgt_path, cwl_path, zip_path):
    """
    """

    # create list for command line tool description files
    step_files = []

    # create the workflow
    cwl_workflow = cwlgen.Workflow('', label='', doc='', cwl_version='v1.0')

    # create files dictionary
    files = {}

    # look for input and output files in the pg_spec
    for index, node in enumerate(pgt):
        command = node.get('command', None)
        dataType = node.get('dt', None)
        outputId = node.get('oid', None)
        outputs = node.get('outputs', [])

        if len(outputs) > 0:
            files[outputs[0]] = "step" + str(index) + "/output_file_0"

    # add steps to the workflow
    for index, node in enumerate(pgt):
        dataType = node.get('dt', '')

        if dataType == 'BashShellApp':
            name = node.get('nm', '')
            inputs = node.get('inputs', [])
            outputs = node.get('outputs', [])

            # create command line tool description
            filename = "step" + str(index) + ".cwl"
            filename_with_path = os.path.join(pgt_path, filename)
            create_command_line_tool(node, filename_with_path)
            step_files.append(filename_with_path)

            # create step
            step = cwlgen.WorkflowStep("step" + str(index), run=filename)

            # add input to step
            for index, input in enumerate(inputs):
                step.inputs.append(cwlgen.WorkflowStepInput('input_file_' + str(index), source=files[input]))

            # add output to step
            for index, output in enumerate(outputs):
                step.out.append(cwlgen.WorkflowStepOutput('output_file_' + str(index)))

            # add step to workflow
            cwl_workflow.steps.append(step)

    # save CWL to path
    with open(cwl_path, "w") as f:
        f.write(cwl_workflow.export_string())

    # put workflow and command line tool description files all together in a zip
    zipObj = ZipFile(zip_path, 'w')
    for step_file in step_files:
        zipObj.write(step_file, os.path.basename(step_file))
    zipObj.write(cwl_path, os.path.basename(cwl_path))
    zipObj.close()


def create_command_line_tool(node, filename):
    # get inputs and outputs
    inputs = node.get('inputs', [])
    outputs = node.get('outputs', [])

    # strip command down to just the basic command, with no input or output parameters
    base_command = node.get('command', '')

    # TODO: find a better way of specifying command line program + arguments
    base_command = base_command[:base_command.index(" ")]

    # cwlgen's Serializer class doesn't support python 2.7's unicode types
    cwl_tool = cwlgen.CommandLineTool(tool_id=node['app'], label=common.u2s(node['nm']), base_command=base_command, cwl_version='v1.0')

    # add inputs
    for index, input in enumerate(inputs):
        file_binding = cwlgen.CommandLineBinding(position=index)
        input_file = cwlgen.CommandInputParameter('input_file_' + str(index), param_type='File', input_binding=file_binding, doc='input file ' + str(index))
        cwl_tool.inputs.append(input_file)

    if len(inputs) == 0:
        cwl_tool.inputs.append(cwlgen.CommandInputParameter('dummy', param_type='null', doc='dummy'))

    # add outputs
    for index, output in enumerate(outputs):
        file_binding = cwlgen.CommandLineBinding()
        output_file = cwlgen.CommandOutputParameter('output_file_' + str(index), param_type='stdout', output_binding=file_binding, doc='output file ' + str(index))
        cwl_tool.outputs.append(output_file)

    # write to file
    with open(filename, "w") as f:
        f.write(cwl_tool.export_string())
