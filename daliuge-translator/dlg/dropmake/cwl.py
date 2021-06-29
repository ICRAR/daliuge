
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
from dlg.common import Categories

# the following node categories are supported by the CWL translator
SUPPORTED_CATEGORIES = [Categories.BASH_SHELL_APP, Categories.FILE]

#from ..common import dropdict, get_roots
logger = logging.getLogger(__name__)


def create_workflow(drops, cwl_filename, buffer):
    """
    Create a CWL workflow from a given Physical Graph Template

    A CWL workflow consists of multiple files. A single file describing the
    workflow, and multiple files each describing one step in the workflow. All
    the files are combined into one zip file, so that a single file can be
    downloaded by the user.

    NOTE: CWL only supports workflow steps that are bash shell applications
          Non-BashShellApp nodes are unable to be implemented in CWL
    """

    # search the drops for non-BashShellApp drops,
    # if found, the graph cannot be translated into CWL
    for index, node in enumerate(drops):
        dataType = node.get('dt', '')
        if dataType not in SUPPORTED_CATEGORIES:
            raise Exception('Node {0} has an unsupported category: {1}'.format(index, dataType))

    # create list for command line tool description files
    step_files = []

    # create the workflow
    cwl_workflow = cwlgen.Workflow('', label='', doc='', cwl_version='v1.0')

    # create files dictionary
    files = {}

    # look for input and output files in the pg_spec
    for index, node in enumerate(drops):
        command = node.get('command', None)
        dataType = node.get('dt', None)
        outputId = node.get('oid', None)
        outputs = node.get('outputs', [])

        if len(outputs) > 0:
            files[outputs[0]] = "step" + str(index) + "/output_file_0"

    # add steps to the workflow
    for index, node in enumerate(drops):
        dataType = node.get('dt', '')

        if dataType == 'BashShellApp':
            name = node.get('nm', '')
            inputs = node.get('inputs', [])
            outputs = node.get('outputs', [])

            # create command line tool description
            filename = "step" + str(index) + ".cwl"
            contents = create_command_line_tool(node)

            # add contents of command line tool description to list of step files
            step_files.append({"filename":filename, "contents": contents})

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

    # put workflow and command line tool description files all together in a zip
    zipObj = ZipFile(buffer, 'w')
    for step_file in step_files:
        zipObj.writestr(step_file["filename"], step_file["contents"].encode('utf8'))
    zipObj.writestr(cwl_filename, cwl_workflow.export_string().encode('utf8'))
    zipObj.close()


def create_command_line_tool(node):
    """
    Create a command line tool description file for a single step in a CWL
    workflow.

    NOTE: CWL only supports workflow steps that are bash shell applications
          Non-BashShellApp nodes are unable to be implemented in CWL
    """

    # get inputs and outputs
    inputs = node.get('inputs', [])
    outputs = node.get('outputs', [])

    # strip command down to just the basic command, with no input or output parameters
    base_command = node.get('command', '')

    # TODO: find a better way of specifying command line program + arguments
    base_command = base_command[:base_command.index(" ")]
    base_command = common.u2s(base_command)

    # cwlgen's Serializer class doesn't support python 2.7's unicode types
    cwl_tool = cwlgen.CommandLineTool(tool_id=common.u2s(node['app']), label=common.u2s(node['nm']), base_command=base_command, cwl_version='v1.0')

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

    return cwl_tool.export_string()
