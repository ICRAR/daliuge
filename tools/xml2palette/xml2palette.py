#!/usr/bin/env python

import csv
import getopt
import json
import logging
import random
import os
import subprocess
import sys
import tempfile
import uuid
import xml.etree.ElementTree as ET
import math
from enum import Enum

from blockdag import build_block_dag

next_key = -1

# NOTE: not sure if all of these are actually required
#       make sure to retrieve some of these from environment variables
DOXYGEN_SETTINGS = [
    ("OPTIMIZE_OUTPUT_JAVA", "YES"),
    ("AUTOLINK_SUPPORT", "NO"),
    ("IDL_PROPERTY_SUPPORT", "NO"),
    ("RECURSIVE", "YES"),
    ("EXCLUDE_PATTERNS", "*/web/*, CMakeLists.txt"),
    ("VERBATIM_HEADERS", "NO"),
    ("GENERATE_HTML", "NO"),
    ("GENERATE_LATEX", "NO"),
    ("GENERATE_XML", "YES"),
    ("XML_PROGRAMLISTING", "NO"),
    ("ENABLE_PREPROCESSING", "NO"),
    ("CLASS_DIAGRAMS", "NO"),
]

# extra doxygen setting for C repositories
DOXYGEN_SETTINGS_C = [
    ("FILE_PATTERNS", "*.h, *.hpp"),
]

DOXYGEN_SETTINGS_PYTHON = [
    ("FILE_PATTERNS", "*.py"),
]

KNOWN_PARAM_DATA_TYPES = [
    "String",
    "Integer",
    "Float",
    "Object",
    "Boolean",
    "Select",
    "Password",
    "Json",
    "Python"
]
KNOWN_CONSTRUCT_TYPES = ["Scatter", "Gather"]
KNOWN_DATA_CATEGORIES = [
    "File",
    "Memory",
    "SharedMemory",
    "NGAS",
    "S3",
    "Plasma",
    "PlasmaFlight",
    "ParameterSet",
    "EnvironmentVariables"
]

KNOWN_FIELD_TYPES = [
    "ComponentParameter",
    "ApplicationArgument",
    "InputPort",
    "OutputPort"
]

BLOCKDAG_DATA_FIELDS = [
    "inputPorts",
    "outputPorts",
    "applicationArgs",
    "category",
    "fields"
]

class Language(Enum):
    UNKNOWN = 0
    C = 1
    PYTHON = 2

def get_options_from_command_line(argv):
    inputdir = ""
    tag = ""
    outputfile = ""
    allow_missing_eagle_start = False
    module_path = ""
    language = Language.UNKNOWN
    try:
        opts, args = getopt.getopt(argv, "hi:t:o:sm:cp", ["idir=", "tag=", "ofile="])
    except getopt.GetoptError:
        print("xml2palette.py -i <input_directory> -t <tag> -o <output_file>")
        sys.exit(2)

    if len(opts) < 2:
        print("xml2palette.py -i <input_directory> -t <tag> -o <output_file>")
        sys.exit()

    for opt, arg in opts:
        if opt == "-h":
            print("xml2palette.py -i <input_directory> -t <tag> -o <output_file>")
            sys.exit()
        elif opt in ("-i", "--idir"):
            inputdir = arg
        elif opt in ("-t", "--tag"):
            tag = arg
        elif opt in ("-o", "--ofile"):
            outputfile = arg
        elif opt in ("-s"):
            allow_missing_eagle_start = True
        elif opt in ("-m", "--module"):
            module_path = arg
        elif opt in ("-c"):
            language = Language.C
        elif opt in ("-p"):
            language = Language.PYTHON
    return inputdir, tag, outputfile, allow_missing_eagle_start, module_path, language


def check_environment_variables():
    required_environment_variables = ["PROJECT_NAME", "PROJECT_VERSION", "GIT_REPO"]

    for variable in required_environment_variables:
        value = os.environ.get(variable)

        if value is None:
            if variable == 'PROJECT_NAME':
                os.environ['PROJECT_NAME'] = os.path.basename(os.path.abspath('.'))
            elif variable == 'PROJECT_VERSION':
                os.environ['PROJECT_VERSION'] = '0.1'
            elif variable == 'GIT_REPO':
                os.environ['GIT_REPO'] = os.environ['PROJECT_NAME']
            else:
                logging.error("No " + variable + " environment variable.")
                return False

    return True


def modify_doxygen_options(doxygen_filename, options):
    with open(doxygen_filename, "r") as dfile:
        contents = dfile.readlines()

    with open(doxygen_filename, "w") as dfile:
        for index, line in enumerate(contents):
            if line[0] == "#":
                continue
            if len(line) <= 1:
                continue

            parts = line.split("=")
            first_part = parts[0].strip()
            written = False

            for key, value in options:
                if first_part == key:
                    dfile.write(key + " = " + value + "\n")
                    written = True
                    break

            if not written:
                dfile.write(line)


def get_next_key():
    global next_key

    next_key -= 1

    return next_key + 1


def create_uuid(seed):
    rnd = random.Random()
    rnd.seed(seed)

    new_uuid = uuid.UUID(int=rnd.getrandbits(128), version=4)
    return new_uuid


def create_port(
    component_name, internal_name, external_name, direction, event, type, description
):
    seed = {
        "component_name": component_name,
        "internal_name": internal_name,
        "external_name": external_name,
        "direction": direction,
        "event": event,
        "type": type,
        "description": description,
    }

    port_uuid = create_uuid(str(seed))

    return {
        "Id": str(port_uuid),
        "IdText": internal_name,
        "text": external_name,
        "event": event,
        "type": type,
        "description": description,
    }


def find_field_by_name(fields, name):
    for field in fields:
        if field["name"] == name:
            return field
    return None


def check_required_fields_for_category(text, fields, category):
    if category in [
        "DynlibApp",
        "PythonApp",
        "Branch",
        "BashShellApp",
        "Mpi",
        "Docker",
    ]:
        alert_if_missing(text, fields, "execution_time")
        alert_if_missing(text, fields, "num_cpus")

    if category in ["DynlibApp", "PythonApp", "Branch", "BashShellApp", "Docker"]:
        alert_if_missing(text, fields, "group_start")

    if category == "DynlibApp":
        alert_if_missing(text, fields, "libpath")

    if category in ["PythonApp", "Branch"]:
        alert_if_missing(text, fields, "appclass")

    if category in [
        "File",
        "Memory",
        "NGAS",
        "ParameterSet",
        "Plasma",
        "PlasmaFlight",
        "S3",
    ]:
        alert_if_missing(text, fields, "data_volume")

    if category in [
        "File",
        "Memory",
        "NGAS",
        "ParameterSet",
        "Plasma",
        "PlasmaFlight",
        "S3",
        "Mpi",
    ]:
        alert_if_missing(text, fields, "group_end")

    if category in ["BashShellApp", "Mpi", "Docker", "Singularity"]:
        alert_if_missing(text, fields, "input_redirection")
        alert_if_missing(text, fields, "output_redirection")
        alert_if_missing(text, fields, "command_line_arguments")
        alert_if_missing(text, fields, "paramValueSeparator")
        alert_if_missing(text, fields, "argumentPrefix")


def create_field(
    internal_name, external_name, value, type, field_type, access, options, precious, positional, description
):
    return {
        "text": external_name,
        "name": internal_name,
        "value": value,
        "defaultValue": value,
        "description": description,
        "type": type,
        "fieldType": field_type,
        "readonly": access == "readonly",
        "options": options,
        "precious": precious,
        "positional": positional,
    }


def alert_if_missing(text, fields, internal_name):
    if find_field_by_name(fields, internal_name) is None:
        logging.warning(text + " component missing " + internal_name + " cparam")
        pass


def parse_value(text, value):
    # parse the value as csv (delimited by '/')
    parts = []
    reader = csv.reader([value], delimiter="/", quotechar='"')
    for row in reader:
        parts = row

    # init attributes of the param
    external_name = ""
    default_value = ""
    type = "String"
    field_type = "cparam"
    access = "readwrite"
    options = []
    precious = False
    positional = False
    description = ""

    # assign attributes (if present)
    if len(parts) > 0:
        external_name = parts[0]
    if len(parts) > 1:
        default_value = parts[1]
    if len(parts) > 2:
        type = parts[2]
    if len(parts) > 3:
        field_type = parts[3]
    if len(parts) > 4:
        access = parts[4]
    else:
        logging.warning(
            text
            + " "
            + field_type
            + " ("
            + external_name
            + ") has no 'access' descriptor, using default (readwrite) : "
            + value
        )
    if len(parts) > 5:
        if parts[5].strip() == "":
            options = []
        else:
            options = parts[5].strip().split(",")
    else:
        logging.warning(
            text
            + " "
            + field_type
            + " ("
            + external_name
            + ") has no 'options', using default ([]) : "
            + value
        )
    if len(parts) > 6:
        precious = parts[6].lower() == "true"
    else:
        logging.warning(
            text
            + " "
            + field_type
            + " ("
            + external_name
            + ") has no 'precious' descriptor, using default (False) : "
            + value
        )
    if len(parts) > 7:
        positional = parts[7].lower() == "true"
    else:
        logging.warning(
            text
            + " "
            + field_type
            + " ("
            + external_name
            + ") has no 'positional', using default (False) : "
            + value
        )
    if len(parts) > 8:
        description = parts[8]

    return (external_name, default_value, type, field_type, access, options, precious, positional, description)


def parse_description(value):
    # parse the value as csv (delimited by '/')
    parts = []
    reader = csv.reader([value], delimiter="/", quotechar='"')
    for row in reader:
        parts = row

    # if parts is empty
    if len(parts) == 0:
        logging.warning("unable to parse description from: " + value)
        return ""

    return parts[-1]


# NOTE: color, x, y, width, height are not specified in palette node, they will be set by the EAGLE importer
def create_palette_node_from_params(params):
    text = ""
    description = ""
    category = ""
    tag = ""
    construct = ""
    inputPorts = []
    outputPorts = []
    inputLocalPorts = []
    outputLocalPorts = []
    fields = []
    applicationArgs = []
    gitrepo = os.environ.get("GIT_REPO")
    version = os.environ.get("PROJECT_VERSION")

    # process the params
    for param in params:
        key = param["key"]
        direction = param["direction"]
        value = param["value"]

        if key == "category":
            category = value
        elif key == "construct":
            construct = value
        elif key == "tag" and not any(s in value for s in KNOWN_FIELD_TYPES):
            tag = value
        elif key == "text":
            text = value
        elif key == "description":
            description = value
        else:
            internal_name = key
            (
                external_name,
                default_value,
                type,
                field_type,
                access,
                options,
                precious,
                positional,
                description
            ) = parse_value(text, value)

            # check that type is in the list of known types
            if type not in KNOWN_PARAM_DATA_TYPES:
                #logging.warning(text + " " + field_type + " '" + name + "' has unknown type: " + type)
                pass

            # check that a param of type "Select" has some options specified,
            # and check that every param with some options specified is of type "Select"
            if type == "Select" and len(options) == 0:
                logging.warning(
                    text
                    + " "
                    + field_type
                    + " '"
                    + external_name
                    + "' is of type 'Select' but has no options specified: "
                    + str(options)
                )
            if len(options) > 0 and type != "Select":
                logging.warning(
                    text
                    + " "
                    + field_type
                    + " '"
                    + external_name
                    + "' has at least one option specified but is not of type 'Select': "
                    + type
                )

            # parse description
            if "\n" in value:
                logging.info(
                    text
                    + " description ("
                    + value
                    + ") contains a newline character, removing."
                )
                value = value.replace("\n", " ")

            # check that access is a known value
            if access != "readonly" and access != "readwrite":
                logging.warning(
                    text
                    + " "
                    + field_type
                    + " '"
                    + external_name
                    + "' has unknown 'access' descriptor: "
                    + access
                )

            # create a field from this data
            field = create_field(
                internal_name,
                external_name,
                default_value,
                type,
                field_type,
                access,
                options,
                precious,
                positional,
                description
            )

            # add the field to the correct list in the component, based on fieldType
            if field_type in KNOWN_FIELD_TYPES:
                fields.append(field)
            else:
                logging.warning(
                    text
                    + " '"
                    + external_name
                    + "' field_type is Unknown: "
                    + field_type
                )

    # check for presence of extra fields that must be included for each category
    check_required_fields_for_category(text, fields, category)

    # create and return the node
    return (
        {"tag": tag, "construct": construct},
        {
            "category": category,
            "drawOrderHint": 0,
            "key": get_next_key(),
            "text": text,
            "description": description,
            "collapsed": False,
            "showPorts": False,
            "streaming": False,
            "subject": None,
            "selected": False,
            "expanded": False,
            "inputApplicationName": "",
            "outputApplicationName": "",
            "inputApplicationType": "None",
            "outputApplicationType": "None",
            "inputPorts": inputPorts,
            "outputPorts": outputPorts,
            "inputLocalPorts": inputLocalPorts,
            "outputLocalPorts": outputLocalPorts,
            "inputAppFields": [],
            "outputAppFields": [],
            "fields": fields,
            "applicationArgs": applicationArgs,
            "git_url": gitrepo,
            "sha": version,
        },
    )


def write_palette_json(outputfile, nodes, gitrepo, version, block_dag):
    # add hashes from block_dag to the nodes
    for i in range(len(nodes)):
        nodes[i]['dataHash'] = block_dag[i]['data_hash'];

    # create the palette object
    palette = {
        "modelData": {
            "fileType": "palette",
            "repoService": "GitHub",
            "repoBranch": "master",
            "repo": "ICRAR/EAGLE_test_repo",
            "readonly": True,
            "filePath": outputfile,
            "sha": version,
            "git_url": gitrepo,
            "signature": block_dag['signature']
        },
        "nodeDataArray": nodes,
        "linkDataArray": [],
    }

    # write palette to file
    with open(outputfile, "w") as outfile:
        json.dump(palette, outfile, indent=4)


def process_compounddef(compounddef):
    result = []
    found_eagle_start = False

    # get child of compounddef called "briefdescription"
    briefdescription = None
    for child in compounddef:
        if child.tag == "briefdescription":
            briefdescription = child
            break

    if briefdescription is not None:
        if len(briefdescription) > 0:
            if briefdescription[0].text is None:
                logging.warning("No brief description text")
                result.append({"key": "text", "direction": None, "value": ""})
            else:
                result.append(
                    {
                        "key": "text",
                        "direction": None,
                        "value": briefdescription[0].text.strip(" ."),
                    }
                )

    # get child of compounddef called "detaileddescription"
    detaileddescription = None
    for child in compounddef:
        if child.tag == "detaileddescription":
            detaileddescription = child
            break

    # check that detailed description was found
    if detaileddescription is not None:

        # search children of detaileddescription node for a para node with "simplesect" children, who have "title" children with text "EAGLE_START" and "EAGLE_END"
        para = None
        description = ""
        for ddchild in detaileddescription:
            if ddchild.tag == "para":
                if ddchild.text is not None:
                    description += ddchild.text + "\n"
                for pchild in ddchild:
                    if pchild.tag == "simplesect":
                        for sschild in pchild:
                            if sschild.tag == "title":
                                if sschild.text.strip() == "EAGLE_START":
                                    found_eagle_start = True

                        para = ddchild

        # add description
        if description != "":
            result.append(
                {"key": "description", "direction": None, "value": description.strip()}
            )

    # check that we found an EAGLE_START, otherwise this is just regular doxygen, skip it
    if not found_eagle_start:
        return []

    # check that we found the correct para
    if para is None:
        return result

    # find parameterlist child of para
    parameterlist = None
    for pchild in para:
        if pchild.tag == "parameterlist":
            parameterlist = pchild
            break

    # check that we found a parameterlist
    if parameterlist is None:
        return result

    # read the parameters from the parameter list
    for parameteritem in parameterlist:
        key = None
        direction = None
        value = None
        for pichild in parameteritem:
            if pichild.tag == "parameternamelist":
                key = pichild[0].text.strip()
                direction = pichild[0].attrib.get("direction", "").strip()
            elif pichild.tag == "parameterdescription":
                if key == "gitrepo" and isinstance(pichild[0], list):
                    # the gitrepo is a URL, so is contained within a <ulink> element,
                    # therefore we need to use pichild[0][0] here to look one level deeper
                    # in the hierarchy
                    if pichild[0][0] is None or pichild[0][0].text is None:
                        logging.warning("No gitrepo text")
                        value = ""
                    else:
                        value = pichild[0][0].text.strip()
                else:
                    if pichild[0].text is None:
                        logging.warning("No key text (key: " + key + ")")
                        value = ""
                    else:
                        value = pichild[0].text.strip()

        result.append({"key": key, "direction": direction, "value": value})

    return result


def process_compounddef_default(compounddef, language):
    result = []

    # check memberdefs
    for child in compounddef:
        if child.tag == "sectiondef" and child.get("kind") == "func":

            for grandchild in child:
                if grandchild.tag == "memberdef" and grandchild.get("kind") == "function":
                    member = {"params":[]}
                    func_path = "Unknown"
                    func_name = "Unknown"
                    return_type = "Unknown"

                    # some defaults
                    # cparam format is (name, default_value, type, access, precious, options, positional, description)
                    if language == Language.C:
                        member["params"].append({"key": "category", "direction": None, "value": "DynlibApp"})
                        member["params"].append({"key": "libpath", "direction": None, "value": "Library Path//String/ComponentParameter/readwrite//False/False/The location of the shared object/DLL that implements this application"})
                    elif language == Language.PYTHON:
                        member["params"].append({"key": "category", "direction": None, "value": "PythonApp"})
                        member["params"].append({"key": "appclass", "direction": None, "value": "Application Class/dlg.apps.pyfunc.PyFuncApp/String/ComponentParameter/readwrite//False/False/The python class that implements this application"})

                    member["params"].append({"key": "execution_time", "direction": None, "value": "Execution Time/5/Integer/ComponentParameter/readwrite//False/False/Estimate of execution time (in seconds) for this application."})
                    member["params"].append({"key": "num_cpus", "direction": None, "value": "No. of CPUs/1/Integer/ComponentParameter/readwrite//False/False/Number of CPUs used for this application."})
                    member["params"].append({"key": "group_start", "direction": None, "value": "Group start/false/Boolean/ComponentParameter/readwrite//False/False/Is this node the start of a group?"})

                    for ggchild in grandchild:
                        if ggchild.tag == "name":
                            member["params"].append({"key": "text", "direction": None, "value": ggchild.text})
                            func_name = ggchild.text
                        if ggchild.tag == "detaileddescription":
                            if len(ggchild) > 0 and len(ggchild[0]) > 0 and ggchild[0][0].text != None:

                                # get detailed description text
                                dd = ggchild[0][0].text

                                # check if a return type exists in the detailed description
                                hasReturn = dd.rfind(":return:") != -1 or dd.rfind(":returns:") != -1

                                # get return type, if it exists
                                if hasReturn:
                                    return_part = dd[dd.rfind(":return:")+8:].strip().replace('\n', ' ')
                                    output_port_name = "output"
                                    print("Add output port:" + str(output_port_name) + "/" + str(return_type) + "/" + str(return_part))
                                    member["params"].append({"key": str(output_port_name), "direction": "out", "value": str(output_port_name) + "//" + str(return_type) + "/OutputPort/readwrite//False/False/" + str(return_part) })

                                # get first part of description, up until when the param are mentioned
                                description = dd[:dd.find(":param")].strip()

                                # find the list of param names and descriptions in the <detaileddescription> tag
                                params = parse_params(dd)

                                # use the params above
                                for p in params:
                                    set_param_description(p[0], p[1], member["params"])

                                member["params"].append({"key": "description", "direction": None, "value": description})
                        if ggchild.tag == "param":
                            type = ""
                            name = ""
                            default_value = ""

                            for gggchild in ggchild:
                                if gggchild.tag == "type":
                                    type = gggchild.text

                                    # also look at children with ref tag
                                    for ggggchild in gggchild:
                                        if ggggchild.tag == "ref":
                                            type = ggggchild.text
                                if gggchild.tag == "declname":
                                    name = gggchild.text
                                if gggchild.tag == "defname":
                                    name = gggchild.text
                                if gggchild.tag == "defval":
                                    default_value = gggchild.text

                            # type recognised?
                            type_recognised = False

                            # fix some types
                            if type == "bool":
                                type = "Boolean"
                                if default_value == "":
                                    default_value = "False"
                                type_recognised = True
                            if type == "int":
                                type = "Integer"
                                if default_value == "":
                                    default_value = "0"
                                type_recognised = True
                            if type == "float":
                                type = "Float"
                                if default_value == "":
                                    default_value = "0"
                                type_recognised = True
                            if type == "string" or type == "*" or type == "**":
                                type = "String"
                                type_recognised = True

                            # try to guess the type based on the default value
                            # TODO: try to parse default_value as JSON to detect JSON types
                            if not type_recognised and default_value != "" and default_value is not None and default_value != "None":
                                #print("name:" + str(name) + " default_value:" + str(default_value))

                                try:
                                    val = int(default_value)
                                    type = "Integer"
                                    #print("Use Integer")
                                except:
                                    try:
                                        val = float(default_value)
                                        type = "Float"
                                        #print("Use Float")
                                    except:
                                        if default_value.lower() == "true" or default_value.lower() == "false":
                                            type = "Boolean"
                                            default_value = default_value.lower()
                                            #print("Use Boolean")
                                        else:
                                            type = "String"
                                            #print("Use String")

                            # TODO: change
                            # add the param
                            member["params"].append({"key":str(name), "direction":"in", "value":str(name) + "/" + str(default_value) + "/" + str(type) + "/ApplicationArgument/readwrite//False/False/"})

                        if ggchild.tag == "definition":
                            return_type = ggchild.text.strip().split(" ")[0]
                            func_path = ggchild.text.strip().split(" ")[-1]

                            # TODO: change
                            # aparams
                            member["params"].append({"key": "func_name", "direction": None, "value": "Function Name/" + func_path + "/String/ApplicationArgument/readonly//False/True/Python function name"})
                            member["params"].append({"key": "pickle", "direction": None, "value": "Pickle/false/Boolean/ApplicationArgument/readwrite//False/True/Whether the python arguments are pickled."})

                            if return_type == "def":
                                return_type = "None"

                    # skip function if it begins with an underscore
                    if func_name[0] == "_":
                        continue

                    # skip component if a module_path was specified, and this component is not within it
                    if module_path != "" and not module_path in func_path:
                        #logging.info("Skip " + func_path + ". Doesn't match module path: " + module_path)
                        continue

                    result.append(member)

    return result


# find the list of param names and descriptions in the <detaileddescription> tag
def parse_params(detailed_description):
    result = []

    detailed_description = detailed_description.split("Returns:")[0]
    param_lines = [p.replace('\n','').strip() for p in detailed_description.split(":param:")[1:]]
    # param_lines = [line.strip() for line in detailed_description]

    for p_line in param_lines:
        logging.debug("p_line: %s" + p_line)

        try:
            index_of_second_colon = p_line.index(':', 0)
        except:
            # didnt find second colon, skip
            continue

        param_name = p_line[:index_of_second_colon].strip()
        param_description = p_line[index_of_second_colon+2:].strip()

        result.append((param_name, param_description))

    return result


# find the named aparam in params, and update the description
def set_param_description(name, description, params):
    #print("set_param_description():" + str(name) + ":" + str(description))
    for p in params:
        if p["key"] == name:
            p["value"] = p["value"] + description
            break


def create_construct_node(type, node):

    # check that type is in the list of known types
    if type not in KNOWN_CONSTRUCT_TYPES:
        logging.warning(text + " construct for node'" + node["text"] + "' has unknown type: " + type)
        pass

    construct_node = {
        "category": type,
        "description": "A default "
        + type
        + " construct for the "
        + node["text"]
        + " component.",
        "fields": [],
        "applicationArgs": [],
        "git_url": gitrepo,
        "key": get_next_key(),
        "precious": False,
        "sha": version,
        "streaming": False,
        "text": type + "/" + node["text"],
    }

    if type == "Scatter" or type == "Gather":
        construct_node["inputAppFields"] = node["fields"]
        construct_node["inputAppArgs"] = node["applicationArgs"]
        construct_node["inputApplicationKey"] = node["key"]
        construct_node["inputApplicationName"] = node["text"]
        construct_node["inputApplicationType"] = node["category"]
        construct_node["inputApplicationDescription"] = node["description"]
        construct_node["inputLocalPorts"] = node["outputPorts"]
        construct_node["inputPorts"] = node["inputPorts"]
        construct_node["outputAppFields"] = []
        construct_node["outputAppArgs"] = []
        construct_node["outputApplicationKey"] = None
        construct_node["outputApplicationName"] = ""
        construct_node["outputApplicationType"] = "None"
        construct_node["outputApplicationDescription"] = ""
        construct_node["outputLocalPorts"] = []
        construct_node["outputPorts"] = []
    else:
        pass  # not sure what to do for other types like MKN yet

    return construct_node


def params_to_nodes(params):
    result = []

    # if no params were found, or only the name and description were found, then don't bother creating a node
    if len(params) > 2:
        # create a node
        data, node = create_palette_node_from_params(params)

        # if the node tag matches the command line tag, or no tag was specified on the command line, add the node to the list to output
        if data["tag"] == tag or tag == "":
            logging.info("Adding component: " + node["text"])
            result.append(node)

            # if a construct is found, add to nodes
            if data["construct"] != "":
                logging.info("Adding component: " + data["construct"] + "/" + node["text"])
                construct_node = create_construct_node(data["construct"], node)
                result.append(construct_node)

    # check if gitrepo and version params were found and cache the values
    for param in params:
        key = param["key"]
        value = param["value"]

        if key == "gitrepo":
            gitrepo = value
        elif key == "version":
            version = value

    return result


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s - %(message)s",
        datefmt="%d-%b-%y %H:%M:%S",
        level=logging.INFO,
    )

    # read environment variables
    if not check_environment_variables():
        sys.exit(1)

    logging.info("PROJECT_NAME:" + os.environ.get("PROJECT_NAME"))
    logging.info("PROJECT_VERSION:" + os.environ.get("PROJECT_VERSION"))
    logging.info("GIT_REPO:" + os.environ.get("GIT_REPO"))

    (inputdir, tag, outputfile, allow_missing_eagle_start, module_path, language) = get_options_from_command_line(sys.argv[1:])
    logging.info("Input Directory:" + inputdir)
    logging.info("Tag:" + tag)
    logging.info("Output File:" + outputfile)
    logging.info("Allow missing EAGLE_START:" + str(allow_missing_eagle_start))
    logging.info("Module Path:" + module_path)

    # create a temp directory for the output of doxygen
    output_directory = tempfile.TemporaryDirectory()

    # add extra doxygen setting for input and output locations
    DOXYGEN_SETTINGS.append(("PROJECT_NAME", os.environ.get("PROJECT_NAME")))
    DOXYGEN_SETTINGS.append(("INPUT", inputdir))
    DOXYGEN_SETTINGS.append(("OUTPUT_DIRECTORY", output_directory.name))

    # create a temp file to contain the Doxyfile
    doxygen_file = tempfile.NamedTemporaryFile()
    doxygen_filename = doxygen_file.name
    doxygen_file.close()

    # create a default Doxyfile
    subprocess.call(
        ["doxygen", "-g", doxygen_filename],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    logging.info("Wrote doxygen configuration file (Doxyfile) to " + doxygen_filename)

    # modify options in the Doxyfile
    modify_doxygen_options(doxygen_filename, DOXYGEN_SETTINGS)

    if language == Language.C:
        modify_doxygen_options(doxygen_filename, DOXYGEN_SETTINGS_C)
    elif language == Language.PYTHON:
        modify_doxygen_options(doxygen_filename, DOXYGEN_SETTINGS_PYTHON)

    # run doxygen
    # os.system("doxygen " + doxygen_filename)
    subprocess.call(
        ["doxygen", doxygen_filename],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # run xsltproc
    output_xml_filename = output_directory.name + "/xml/doxygen.xml"

    with open(output_xml_filename, "w") as outfile:
        subprocess.call(
            [
                "xsltproc",
                output_directory.name + "/xml/combine.xslt",
                output_directory.name + "/xml/index.xml",
            ],
            stdout=outfile,
            stderr=subprocess.DEVNULL,
        )

    # debug - copy output xml to local dir
    os.system('cp ' + output_xml_filename + ' output.xml')
    logging.info("Wrote doxygen XML to output.xml")

    # get environment variables
    gitrepo = os.environ.get("GIT_REPO")
    version = os.environ.get("PROJECT_VERSION")

    # init nodes array
    nodes = []

    # load the input xml file
    tree = ET.parse(output_xml_filename)
    xml_root = tree.getroot()

    for compounddef in xml_root:

        # debug - we need to determine this correctly
        is_eagle_node = False

        if is_eagle_node or not allow_missing_eagle_start:
            params = process_compounddef(compounddef)

            ns = params_to_nodes(params)
            nodes.extend(ns)

        else: # not eagle node
            functions = process_compounddef_default(compounddef, language)

            for f in functions:
                ns = params_to_nodes(f["params"])

                for n in ns:
                    alreadyPresent = False
                    for node in nodes:
                        if node["text"] == n["text"]:
                            alreadyPresent = True

                    #print("component " + n["text"] + " alreadyPresent " + str(alreadyPresent))

                    if not alreadyPresent:
                        nodes.append(n)

    # add signature for whole palette using BlockDAG
    vertices = {}
    for i in range(len(nodes)):
        vertices[i] = nodes[i]
    block_dag = build_block_dag(vertices, [], data_fields=BLOCKDAG_DATA_FIELDS)

    # write the output json file
    write_palette_json(outputfile, nodes, gitrepo, version, block_dag)
    logging.info("Wrote " + str(len(nodes)) + " component(s)")

    # cleanup the output directory
    output_directory.cleanup()
