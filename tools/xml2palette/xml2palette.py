#!/usr/bin/env python
"""
Script processes a file or a directory of source files and
produces a DALiuGE compatible palette file containing the
information required to use functions and components in graphs.
For more information please refer to the documentation
https://daliuge.readthedocs.io/en/latest/development/app_development/eagle_app_integration.html#automatic-eagle-palette-generation


TODO: This whole tool needs re-factoring into separate class files 
(compound, child, grandchild, grandgrandchild, node, param, pluggable parsers)
Should also be made separate sub-repo with proper installation and entry point.
"""

import argparse
import csv
from dataclasses import dataclass
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
import ast
import re
import types
from enum import Enum

from blockdag import build_block_dag

next_key = -1

# NOTE: not sure if all of these are actually required
#       make sure to retrieve some of these from environment variables
DOXYGEN_SETTINGS = [
    ("OPTIMIZE_OUTPUT_JAVA", "YES"),
    ("AUTOLINK_SUPPORT", "NO"),
    ("IDL_PROPERTY_SUPPORT", "NO"),
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
    "Python",
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
    "EnvironmentVariables",
]

KNOWN_FIELD_TYPES = [
    "ComponentParameter",
    "ApplicationArgument",
    "ConstructParameter",
    "InputPort",
    "OutputPort",
]

VALUE_TYPES = {
    str: "String",
    int: "Integer",
    float: "Float",
    bool: "Boolean",
    list: "Json",
    dict: "Json",
    tuple: "Json",
}

BLOCKDAG_DATA_FIELDS = [
    "inputPorts",
    "outputPorts",
    "applicationArgs",
    "category",
    "fields",
]


class Language(Enum):
    UNKNOWN = 0
    C = 1
    PYTHON = 2


def get_args():
    """
    Deal with the command line arguments

    :returns (
                args.idir:str,
                args.tag:str,
                args.ofile:str,
                args.parse_all:bool,
                args.module:str,
                language)
    """
    # inputdir, tag, outputfile, allow_missing_eagle_start, module_path, language
    parser = argparse.ArgumentParser(
        epilog=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("idir", help="input directory path or file name")
    parser.add_argument("ofile", help="output file name")
    parser.add_argument(
        "-m", "--module", help="Module load path name", default=""
    )
    parser.add_argument(
        "-t", "--tag", help="filter components with matching tag", default=""
    )
    parser.add_argument(
        "-c",
        help="C mode, if not set Python will be used",
        action="store_true",
    )
    parser.add_argument(
        "-r",
        "--recursive",
        help="Traverse sub-directories",
        action="store_true",
    )
    parser.add_argument(
        "-s",
        "--parse_all",
        help="Try to parse non DAliuGE compliant functions and methods",
        action="store_true",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="increase output verbosity",
        action="store_true",
    )
    args = parser.parse_args()
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    logger.debug("DEBUG logging switched on")
    if args.recursive:
        DOXYGEN_SETTINGS.append(("RECURSIVE", "YES"))
        logger.info("Recursive flag ON")
    else:
        DOXYGEN_SETTINGS.append(("RECURSIVE", "NO"))
        logger.info("Recursive flag OFF")
    language = Language.C if args.c else Language.PYTHON
    return (
        args.idir,
        args.tag,
        args.ofile,
        args.parse_all,
        args.module,
        language,
    )


def check_environment_variables() -> bool:
    """
    Check environment variables and set them if not defined.

    :returns True
    """
    required_environment_variables = [
        "PROJECT_NAME",
        "PROJECT_VERSION",
        "GIT_REPO",
    ]

    for variable in required_environment_variables:
        value = os.environ.get(variable)

        if value is None:
            if variable == "PROJECT_NAME":
                os.environ["PROJECT_NAME"] = os.path.basename(
                    os.path.abspath(".")
                )
            elif variable == "PROJECT_VERSION":
                os.environ["PROJECT_VERSION"] = "0.1"
            elif variable == "GIT_REPO":
                os.environ["GIT_REPO"] = os.environ["PROJECT_NAME"]
            else:
                logger.error("No " + variable + " environment variable.")
                return False

    return True


def modify_doxygen_options(doxygen_filename: str, options: dict):
    """
    Updates default doxygen config for this task

    :param doxygen_filename: str, the file name of the config file
    :param options: dict, dictionary of the options to be modified
    """
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
    """
    TODO: This needs to disappear!!
    """
    global next_key

    next_key -= 1

    return next_key + 1


def create_uuid(seed):
    """
    Simple helper function to create a UUID

    :param seed: [int| str| bytes| bytearray], seed value, if not provided timestamp is used

    :returns uuid
    """
    rnd = random.Random()
    rnd.seed(seed)

    new_uuid = uuid.UUID(int=rnd.getrandbits(128), version=4)
    return new_uuid


def create_port(
    component_name,
    internal_name,
    external_name,
    direction,
    event,
    value_type,
    description,
) -> dict:
    """
    Create the dict data structure used to describe a port
    TODO: This should be a dataclass

    :param component_name: str, the name of the component
    :param internal_name: str, the identifier name for the component
    :param external_name: str, the display name of the component
    :param direction: str, ['input'|'output']
    :param event: str, if event this string contains event name
    :param value_type: str, type of the port (not limited to standard data types)
    :param description: str, short description of the port

    :return dict: {
                    'Id':uuid,
                    'IdText': internal_name,
                    'text': external_name,
                    'event': event,
                    'type': value_type,
                    'description': description
                    }
    """
    seed = {
        "component_name": component_name,
        "internal_name": internal_name,
        "external_name": external_name,
        "direction": direction,
        "event": event,
        "type": value_type,
        "description": description,
    }

    port_uuid = create_uuid(str(seed))

    return {
        "Id": str(port_uuid),
        "IdText": internal_name,
        "name": external_name,
        "event": event,
        "type": value_type,
        "description": description,
    }


def find_field_by_name(fields, name):
    """
    Get a field from a list of field dictionaries.

    :param fields: list, list of field dictionaries
    :param name: str, field name to check for

    :returns field dict if found, else None
    """
    for field in fields:
        if field["name"] == name:
            return field
    return None


def _check_required_fields_for_category(
    text: str, fields: list, category: str
):
    """
    Check if fields have mandatory content and alert with <text> if not.

    :param text: str, the text to be used for the alert
    :param fields: list of field dicts to be checked
    :param category: str, category to be checked
    """
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

    if category in [
        "DynlibApp",
        "PythonApp",
        "Branch",
        "BashShellApp",
        "Docker",
    ]:
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
    internal_name: str,
    external_name: str,
    value: str,
    value_type: str,
    field_type: str,
    access: str,
    options: str,
    precious: bool,
    positional: bool,
    description: str,
):
    """
    TODO: field should be a dataclass
    For now just create a dict using the values provided

    :param internal_name: str, the internal name of the parameter
    :param external_name: str, the visible name of the parameter
    :param value: str, the value of the parameter
    :param value_type: str, the type of the value
    :param field_type: str, the type of the field
    :param access: str, readwrite|readonly (default readonly)
    :param options: str, options
    :param precious: bool,
        should this parameter appear, even if empty or None
    :param positional: bool,
        is this a positional parameter
    :param description: str, the description used in the palette

    :returns field: dict
    """
    return {
        "name": internal_name,
        "value": value,
        "defaultValue": value,
        "description": description,
        "type": value_type,
        "fieldType": field_type,
        "readonly": access == "readonly",
        "options": options,
        "precious": precious,
        "positional": positional,
    }


def alert_if_missing(message: str, fields: list, internal_name: str):
    """
    Produce a warning message using <text> if a field with <internal_name>
    does not exist.

    :param message: str, message text to be used
    :param fields: list of dicts of field definitions
    :param internal_name: str, identifier name of field to check
    """
    if find_field_by_name(fields, internal_name) is None:
        logger.warning(
            message + " component missing " + internal_name + " cparam"
        )
        pass


def parse_value(message: str, value: str) -> tuple:
    """
    Parse the value from the EAGLE compatible string. These are csv strings
    delimited by '/'
    TODO: This parser should be pluggable

    :param message: str, message text to be used for messages.
    :param value: str, the csv string to be parsed

    :returns tuple of parsed values
    """
    parts = []
    reader = csv.reader([value], delimiter="/", quotechar='"', escapechar="\\")
    for row in reader:
        parts = row

    # init attributes of the param
    external_name = ""
    default_value = ""
    value_type = "String"
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
        value_type = parts[2]
    if len(parts) > 3:
        field_type = parts[3]
    if len(parts) > 4:
        access = parts[4]
    else:
        logger.warning(
            message
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
        logger.warning(
            message
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
        logger.warning(
            message
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
        logger.warning(
            message
            + " "
            + field_type
            + " ("
            + external_name
            + ") has no 'positional', using default (False) : "
            + value
        )
    if len(parts) > 8:
        description = parts[8]

    return (
        external_name,
        default_value,
        value_type,
        field_type,
        access,
        options,
        precious,
        positional,
        description,
    )


def parse_description(value: str) -> str:
    """
    Parse the description from a EAGLE formatted csv string.

    :param value: str, csv string to be parsed

    :returns: str, last item from parsed csv

    TODO: This parser should be pluggable
    """
    # parse the value as csv (delimited by '/')
    parts = []
    reader = csv.reader([value], delimiter="/", quotechar='"')
    for row in reader:
        parts = row

    # if parts is empty
    if len(parts) == 0:
        logger.warning("unable to parse description from: " + value)
        return ""

    return parts[-1]


# NOTE: color, x, y, width, height are not specified in palette node, they will be set by the EAGLE importer
def create_palette_node_from_params(params) -> tuple:
    """
    Construct the palette node entry from the parameter structure

    TODO: Should split this up into individual parts

    :param params: list of dicts of params

    :returns tuple of dicts

    TODO: This should return a node dataclass object
    """
    text = ""
    description = ""
    comp_description = ""
    category = ""
    tag = ""
    construct = ""
    inputPorts = []
    outputPorts = []
    inputLocalPorts = []
    outputLocalPorts = []
    fields = []
    construct_fields = []
    applicationArgs = []

    # process the params
    for param in params:
        if not isinstance(param, dict):
            logger.error(
                "param %s has wrong type %s. Ignoring!", param, type(param)
            )
            continue
        key = param["key"]
        direction = param["direction"]
        value = param["value"]

        if key == "category":
            category = value
        elif key == "construct":
            construct = value
        elif key == "tag" and not any(s in value for s in KNOWN_FIELD_TYPES):
            tag = value
        elif key == "name":
            text = value
        elif key == "description":
            comp_description = value
        else:
            internal_name = key
            (
                external_name,
                default_value,
                value_type,
                field_type,
                access,
                options,
                precious,
                positional,
                description,
            ) = parse_value(text, value)

            # check that type is in the list of known types
            if value_type not in KNOWN_PARAM_DATA_TYPES:
                # logger.warning(text + " " + field_type + " '" + name + "' has unknown type: " + type)
                pass

            # check that a param of type "Select" has some options specified,
            # and check that every param with some options specified is of type "Select"
            if value_type == "Select" and len(options) == 0:
                logger.warning(
                    text
                    + " "
                    + field_type
                    + " '"
                    + external_name
                    + "' is of type 'Select' but has no options specified: "
                    + str(options)
                )
            if len(options) > 0 and value_type != "Select":
                logger.warning(
                    text
                    + " "
                    + field_type
                    + " '"
                    + external_name
                    + "' has at least one option specified but is not of type 'Select': "
                    + value_type
                )

            # parse description
            if "\n" in value:
                logger.info(
                    text
                    + " description ("
                    + value
                    + ") contains a newline character, removing."
                )
                value = value.replace("\n", " ")

            # check that access is a known value
            if access != "readonly" and access != "readwrite":
                logger.warning(
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
                value_type,
                field_type,
                access,
                options,
                precious,
                positional,
                description,
            )

            # add the field to the correct list in the component, based on fieldType
            if field_type in KNOWN_FIELD_TYPES:
                if field_type == "ConstructParameter":
                    construct_fields.append(field)
                else:
                    fields.append(field)
            else:
                logger.warning(
                    text
                    + " '"
                    + external_name
                    + "' field_type is Unknown: "
                    + field_type
                )

    # check for presence of extra fields that must be included for each category
    _check_required_fields_for_category(text, fields, category)
    # create and return the node
    return (
        {"tag": tag, "construct": construct},
        {
            "category": category,
            "drawOrderHint": 0,
            "key": get_next_key(),
            "name": text,
            "description": comp_description,
            "collapsed": False,
            "showPorts": False,
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
            "constructFields": construct_fields,
            "fields": fields,
            "applicationArgs": applicationArgs,
            "repositoryUrl": gitrepo,
            "commitHash": version,
            "paletteDownloadUrl": "",
            "dataHash": "",
        },
    )


def write_palette_json(
    outputfile: str, nodes: list, gitrepo: str, version: str, block_dag: list
):
    """
    Construct palette header and Write nodes to the output file

    :param outputfile: str, the name of the output file
    :param nodes: list of nodes
    :param gitrepo: str, the gitrepo URL
    :param version: str, version string to be used
    :param block_dag: list, the reproducibility information
    """
    for i in range(len(nodes)):
        nodes[i]["dataHash"] = block_dag[i]["data_hash"]
    palette = {
        "modelData": {
            "fileType": "palette",
            "repoService": "GitHub",
            "repoBranch": "master",
            "repo": "ICRAR/EAGLE_test_repo",
            "readonly": True,
            "filePath": outputfile,
            "repositoryUrl": gitrepo,
            "commitHash": version,
            "downloadUrl": "",
            "signature": block_dag["signature"],
        },
        "nodeDataArray": nodes,
        "linkDataArray": [],
    }

    # write palette to file
    with open(outputfile, "w") as outfile:
        json.dump(palette, outfile, indent=4)


def _typeFix(value_type: str, default_value: str = None) -> str:
    """
    Trying to fix or guess the type of a parameter

    :param value_type: str, convert type string to something known

    :returns output_type: str, the converted type
    """
    type_recognised = False
    # fix some types
    if value_type == "bool":
        value_type = "Boolean"
        if default_value == "":
            default_value = "False"
        type_recognised = True
    if value_type == "int":
        value_type = "Integer"
        if default_value == "":
            default_value = "0"
        type_recognised = True
    if value_type == "float":
        value_type = "Float"
        if default_value == "":
            default_value = "0"
        type_recognised = True
    if value_type in ["string", "str", "*", "**"]:
        value_type = "String"
        type_recognised = True

    # try to guess the type based on the default value
    # TODO: try to parse default_value as JSON to detect JSON types

    if (
        not type_recognised
        and default_value != ""
        and default_value is not None
        and default_value != "None"
    ):
        # print("name:" + str(name) + " default_value:" + str(default_value))
        try:
            # we'll try to interpret what the type of the default_value is using ast
            l = {}
            try:
                eval(
                    compile(
                        ast.parse(f"t = {default_value}"),
                        filename="",
                        mode="exec",
                    ),
                    l,
                )
                vt = type(l["t"])
                if not isinstance(l["t"], type):
                    default_value = l["t"]
                else:
                    vt = str
            except NameError:
                vt = str
            except SyntaxError:
                vt = str

            value_type = VALUE_TYPES[vt] if vt in VALUE_TYPES else "String"
            if value_type == "String":
                # if it is String we need to do a few more tests
                try:
                    val = int(default_value)
                    value_type = "Integer"
                    # print("Use Integer")
                except TypeError:
                    if isinstance(default_value, types.BuiltinFunctionType):
                        value_type = "String"
                except:
                    try:
                        val = float(default_value)
                        value_type = "Float"
                        # print("Use Float")
                    except:
                        if (
                            default_value.lower() == "true"
                            or default_value.lower() == "false"
                        ):
                            value_type = "Boolean"
                            default_value = default_value.lower()
                            # print("Use Boolean")
                        else:
                            value_type = "String"
                            # print("Use String")
        except NameError or TypeError:
            raise

    return value_type


class greatgrandchild:
    """
    The great-grandchild class performs most of the parsing to construct the
    palette nodes from the doxygen XML.
    """

    KNOWN_FORMATS = {
        "rEST": r"\n:param .*",
        "Google": r"\nArgs:",
        "Numpy": r"\nParameters\n----------",
    }

    def __init__(
        self,
        ggchild: dict = {},
        func_name: str = "Unknown",
        return_type: str = "Unknown",
    ):
        """
        Constructor of great-grandchild object.

        :param ggchild: dict, if existing great-grandchild
        :param func_name: str, the function name
        :param return_type: str, the return type of the component
        """

        self.func_path = ""
        self.func_name = func_name
        self.fname = func_name
        self.return_type = return_type
        self.pcount = 0  # number of parameters
        if ggchild:
            self.member = self._process_greatgrandchild(ggchild)
        else:
            self.member = {"params": []}

    def _process_rEST(self, detailed_description) -> tuple:
        """
        Parse parameter descirptions found in a detailed_description tag. This assumes
        rEST style documentation.

        :param detailed_description: str, the content of the description XML node

        :returns: tuple, description and parameter dictionary
        """
        logger.debug("Processing rEST style doc_strings")
        result = {}

        if detailed_description.find("Returns:") >= 0:
            split_str = "Returns:"
        elif detailed_description.find(":returns") >= 0:
            split_str = ":returns"
        else:
            split_str = ""
        detailed_description = (
            detailed_description.split(split_str)[0]
            if split_str
            else detailed_description
        )
        param_lines = [
            p.replace("\n", "").strip()
            for p in detailed_description.split(":param")[1:]
        ]
        type_lines = [
            p.replace("\n", "").strip()
            for p in detailed_description.split(":type")[1:]
        ]
        # param_lines = [line.strip() for line in detailed_description]

        for p_line in param_lines:
            # logger.debug("p_line: %s", p_line)

            try:
                index_of_second_colon = p_line.index(":", 0)
            except:
                # didnt find second colon, skip
                # logger.debug("Skipping this one: %s", p_line)
                continue

            param_name = p_line[:index_of_second_colon].strip()
            param_description = p_line[index_of_second_colon + 2 :].strip()
            t_ind = param_description.find(":type")
            t_ind = t_ind if t_ind > -1 else None
            param_description = param_description[:t_ind]
            # logger.debug("%s description: %s", param_name, param_description)

            if len(type_lines) != 0:
                result.update(
                    {param_name: {"desc": param_description, "type": None}}
                )
            else:
                result.update(
                    {
                        param_name: {
                            "desc": param_description,
                            "type": _typeFix(
                                re.split(
                                    r"[,\s\n]", param_description.strip()
                                )[0]
                            ),
                        }
                    }
                )

        for t_line in type_lines:
            # logger.debug("t_line: %s", t_line)

            try:
                index_of_second_colon = t_line.index(":", 0)
            except:
                # didnt find second colon, skip
                # logger.debug("Skipping this one: %s", t_line)
                continue

            param_name = t_line[:index_of_second_colon].strip()
            param_type = t_line[index_of_second_colon + 2 :].strip()
            p_ind = param_type.find(":param")
            p_ind = p_ind if p_ind > -1 else None
            param_type = param_type[:p_ind]
            param_type = _typeFix(param_type)
            # logger.debug("%s type after fix: %s", param_name, param_type)

            if param_name in result:
                result[param_name]["type"] = param_type
            else:
                logger.warning(
                    "No parameter named %s found in parameter dictionary. Known parameters are: %s",
                    param_name,
                    ", ".join(str(key) for key in result.keys()),
                )
            rdict = {}  # TODO
        return detailed_description.split(":param")[0], result, rdict

    def _process_Numpy(self, dd: str) -> tuple:
        """
        Process the Numpy-style docstring

        :param dd: str, the content of the detailed description tag

        :returns: tuple, description and parameter dictionary
        """
        logger.debug("Processing Numpy style doc_strings")
        ds = "\n".join(
            [d.strip() for d in dd.split("\n")]
        )  # remove whitespace from lines
        # extract main documentation (up to Parameters line)
        (description, rest) = ds.split("\nParameters\n----------\n")
        # extract parameter documentation (up to Returns line)
        pds = rest.split("\nReturns\n-------\n")
        spds = re.split("([\w_]+) :", pds[0])[1:]  # split :param lines
        pdict = dict(zip(spds[::2], spds[1::2]))  # create initial param dict
        pdict = {
            k: {
                "desc": v.replace("\n", " "),
                # this cryptic line tries to extract the type
                "type": _typeFix(re.split(r"[,\n\s]", v.strip())[0]),
            }
            for k, v in pdict.items()
        }
        logger.debug("numpy_style param dict %r", pdict)
        # extract return documentation
        rest = pds[1] if len(pds) > 1 else ""
        ret = re.split("\nRaises\n------\n", rest)
        rdict = {}  # TODO
        rai = ret[1] if len(ret) > 1 else ""
        return description, pdict, rdict

    def _process_Google(self, dd: str):
        """
        Process the Google-style docstring
        TODO: not yet implemented

        :param dd: str, the content of the detailed description tag

        :returns: tuple, description and parameter dictionary
        """
        logger.debug("Processing Google style doc_strings")
        ds = "\n".join(
            [d.strip() for d in dd.split("\n")]
        )  # remove whitespace from lines
        # extract main documentation (up to Parameters line)
        (description, rest) = ds.split("\nArgs:")
        # logger.debug("Splitting: %s %s", description, rest)
        # extract parameter documentation (up to Returns line)
        pds = re.split("\nReturns?:\n(.+)\n", rest)
        spds = re.split(r"\n?([\w_]+)\s?\((\w+.+)\)\s?:", pds[0])[
            1:
        ]  # split param lines
        types = spds[1::3]
        types = [re.split(r"[\,\s]", t)[0] for t in types]
        pdict = dict(
            zip(spds[::3], zip(types, spds[2::3]))
        )  # create initial param dict
        pdict = {
            k: {"desc": v[1].replace("\n", " "), "type": _typeFix(v[0])}
            for k, v in pdict.items()
        }
        # extract return documentation
        ret = pds[1] if len(pds) > 1 else ""
        rest = pds[2] if len(pds) > 2 else ""
        logger.debug("Return string: %s", ret)
        if ret:
            rpds = re.split(r"\n?\(([\w_]+)\)\s?:", ret)  # split return lines
            if len(rpds) > 1:  # complete with type
                rpds = rpds[1:]
            else:  # if something else use first word and type Unknown
                rpds = re.findall(r"\w+", rpds[0])[0:1] + ["Unknown"]
            rdict = dict(
                zip(rpds[::3], zip(rpds[1::3]))
            )  # create initial return dict
            rdict = {
                k: {"desc": v[0].replace("\n", " "), "type": _typeFix(k)}
                for k, v in rdict.items()
            }
        else:
            rdict = {}
        rai = ret[1] if len(ret) > 1 else ""
        logger.debug("Raises string: %s", rai)
        return description, pdict, rdict

    def _identify_format(self, descr_string: str) -> str:
        """
        :param descr_string: str, the content of the detailed description tag

        :returns: str, the identified format or None
        """
        logger.debug("Identifying doc_string style format")
        dd = descr_string.split("\n")
        ds = "\n".join([d.strip() for d in dd])  # remove whitespace from lines
        for k, v in self.KNOWN_FORMATS.items():
            rc = re.compile(v)
            if rc.search(ds):
                return k
        logger.info("Unknown format of docstring: Not parsing it!")
        return None

    def process_descr(self, name: str, dd):
        """
        Helper function to provide plugin style parsers for various
        formats.

        :param name: str, name of the processor to call
        :param dd: str, the detailed description docstring
        """
        do = f"_process_{name}"
        if hasattr(self, do) and callable(func := getattr(self, do)):
            logger.debug("Calling %s parser function", do)
            return func(dd)
        else:
            logger.error(
                "Don't know or can't execute %s",
            )

    def process_greatgrandchild(self, ggchild: dict) -> dict:
        """
        Process Greatgrandchild

        :param ggchild: dict, the great grandchild element

        :returns member dict
        """

        logger.debug("Initialized ggchild member: %s", self.member)
        logger.debug("greatgrandchild element: %s", ggchild.tag)
        if ggchild.tag == "name":
            self.func_name = (
                ggchild.text if self.func_name == "Unknown" else self.func_name
            )
            self.member["params"].append(
                {"key": "name", "direction": None, "value": self.func_name}
            )
        if ggchild.tag == "argsstring":
            args = ggchild.text[1:-1]  # get rid of parantheses
            args = [a.strip() for a in args.split(",")]
            if "self" in args:
                self.class_name = self.func_path.rsplit(".", 1)[-1]
                self.fname = self.func_name
                self.func_name = f"{self.class_name}::{self.func_name}"

        if ggchild.tag == "detaileddescription":
            # this contains the main description of the function and the parameters.
            # Might not be complete or correct and has to be merged with the information
            # in the param section below.
            direction = None
            if (
                len(ggchild) > 0
                and len(ggchild[0]) > 0
                and ggchild[0][0].text != None
            ):
                # get description, params and return
                dd = ggchild[0][0].text
                d_format = self._identify_format(dd)  # identify docstyle
                if d_format:
                    # process docstyle
                    (desc, params, ret) = self.process_descr(d_format, dd)
                else:
                    (desc, params, ret) = dd, {}, {}

                # use the params above
                for p_key, p_value in params.items():
                    set_param_description(
                        p_key,
                        p_value["desc"],
                        p_value["type"],
                        self.member["params"],
                    )
                    direction = None
                for r_key, r_value in ret.items():
                    set_param_description(
                        r_key,
                        r_value["desc"],
                        r_value["type"],
                        self.member["params"],
                    )
                    direction = "Out"
                    logger.debug(
                        "adding port: %s",
                        {
                            "key": r_key,
                            "direction": "out",
                            "value": r_value["type"]
                            + "//"
                            + r_value["type"]
                            + "/OutputPort/readwrite//False/False/",
                        },
                    )
                    self.member["params"].append(
                        {
                            "key": r_key,
                            "direction": "out",
                            "value": r_value["type"]
                            + "//"
                            + r_value["type"]
                            + "/OutputPort/readwrite//False/False/",
                        }
                    )

                logger.debug(
                    "adding description param: %s",
                    {
                        "key": "description",
                        "direction": direction,
                        "value": desc,
                    },
                )
                self.member["params"].append(
                    {
                        "key": "description",
                        "direction": direction,
                        "value": desc,
                    }
                )

        if ggchild.tag == "param":
            # Depending on the format used this section only contains parameter names
            # this should be merged with the detaileddescription element above, keeping in
            # mind that the description might be wrong and/or incomplete.
            value_type = ""
            name = ""
            default_value = ""
            self.pcount += 1

            for gggchild in ggchild:
                if gggchild.tag == "type":
                    value_type = gggchild.text
                    if value_type not in VALUE_TYPES.values():
                        value_type = f"Object.{value_type}"
                    # also look at children with ref tag
                    for ggggchild in gggchild:
                        if ggggchild.tag == "ref":
                            value_type = ggggchild.text
                if gggchild.tag == "declname":
                    name = gggchild.text
                if gggchild.tag == "defname":
                    name = gggchild.text
                if gggchild.tag == "defval":
                    default_value = gggchild.text
            # if str(name) == "self" and \
            #     self.func_name.rsplit("::",1)[-1] in ["__init__", "__class__"]:
            #     return None
            if (
                name in self.member["params"]
                and "type" in self.member["params"][name]
            ):
                logger.debug(
                    "Existing type definition found for %s: %s",
                    name,
                    self.member["params"][name]["type"],
                )
                value_type = self.member["params"][name]["type"]
            # type recognised?

            value_type = _typeFix(value_type, default_value=default_value)

            # add the param
            if str(value_type) == "String":
                default_value = str(default_value).replace("'", "")
                if default_value.find("/") >= 0:
                    default_value = f'"{default_value}"'
            logger.debug("Func name %s, pcount %d", self.fname, self.pcount)
            if (
                self.fname in ["__init__", "__call__"] and self.pcount == 1
            ):  # first parameter is "self"
                value_type = f"Object.{self.class_name}"
                logger.debug(
                    "adding port: %s",
                    {
                        "key": str(name),
                        "direction": "out",
                        "value": self.class_name
                        + "/"
                        + str(default_value)
                        + "/"
                        + str(value_type)
                        + "/OutputPort/readwrite//False/False/",
                    },
                )
                self.member["params"].append(
                    {
                        "key": str(name),
                        "direction": "out",
                        "value": self.class_name
                        + "/"
                        + str(default_value)
                        + "/"
                        + str(value_type)
                        + "/OutputPort/readwrite//False/False/",
                    }
                )
            elif (
                hasattr(self, "class_name")
                and self.func_name != self.fname
                and self.pcount == 1
            ):
                value_type = f"Object.{self.class_name}"
                logger.debug(
                    "adding port: %s",
                    {
                        "key": str(name),
                        "direction": "in",
                        "value": self.class_name
                        + "/"
                        + str(default_value)
                        + "/"
                        + str(value_type)
                        + "/InPort/readwrite//False/False/",
                    },
                )
                self.member["params"].append(
                    {
                        "key": str(name),
                        "direction": "in",
                        "value": self.class_name
                        + "/"
                        + str(default_value)
                        + "/"
                        + str(value_type)
                        + "/InputPort/readwrite//False/False/",
                    }
                )
            else:
                logger.debug(
                    "adding param: %s",
                    {
                        "key": str(name),
                        "direction": "in",
                        "value": str(name)
                        + "/"
                        + str(default_value)
                        + "/"
                        + str(value_type)
                        + "/ApplicationArgument/readwrite//False/False/",
                    },
                )
                self.member["params"].append(
                    {
                        "key": str(name),
                        "direction": "in",
                        "value": str(name)
                        + "/"
                        + str(default_value)
                        + "/"
                        + str(value_type)
                        + "/ApplicationArgument/readwrite//False/False/",
                    }
                )

        if ggchild.tag == "definition":
            self.return_type = ggchild.text.strip().split(" ")[0]
            func_path = ggchild.text.strip().split(" ")[-1]
            # if func_path.find("._") >=0 and ggchild.text.find('casatasks') >= 0:
            # skip function if it begins with a single underscore, but keep __init__ and __call__
            if func_path.find(".") >= 0:
                self.func_path, self.func_name = func_path.rsplit(".", 1)
            logger.debug(
                "func_path '%s' for function '%s'",
                self.func_path,
                self.func_name,
            )

            if self.func_name in ["__init__", "__call__"]:
                pass
                # self.func_name = "OBJ:" + self.func_path.rsplit(".",1)[-1]
                # logger.debug("Using name %s for %s function", self.func_path, self.func_name)
            elif (
                self.func_name.startswith("_")
                or self.fname.startswith("_")
                or self.func_path.find("._") >= 0
            ):
                logger.debug("Skipping %s.%s", self.func_path, self.func_name)
                self.member = None
            # else:
            # self.func_name = f"{self.func_path}.{self.func_name}"
            if self.member:
                self.return_type = (
                    "None" if self.return_type == "def" else self.return_type
                )
                self.member["params"].append(
                    {
                        "key": "func_name",
                        "direction": None,
                        "value": "Function Name/"
                        + f"{self.func_path}.{self.func_name}"
                        + "/String/ApplicationArgument/readonly//False/True/Python function name",
                    }
                )
                self.member["params"].append(
                    {
                        "key": "input_parser",
                        "direction": None,
                        "value": "Input Parser/pickle/Select/ApplicationArgument/readwrite/pickle,eval,npy,path,dataurl/False/False/Input port parsing technique",
                    }
                )
                self.member["params"].append(
                    {
                        "key": "output_parser",
                        "direction": None,
                        "value": "Output Parser/pickle/Select/ApplicationArgument/readwrite/pickle,eval,npy,path,dataurl/False/False/Output port parsing technique",
                    }
                )


def process_compounddef(compounddef: dict) -> list:
    """
    Interpret a compound definition element.

    :param compounddef: dict, the compounddef dictionary derived from the respective element

    :returns list of dictionaries

    TODO: This should be split up.
    """
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
                logger.warning("No brief description text")
                result.append({"key": "name", "direction": None, "value": ""})
            else:
                result.append(
                    {
                        "key": "name",
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
                {
                    "key": "description",
                    "direction": None,
                    "value": description.strip(),
                }
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
                        logger.warning("No gitrepo text")
                        value = ""
                    else:
                        value = pichild[0][0].text.strip()
                else:
                    if pichild[0].text is None:
                        logger.warning("No key text (key: " + key + ")")
                        value = ""
                    else:
                        value = pichild[0].text.strip()

        result.append({"key": key, "direction": direction, "value": value})
    return result


def _process_child(child: dict, language: str) -> dict:
    """
    Private function to process a child element.

    :param child: dict, the parsed child element from XML

    :returns: dict of grandchild element
    """
    members = []
    member = {"params": []}
    # logger.debug("Initialized child member: %s", member)

    logger.debug(
        "Found child element: %s with tag: %s kind: %s",
        child,
        child.tag,
        child.get("kind"),
    )
    hold_name = "Unknown"
    casa_mode = False
    if child.tag == "compoundname":
        if child.text.find("casatasks:") == 0:
            casa_mode = True
            hold_name = child.text.split("::")[1]
        else:
            casa_mode = False
            hold_name = "Unknown"
        logger.debug(
            "Found compoundname: %s; extracted: %s", child.text, hold_name
        )
    if child.tag == "detaileddescription" and len(child) > 0 and casa_mode:
        # for child in ggchild:
        dStr = child[0][0].text
        descDict, comp_description = parseCasaDocs(dStr)
        member["params"].append(
            {
                "key": "description",
                "direction": None,
                "value": comp_description,
            }
        )

        pkeys = {p["key"]: i for i, p in enumerate(member["params"])}
        for p in descDict.keys():
            if p in pkeys:
                member["params"][pkeys[p]]["value"] += f'"{descDict[p]}"'

    if child.tag == "sectiondef" and child.get("kind") in [
        "func",
        "public-func",
    ]:
        logger.debug("Processing %d grand children", len(child))
        for grandchild in child:
            gmember = _process_grandchild(grandchild, hold_name, language)
            if gmember is None:
                logger.debug("Bailing out of grandchild processing!")
                continue
            elif gmember != member:
                # logger.debug("Adding grandchild members: %s", gmember)
                member["params"].extend(gmember["params"])
                members.append(gmember)
        logger.debug("Finished processing grand children")
    return members


def _process_grandchild(gchild: dict, hold_name: str, language: str) -> dict:
    """
    Private function to process a grandchild element
    Starts the construction of the member data structure

    :param gchild: dict, the parsed grandchild element from XML
    :param hold_name: str, the initial name of a function
    :param language: int, the languange indicator flag, 0 unknown, 1: Python, 2: C

    :returns: dict, the member data structure
    """
    member = {"params": []}
    # logger.debug("Initialized grandchild member: %s", member)

    if gchild.tag == "memberdef" and gchild.get("kind") == "function":
        func_path = "Unknown"
        func_name = hold_name
        return_type = "Unknown"

        # some defaults
        # param string format is (idText name/value/value_type/param_type/access_restriction/options/precious/positional/description)
        if language == Language.C:
            member["params"].append(
                {"key": "category", "direction": None, "value": "DynlibApp"}
            )
            member["params"].append(
                {
                    "key": "libpath",
                    "direction": None,
                    "value": "Library Path//String/ComponentParameter/readwrite//False/False/The location of the shared object/DLL that implements this application",
                }
            )
        elif language == Language.PYTHON:
            member["params"].append(
                {"key": "category", "direction": None, "value": "PythonApp"}
            )
            member["params"].append(
                {
                    "key": "appclass",
                    "direction": None,
                    "value": "Application Class/dlg.apps.pyfunc.PyFuncApp/String/ComponentParameter/readwrite//False/False/The python class that implements this application",
                }
            )

        member["params"].append(
            {
                "key": "execution_time",
                "direction": None,
                "value": "Execution Time/5/Integer/ComponentParameter/readwrite//False/False/Estimate of execution time (in seconds) for this application.",
            }
        )
        member["params"].append(
            {
                "key": "num_cpus",
                "direction": None,
                "value": "No. of CPUs/1/Integer/ComponentParameter/readwrite//False/False/Number of CPUs used for this application.",
            }
        )
        member["params"].append(
            {
                "key": "group_start",
                "direction": None,
                "value": "Group start/false/Boolean/ComponentParameter/readwrite//False/False/Is this node the start of a group?",
            }
        )

        logger.debug("Processing %d great grand children", len(gchild))
        gg = greatgrandchild()
        for ggchild in gchild:
            gg.process_greatgrandchild(ggchild)
            if gg.member is None:
                logger.debug("Bailing out ggchild processing: %s", gg.member)
                del gg
                return None
        if gg.member != member and gg.member["params"] not in [None, []]:
            member["params"].extend(gg.member["params"])
            logger.debug("member after adding gg_members: %s", member)
        logger.debug("Finished processing great grand children")
        del gg

    return member


def process_compounddef_default(compounddef, language):
    """
    Process the all the sub-elements in a compund definition

    :param compunddef: list of children of compounddef
    :param language: int
    """
    result = []

    # check memberdefs
    for child in compounddef:
        logger.debug("Handling child: %s", child)
        cmember = _process_child(child, language)
        if cmember not in [None, []]:
            result.append(cmember)
        else:
            continue
    return result


# find the named aparam in params, and update the description
def set_param_description(
    name: str, description: str, p_type: str, params: dict
):
    """
    Set the description field of a of parameter <name> from parameters.
    TODO: This should really be part of a class

    :param name: str, the parameter to set the description
    :param description: str, the description to add to the existing string
    :param p_type: str, the type of the parameter if known
    :param params: dict, the set of parameters
    """
    # print("set_param_description():" + str(name) + ":" + str(description))
    p_type = "" if not p_type else p_type
    for p in params:
        if p["key"] == name:
            p["value"] = p["value"] + description
            # insert the type
            pp = p["value"].split("/", 3)
            p["value"] = "/".join(pp[:2] + [p_type] + pp[3:])
            p["type"] = p_type
            break


def create_construct_node(node_type: str, node: dict) -> dict:
    """
    Create the special node for constructs.

    :param node_type: str, the type of the construct node to be created
    :param node: dict, node description (TODO: should be a node object)

    :returns dict of the construct node
    """

    # check that type is in the list of known types
    if node_type not in KNOWN_CONSTRUCT_TYPES:
        logger.warning(
            " construct for node'"
            + node["name"]
            + "' has unknown type: "
            + node_type
        )
        pass

    construct_node = {
        "category": node_type,
        "description": "A default "
        + node_type
        + " construct for the "
        + node["name"]
        + " component.",
        "fields": [],
        "applicationArgs": [],
        "repositoryUrl": gitrepo,
        "commitHash": version,
        "paletteDownloadUrl": "",
        "dataHash": "",
        "key": get_next_key(),
        "name": node_type + "/" + node["name"],
    }

    if node_type == "Scatter" or node_type == "Gather":
        construct_node["fields"] = node["constructFields"]
        construct_node["inputAppFields"] = node["fields"]
        construct_node["inputAppArgs"] = node["applicationArgs"]
        construct_node["inputApplicationKey"] = node["key"]
        construct_node["inputApplicationName"] = node["name"]
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


def params_to_nodes(params: dict) -> list:
    """
    Generate a list of nodes from the params found

    :param params: dict, the parameters to be converted

    :returns list of node dictionaries
    """
    # logger.debug("params_to_nodes: %s", params)
    result = []

    # if no params were found, or only the name and description were found, then don't bother creating a node
    if len(params) > 2:
        # create a node
        data, node = create_palette_node_from_params(params)

        # if the node tag matches the command line tag, or no tag was specified on the command line, add the node to the list to output
        if data["tag"] == tag or tag == "":
            logger.info("Adding component: " + node["name"])
            result.append(node)

            # if a construct is found, add to nodes
            if data["construct"] != "":
                logger.info(
                    "Adding construct component: "
                    + data["construct"]
                    + "/"
                    + node["name"]
                )
                construct_node = create_construct_node(data["construct"], node)
                result.append(construct_node)
            # have to get rid of them for the non-construct nodes
            if "constructFields" in node:
                del node["constructFields"]

    # check if gitrepo and version params were found and cache the values
    for param in params:
        key = param["key"]
        value = param["value"]

        if key == "gitrepo":
            gitrepo = value
        elif key == "version":
            version = value

    return result


def cleanString(input_text: str) -> str:
    """
    Remove ANSI escape strings from input"

    :param input_text: string to clean

    :returns: str, cleaned string
    """
    # ansi_escape = re.compile(r'[@-Z\\-_]|\[[0-?]*[ -/]*[@-~]')
    ansi_escape = re.compile(r"\[[0-?]*[ -/]*[@-~]")
    return ansi_escape.sub("", input_text)


def parseCasaDocs(dStr: str) -> dict:
    """
    Parse the special docstring for casatasks
    Extract the parameters from the casatask doc string.

    :param task: The casatask to derive the parameters from.

    :returns: Dictionary of form {<paramKey>:<paramDoc>}
    """
    dStr = cleanString(dStr)
    dList = dStr.split("\n")
    try:
        start_ind = [
            idx for idx, s in enumerate(dList) if "-- parameter" in s
        ][0] + 1
        end_ind = [idx for idx, s in enumerate(dList) if "-- example" in s][0]
    except IndexError:
        logger.debug("Problems finding start or end index for task: {task}")
        return {}, ""
    paramsList = dList[start_ind:end_ind]
    paramsSidx = [
        idx + 1
        for idx, p in enumerate(paramsList)
        if len(p) > 0 and p[0] != " "
    ]
    paramsEidx = paramsSidx[1:] + [len(paramsList) - 1]
    paramFirstLine = [
        (p.strip().split(" ", 1)[0], p.strip().split(" ", 1)[1].strip())
        for p in paramsList
        if len(p) > 0 and p[0] != " "
    ]
    paramNames = [p[0] for p in paramFirstLine]
    paramDocs = [p[1].strip() for p in paramFirstLine]
    for i in range(len(paramDocs)):
        if paramsSidx[i] < paramsEidx[i]:
            pl = [
                p.strip()
                for p in paramsList[paramsSidx[i] : paramsEidx[i] - 1]
                if len(p.strip()) > 0
            ]
            paramDocs[i] = paramDocs[i] + " " + " ".join(pl)
    params = dict(zip(paramNames, paramDocs))
    comp_description = "\n".join(
        dList[: start_ind - 1]
    )  # return main description as well
    return params, comp_description


if __name__ == "__main__":
    """
    Main method

    TODO: Should be split up
    """
    logger = logging.getLogger(__name__)
    FORMAT = "%(asctime)s [  %(filename)s  ] [  %(lineno)s  ] [  %(funcName)s  ] || %(message)s ||"
    logging.basicConfig(
        format=FORMAT,
        datefmt="%d-%b-%yT%H:%M:%S",
        level=logging.INFO,
    )

    # read environment variables
    if not check_environment_variables():
        sys.exit(1)
    (
        inputdir,
        tag,
        outputfile,
        allow_missing_eagle_start,
        module_path,
        language,
    ) = get_args()

    logger.info("PROJECT_NAME:" + os.environ.get("PROJECT_NAME"))
    logger.info("PROJECT_VERSION:" + os.environ.get("PROJECT_VERSION"))
    logger.info("GIT_REPO:" + os.environ.get("GIT_REPO"))

    logger.info("Input Directory:" + inputdir)
    logger.info("Tag:" + tag)
    logger.info("Output File:" + outputfile)
    logger.info("Allow missing EAGLE_START:" + str(allow_missing_eagle_start))
    logger.info("Module Path:" + module_path)

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
    logger.info(
        "Wrote doxygen configuration file (Doxyfile) to " + doxygen_filename
    )

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
    os.system("cp " + output_xml_filename + " output.xml")
    logger.info("Wrote doxygen XML to output.xml")

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

        else:  # not eagle node
            logger.debug("Handling compound: %s", compounddef)
            functions = process_compounddef_default(compounddef, language)
            functions = functions[0] if len(functions) > 0 else functions
            logger.debug("Number of functions in compound: %d", len(functions))
            for f in functions:
                f_name = [
                    k["value"] for k in f["params"] if k["key"] == "name"
                ]
                logger.debug("Function names: %s", f_name)
                if f_name == [".Unknown"]:
                    continue

                ns = params_to_nodes(f["params"])

                for n in ns:
                    alreadyPresent = False
                    for node in nodes:
                        if node["name"] == n["name"]:
                            alreadyPresent = True

                    # print("component " + n["name"] + " alreadyPresent " + str(alreadyPresent))

                    if not alreadyPresent:
                        nodes.append(n)

    # add signature for whole palette using BlockDAG
    vertices = {}
    for i in range(len(nodes)):
        vertices[i] = nodes[i]
    block_dag = build_block_dag(vertices, [], data_fields=BLOCKDAG_DATA_FIELDS)

    # write the output json file
    write_palette_json(outputfile, nodes, gitrepo, version, block_dag)
    logger.info("Wrote " + str(len(nodes)) + " component(s)")

    # cleanup the output directory
    output_directory.cleanup()
