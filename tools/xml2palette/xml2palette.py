#!/usr/bin/env python
"""
Script processes a file or a directory of source files and
produces a DALiuGE compatible palette file containing the
information required to use functions and components in graphs.
For more information please refer to the documentation
https://daliuge.readthedocs.io/en/latest/development/app_development/eagle_app_integration.html#automatic-eagle-palette-generation

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

VALUE_TYPES = {
    str: "String",
    int: "Integer",
    float: "Float",
    bool: "Boolean",
    list: "Json",
    dict: "Json",
    tuple: "Json"
}

class Language(Enum):
    UNKNOWN = 0
    C = 1
    PYTHON = 2

def get_args():
    """
    """
    # inputdir, tag, outputfile, allow_missing_eagle_start, module_path, language
    parser = argparse.ArgumentParser(epilog=__doc__,
                            formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("idir", help="input directory path or file name")
    parser.add_argument("ofile", help="output file name")
    parser.add_argument("-m", "--module", help="Module load path name",
                        default="")
    parser.add_argument("-t", "--tag", help="filter components with matching tag", default="")
    parser.add_argument("-c", help="C mode, if not set Python will be used",
                        action="store_true")
    parser.add_argument("-r", "--recursive", help="Traverse sub-directories",
                        action="store_true")
    parser.add_argument("-s", "--parse_all", 
        help="Try to parse non DAliuGE compliant functions and methods",
        action="store_true")
    parser.add_argument("-v", "--verbose", help="increase output verbosity",
                        action="store_true")
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
    return args.idir, args.tag, args.ofile, args.parse_all, args.module, language

def check_environment_variables() -> bool:
    """
    Check environment variables and set them if required.
    """
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
                logger.error("No " + variable + " environment variable.")
                return False

    return True


def modify_doxygen_options(doxygen_filename:str, options:dict):
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
    component_name, internal_name, external_name, direction, event, value_type, description
):
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
        "text": external_name,
        "event": event,
        "type": value_type,
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
    internal_name, external_name, value, value_type, field_type, access, options, precious, positional, description
):
    return {
        "text": external_name,
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


def alert_if_missing(text, fields, internal_name):
    if find_field_by_name(fields, internal_name) is None:
        logger.warning(text + " component missing " + internal_name + " cparam")
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
        logger.warning(
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
        logger.warning(
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
        logger.warning(
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

    return (external_name, default_value, value_type, field_type, access, options, precious, positional, description)


def parse_description(value):
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
def create_palette_node_from_params(params) -> dict:
    """
    Construct the palette from the parameter structure

    TODO: Should split this up into individual parts
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
    applicationArgs = []
    gitrepo = os.environ.get("GIT_REPO")
    version = os.environ.get("PROJECT_VERSION")

    # process the params
    for param in params:
        if not isinstance(param, dict):
            logger.error("param %s has wrong type %s. Ignoring!", param, type(param))
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
        elif key == "text":
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
                description
            ) = parse_value(text, value)

            # check that type is in the list of known types
            if value_type not in KNOWN_PARAM_DATA_TYPES:
                #logger.warning(text + " " + field_type + " '" + name + "' has unknown type: " + type)
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
                description
            )

            # add the field to the correct list in the component, based on fieldType
            if field_type in KNOWN_FIELD_TYPES:
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
    check_required_fields_for_category(text, fields, category)
    # create and return the node
    return (
        {"tag": tag, "construct": construct},
        {
            "category": category,
            "drawOrderHint": 0,
            "key": get_next_key(),
            "text": text,
            "description": comp_description,
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


def write_palette_json(outputfile:str, nodes:list, gitrepo:str, version:str):
    """
    Write palette to file
    """
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
        },
        "nodeDataArray": nodes,
        "linkDataArray": [],
    }

    with open(outputfile, "w") as outfile:
        json.dump(palette, outfile, indent=4)


def process_compounddef(compounddef:dict) -> list:
    """
    Interpret the a compound definition.

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

def _process_child(child:dict, language:str) -> dict:
    """
    Private function to process a child element.
    """
    members = []
    member = {"params":[]}
    # logger.debug("Initialized child member: %s", member)

    logger.debug("Found child element: %s with tag: %s kind: %s", child, child.tag, child.get("kind"))
    hold_name = "Unknown"
    casa_mode = False
    if child.tag == "compoundname":
        if child.text.find('casatasks:') == 0:
            casa_mode = True
            hold_name = child.text.split('::')[1]  
        else:
            casa_mode = False
            hold_name = "Unknown"
        logger.debug("Found compoundname: %s; extracted: %s", child.text, hold_name)
    if child.tag == "detaileddescription" and len(child) > 0 and casa_mode:
        # for child in ggchild:
        dStr = child[0][0].text
        descDict, comp_description = parseCasaDocs(dStr)
        member["params"].append({"key": "description", "direction": None, "value": comp_description})

        pkeys = {p["key"]:i for i,p in enumerate(member["params"])}
        for p in descDict.keys():
            if p in pkeys:
                member["params"][pkeys[p]]["value"] += f'"{descDict[p]}"'

    if child.tag == "sectiondef" and child.get("kind") in ["func", "public-func"]:
        for grandchild in child:
            gmember = _process_grandchild(grandchild, member, hold_name, language)
            if not gmember:
                logger.debug("No gmembers found: %s", gmember)
                return None
            elif gmember != member:
                # logger.debug("Adding grandchild members: %s", gmember)
                member["params"].extend(gmember["params"])
                members.append(gmember)
    return members

def _process_grandchild(gchild, omember, hold_name, language):
    """
    """
    member = {"params": []}
    # logger.debug("Initialized grandchild member: %s", member)

    if gchild.tag == "memberdef" and gchild.get("kind") == "function":
        logger.debug(">>> Found function element")

        func_path = "Unknown"
        func_name = hold_name
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

        for ggchild in gchild:
            gg_member = _process_greatgrandchild(ggchild, member)
            if not gg_member:
                # logger.debug("No ggchild members found: %s", gg_member)
                return None
            elif gg_member != member and gg_member["params"] not in [None, []]:
                logger.debug("Adding ggchild members: %s", gg_member)
                member["params"].extend(gg_member["params"])

        # skip function if it begins with a single underscore, but keep __init__ and __call__
        if func_path.find('._') >=0 or\
            (func_name.startswith('_') and func_name not in ["__init__", "__call__"]):
            logger.debug("Skipping %s.%s starts with underscore", func_path, func_name)
            return None

        # skip component if a module_path was specified, and this component is not within it
        if module_path != "" and not module_path in func_path:
            logger.debug("Skip " + func_path + ". Doesn't match module path: " + module_path)
            return None
    logger.debug("Returning grandchild members: %s", member)
    return member

def _process_greatgrandchild(ggchild, omember):
    member = omember
    member = {"params": []}
    logger.debug("Initialized ggchild member: %s", member)
    func_name = "Unknown"
    logger.debug("greatgrandchild element: %s", ggchild.tag)
    if ggchild.tag == "name":
        func_name = ggchild.text if func_name == "Unknown" else func_name
        member["params"].append({"key": "text", "direction": None, "value": func_name})
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
                logger.debug("Add output port:" + str(output_port_name) + "/" + str(return_type) + "/" + str(return_part))
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
        value_type = ""
        name = ""
        default_value = ""

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
        if str(name) == "self": return None
        # type recognised?
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
        if value_type in ["string", "*" , "**"]:
            value_type = "String"
            type_recognised = True

        # try to guess the type based on the default value
        # TODO: try to parse default_value as JSON to detect JSON types
        if not type_recognised and default_value != "" and default_value is not None and default_value != "None":
            #print("name:" + str(name) + " default_value:" + str(default_value))
            try:
                # we'll try to interpret what the type of the default_value is using ast
                l = {}
                try:
                    eval(compile(ast.parse(f't = {default_value}'),filename="",mode="exec"), l)
                    vt = type(l['t'])
                    if not isinstance(l['t'], type):
                        default_value = l['t']
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
                        #print("Use Integer")
                    except TypeError:
                        if isinstance(default_value, types.BuiltinFunctionType):
                            value_type = "String"
                    except:
                        try:
                            val = float(default_value)
                            value_type = "Float"
                            #print("Use Float")
                        except:
                            if default_value.lower() == "true" or default_value.lower() == "false":
                                value_type = "Boolean"
                                default_value = default_value.lower()
                                #print("Use Boolean")
                            else:
                                value_type = "String"
                                #print("Use String")
            except NameError or TypeError:
                raise

        # add the param                                           
        if str(value_type) == "String":
            default_value = str(default_value).replace("'", "")
            if default_value.find("/") >=0:
                default_value = f'"{default_value}"'
        logger.debug("adding param: %s",{"key":str(name), "direction":"in", "value":str(name) + "/" + str(default_value) + "/" + str(value_type) + "/ApplicationArgument/readwrite//False/False/"})
        member["params"].append({"key":str(name), "direction":"in", "value":str(name) + "/" + str(default_value) + "/" + str(value_type) + "/ApplicationArgument/readwrite//False/False/"})

    if ggchild.tag == "definition":
        return_type = ggchild.text.strip().split(" ")[0]
        func_path = ggchild.text.strip().split(" ")[-1]
        # if func_path.find("._") >=0 and ggchild.text.find('casatasks') >= 0:
        if func_path.find(".") >=0:
            func_path = func_path.split('.')
            func_path, func_name = '.'.join(func_path[:-1]), func_path[-1]
        if func_path.find('._') >= 0:
            logger.debug("Skipping " + func_path + " contains underscore")
            return None
        logger.debug("Using name '%s' for function %s", func_path, func_name)

        # aparams
        if func_name in ["__init__", "__call__"]:
            logger.debug("Found %s for %s", func_name, func_path)
            func_name = func_path
        else:
            func_name = f"{func_path}.{func_name}"
        member["params"].append({"key": "func_name", "direction": None, "value": "Function Name/" + func_name + "/String/ApplicationArgument/readonly//False/True/Python function name"})
        member["params"].append({"key": "input_parser", "direction": None, "value": "Input Parser/pickle/Select/ApplicationArgument/readwrite/pickle,eval,npy,path,dataurl/False/False/Input port parsing technique"})
        member["params"].append({"key": "output_parser", "direction": None, "value": "Output Parser/pickle/Select/ApplicationArgument/readwrite/pickle,eval,npy,path,dataurl/False/False/Output port parsing technique"})

        return_type = "None" if return_type == "def" else return_type
    logger.debug("Returning gg_members %s", member)
    return member

def process_compounddef_default(compounddef, language):
    result = []

    # check memberdefs
    for child in compounddef:
        logger.debug("Handling child: %s",child)
        cmember = _process_child(child, language)
        logger.debug("Member content: %s", cmember)
        if cmember not in [None, []] and cmember["params"] != []:
            result.append(cmember)
        else:
            continue
    logger.debug("Result for child: %s",result)
    return result


# find the list of param names and descriptions in the <detaileddescription> tag
def parse_params(detailed_description):
    result = []

    detailed_description = detailed_description.split("Returns:")[0]
    param_lines = [p.replace('\n','').strip() for p in detailed_description.split(":param")[1:]]
    # param_lines = [line.strip() for line in detailed_description]

    for p_line in param_lines:
        logger.debug("p_line: %s" + p_line)

        try:
            index_of_second_colon = p_line.index(':', 0)
        except:
            # didnt find second colon, skip
            continue

        param_name = p_line[1:index_of_second_colon].strip()
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


def create_construct_node(node_type, node):

    # check that type is in the list of known types
    if type not in KNOWN_CONSTRUCT_TYPES:
        logger.warning(" construct for node'" + node["text"] + "' has unknown type: " + node_type)
        pass

    construct_node = {
        "category": node_type,
        "description": "A default "
        + node_type
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
        "text": node_type + "/" + node["text"],
    }

    if node_type == "Scatter" or node_type == "Gather":
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
    # logger.debug("params_to_nodes: %s", params)
    result = []

    # if no params were found, or only the name and description were found, then don't bother creating a node
    if len(params) > 2:
        # create a node
        data, node = create_palette_node_from_params(params)

        # if the node tag matches the command line tag, or no tag was specified on the command line, add the node to the list to output
        if data["tag"] == tag or tag == "":
            logger.info("Adding component: " + node["text"])
            result.append(node)

            # if a construct is found, add to nodes
            if data["construct"] != "":
                logger.info("Adding component: " + data["construct"] + "/" + node["text"])
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

def cleanString(text: str) -> str:
    """
    Remove ANSI escape strings from text"

    :param text: string to clean
    """
    # ansi_escape = re.compile(r'[@-Z\\-_]|\[[0-?]*[ -/]*[@-~]')
    ansi_escape = re.compile(r'\[[0-?]*[ -/]*[@-~]')
    return ansi_escape.sub('', text)


def parseCasaDocs(dStr:str) -> dict:
    """
    Parse the special docstring for casatasks
    Extract the parameters from the casatask doc string.

    :param task: The casatask to derive the parameters from.

    :returns: Dictionary of form {<paramKey>:<paramDoc>}
    """
    dStr = cleanString(dStr)
    dList = dStr.split('\n')
    try:
        start_ind = [idx for idx, s in enumerate(dList) if '-- parameter' in s][0] + 1
        end_ind = [idx for idx, s in enumerate(dList) if '-- example' in s][0]
    except IndexError:
        logger.debug('Problems finding start or end index for task: {task}')
        return {}, ""
    paramsList = dList[start_ind:end_ind]
    paramsSidx = [idx+1 for idx, p in enumerate(paramsList) if len(p) > 0 and p[0] != ' ']
    paramsEidx = paramsSidx[1:] + [len(paramsList) - 1]
    paramFirstLine = [(p.strip().split(' ',1)[0], p.strip().split(' ',1)[1].strip()) 
        for p in paramsList if len(p) > 0 and p[0] != ' ']
    paramNames = [p[0] for p in paramFirstLine]
    paramDocs  = [p[1].strip() for p in paramFirstLine]
    for i in range(len(paramDocs)):
        if paramsSidx[i] < paramsEidx[i]:
            pl = [p.strip() for p in paramsList[paramsSidx[i]:paramsEidx[i]-1] if len(p.strip()) > 0]
            paramDocs[i] = paramDocs[i] + ' '+' '.join(pl)
    params = dict(zip(paramNames, paramDocs))
    comp_description = "\n".join(dList[:start_ind-1]) # return main description as well
    return params, comp_description


if __name__ == "__main__":
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
    (inputdir, tag, outputfile, allow_missing_eagle_start, module_path, language) =\
        get_args()

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
    logger.info("Wrote doxygen configuration file (Doxyfile) to " + doxygen_filename)

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

        else: # not eagle node
            logger.debug("Handling compound: %s",compounddef)
            functions = process_compounddef_default(compounddef, language)
            for f in functions:
                logger.debug("Number of functions in compound: %d", len(f))
                f_name = [k["value"] for k in f["params"] if k["key"] == "text"]
                logger.debug("Function names: %s", f_name)
                ns = params_to_nodes(f["params"])

                for n in ns:
                    alreadyPresent = False
                    for node in nodes:
                        if node["text"] == n["text"]:
                            alreadyPresent = True

                    #print("component " + n["text"] + " alreadyPresent " + str(alreadyPresent))

                    if not alreadyPresent:
                        nodes.append(n)


    # write the output json file
    write_palette_json(outputfile, nodes, gitrepo, version)
    logger.info("Wrote " + str(len(nodes)) + " component(s)")

    # cleanup the output directory
    output_directory.cleanup()
