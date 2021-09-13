#!/usr/bin/python

import sys
import getopt
import xml.etree.ElementTree as ET
import json
import uuid
import csv

next_key = -1

def get_filenames_from_command_line(argv):
    inputfile = ''
    outputfile = ''
    try:
        opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
    except getopt.GetoptError:
        print("xml2palette.py -i <inputfile> -o <outputfile>")
        sys.exit(2)

    if len(opts) < 2:
        print("xml2palette.py -i <inputfile> -o <outputfile>")
        sys.exit()

    for opt, arg in opts:
        if opt == '-h':
            print("xml2palette.py -i <inputfile> -o <outputfile>")
            sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
        elif opt in ("-o", "--ofile"):
            outputfile = arg
    return inputfile, outputfile


def get_next_key():
    global next_key

    next_key -= 1

    return next_key + 1


def create_port(name):
    return {
        "Id": str(uuid.uuid4()),
        "IdText": name
    }


def find_field_by_name(fields, name):
    for field in fields:
        if field['name'] == name:
            return field
    return None


def add_required_fields_for_category(fields, category):
    if category == "DynlibApp":
        if find_field_by_name(fields, "execution_time") is None:
            fields.append(create_field("execution_time", "Execution time", 5, "Estimated execution time", "readwrite", "Float"))
        if find_field_by_name(fields, "num_cpus") is None:
            fields.append(create_field("num_cpus", "Num CPUs", 1, "Number of cores used", "readwrite", "Integer"))
        if find_field_by_name(fields, "group_start") is None:
            fields.append(create_field("group_start", "Group start", 0, "Component is start of a group", "readwrite", "Boolean"))
        if find_field_by_name(fields, "libpath") is None:
            fields.append(create_field("libpath", "Library path", "", "", "readwrite", "String"))
    elif category == "PythonApp":
        if find_field_by_name(fields, "execution_time") is None:
            fields.append(create_field("execution_time", "Execution time", 5, "Estimated execution time", "readwrite", "Float"))
        if find_field_by_name(fields, "num_cpus") is None:
            fields.append(create_field("num_cpus", "Num CPUs", 1, "Number of cores used", "readwrite", "Integer"))
        if find_field_by_name(fields, "group_start") is None:
            fields.append(create_field("group_start", "Group start", 0, "Component is start of a group", "readwrite", "Boolean"))
        if find_field_by_name(fields, "appclass") is None:
            fields.append(create_field("appclass", "Appclass", "dlg.apps.simple.SleepApp", "Application class", "readwrite", "String"))


def create_field(internal_name, name, value, description, access, type):
    return {
        "text": name,
        "name": internal_name,
        "value": value,
        "description": description,
        "readonly": access == "readonly",
        "type": type
    }


def parse_param_key(key):
    # parse the key as csv (delimited by '/')
    parts = []
    reader = csv.reader([key], delimiter='/', quotechar='"')
    for row in reader:
        parts = row

    # init attributes of the param
    param = ""
    internal_name = ""
    name = ""
    default_value = ""
    type = "String"
    access = "readwrite"

    # assign attributes (if present)
    if len(parts) > 0:
        param = parts[0]
    if len(parts) > 1:
        internal_name = parts[1]
    if len(parts) > 2:
        name = parts[2]
    if len(parts) > 3:
        default_value = parts[3]
    if len(parts) > 4:
        type = parts[4]
    if len(parts) > 5:
        access = parts[5]
    else:
        print("param (" + name + ") has no 'access' descriptor, using default (readwrite) : " + key)

    return (param, internal_name, name, default_value, type, access)


# NOTE: color, x, y, width, height are not specified in palette node, they will be set by the EAGLE importer
def create_palette_node_from_params(params):
    text = ""
    description = ""
    category = ""
    categoryType = ""
    inputPorts = []
    outputPorts = []
    inputLocalPorts = []
    outputLocalPorts = []
    fields = []

    # process the params
    for param in params:
        key = param['key']
        direction = param['direction']
        value = param['value']

        if key == "category":
            category = value
        elif key == "text":
            text = value
        elif key == "description":
            description = value
        elif key.startswith("param/"):
            # parse the param key into name, type etc
            (param, internal_name, name, default_value, type, access) = parse_param_key(key)

            # check that access is a known value
            if access != "readonly" and access != "readwrite":
                print("ERROR: Unknown access: " + access)

            # add a field
            fields.append(create_field(internal_name, name, default_value, value, access, type))
            pass
        elif key.startswith("port/") or key.startswith("local-port/"):
            # parse the port into data
            if key.count("/") == 1:
                (port, name) = key.split("/")
            elif key.count("/") == 2:
                (port, name, type) = key.split("/")
            else:
                print("ERROR: port expects format `param[Direction] port/Name/Data Type`: got", key)

            # add a port
            if port == "port":
                if direction == "in":
                    inputPorts.append(create_port(name))
                elif direction == "out":
                    outputPorts.append(create_port(name))
                else:
                    print("ERROR: Unknown port direction: " + direction)
            else:
                if direction == "in":
                    inputLocalPorts.append(create_port(name))
                elif direction == "out":
                    outputLocalPorts.append(create_port(name))
                else:
                    print("ERROR: Unknown local-port direction: " + direction)

    # add extra fields that must be included for the category
    add_required_fields_for_category(fields, category)

    # create and return the node
    return {
        "category": category,
        "categoryType": "Application",
        "isData": False,
        "isGroup": False,
        "canHaveInputs": True,
        "canHaveOutputs": True,
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
        "exitApplicationName": "",
        "inputApplicationType": "None",
        "outputApplicationType": "None",
        "exitApplicationType": "None",
        "inputPorts": inputPorts,
        "outputPorts": outputPorts,
        "inputLocalPorts": inputLocalPorts,
        "outputLocalPorts": outputLocalPorts,
        "inputAppFields": [],
        "outputAppFields": [],
        "fields": fields
    }


def write_palette_json(outputfile, nodes, gitrepo, version):
    palette = {
        "modelData": {
            "fileType": "palette",
            "repoService": "GitHub",
            "repoBranch": "master",
            "repo": "ICRAR/EAGLE_test_repo",
            "readonly": True,
            "filePath": outputfile,
            "sha": version,
            "git_url": gitrepo
        },
        "nodeDataArray": nodes,
        "linkDataArray": []
    }

    with open(outputfile, 'w') as outfile:
        json.dump(palette, outfile, indent=4)


def process_compounddef(compounddef):
    result = []

    # get child of compounddef called "briefdescription"
    briefdescription = None
    for child in compounddef:
        if child.tag == "briefdescription":
            briefdescription = child
            break

    if briefdescription is not None:
        if len(briefdescription) > 0:
            if briefdescription[0].text is None:
                print("No brief description text")
                result.append({
                    "key":"text",
                    "direction":None,
                    "value":""
                })
            else:
                result.append({
                    "key":"text",
                    "direction":None,
                    "value":briefdescription[0].text.strip(" .")
                })

    # get child of compounddef called "detaileddescription"
    detaileddescription = None
    for child in compounddef:
        if child.tag == "detaileddescription":
            detaileddescription = child
            break

    # check that detailed description was found
    if detaileddescription is None:
        return result

    # search children of detaileddescription node for a para node with "simplesect" children, who have "title" children with text "EAGLE_START" and "EAGLE_END"
    para = None
    description = ""
    for ddchild in detaileddescription:
        if ddchild.tag == "para":
            if ddchild.text is not None:
                description += ddchild.text + "\n"
            for pchild in ddchild:
                if pchild.tag == "simplesect":
                    para = ddchild

    # add description
    if description != "":
        result.append({"key":"description", "direction":None, "value":description.strip()})

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
                if key == "gitrepo":
                    if pichild[0].text is None:
                        print("No gitrepo text")
                        value = ""
                    else:
                        value = pichild[0].text.strip()
                else:
                    if pichild[0].text is None:
                        print("No key text (key: " + key + ")")
                        value = ""
                    else:
                        value = pichild[0].text.strip()

        result.append({"key":key,"direction":direction,"value":value})

    return result


if __name__ == "__main__":
    (inputfile, outputfile) = get_filenames_from_command_line(sys.argv[1:])

    gitrepo = ""
    version = ""

    # init nodes array
    nodes = []

    # load the input xml file
    tree = ET.parse(inputfile)
    xml_root = tree.getroot()

    for compounddef in xml_root:
        params = process_compounddef(compounddef)

        # if no params were found, or only the name and description were found, then don't bother creating a node
        if len(params) > 2:
            # print("params: " + str(params))

            # create a node
            n = create_palette_node_from_params(params)
            nodes.append(n)

        # check if gitrepo and version params were found and cache the values
        for param in params:
            key = param['key']
            value = param['value']

            if key == "gitrepo":
                gitrepo = value
            elif key == "version":
                version = value

    # write the output json file
    write_palette_json(outputfile, nodes, gitrepo, version)
