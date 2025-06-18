#
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

"""
Dropmake utils
"""

import copy
import json
import logging
import os
import os.path as osp
import uuid

from .definition_classes import ConstructTypes

logger = logging.getLogger(f"dlg.{__name__}")

LG_VER_OLD = 1
LG_VER_EAGLE_CONVERTED = 2
LG_VER_EAGLE = 3
LG_APPREF = "AppRef"

TEMP_FILE_FOLDER = "/tmp"


class GraphException(Exception):
    pass


class GInvalidLink(GraphException):
    pass


class GInvalidNode(GraphException):
    pass


class Port(dict):
    """
    Class to represent a port of a node
    """

    def __init__(self, port_type, port_name, port_id):
        self.port_type = port_type
        self.port_name = port_name
        self.port_id = port_id
        self.target_id = ""

    def __str__(self):
        return f'{{"{self.port_name}":{{"id":"{self.port_id}", "type":"{self.port_type}", "target_id": "{self.target_id}"}}}}'

    def __repr__(self):
        return f'{{"{self.port_name}":{{"id":"{self.port_id}", "type":"{self.port_type}", "target_id": "{self.target_id}"}}}}'

def get_lg_ver_type(lgo):
    """
    Get the version type of this logical graph
    """
    # with open(lg_name, 'r+') as file_object:
    #     lgo = json.load(file_object)

    nodes = lgo["nodeDataArray"]
    if len(nodes) == 0:
        raise Exception("Invalid LG, nodes not found")

    # First check whether modelData and schemaVersion is in graph
    if (
        "modelData" in lgo
        and len(lgo["modelData"]) > 0
        and "schemaVersion" in lgo["modelData"]
    ):
        if lgo["modelData"]["schemaVersion"] != "OJS":
            return lgo["modelData"]["schemaVersion"]

    # else do the old stuff...
    for i, node in enumerate(nodes):
        if i > 5:
            break
        if "fields" in node:
            fds = node["fields"]
            for fd in fds:
                if "name" in fd:
                    kw = fd["name"]
                    if kw in node and kw not in ["description"]:
                        return LG_VER_EAGLE_CONVERTED
                    else:
                        return LG_VER_EAGLE

    if "linkDataArray" in lgo and len(lgo["linkDataArray"]) > 0:
        lnk = lgo["linkDataArray"][0]
        if "fromPort" not in lnk:
            return LG_VER_OLD
        if lnk["fromPort"] in ["B", "T", "R", "L"]:
            # only the old lg links can be Bottom, Top, Right and Left
            return LG_VER_OLD
        else:
            return LG_VER_EAGLE_CONVERTED
    else:
        if "copiesArrayObjects" in lgo or "copiesArrays" in lgo:
            return LG_VER_EAGLE_CONVERTED
        else:
            return LG_VER_OLD


def get_keyset(lgo):
    return set([x["id"] for x in lgo["nodeDataArray"]])


def getNodesKeyDict(lgo):
    """
    Return a dictionary of all nodes with the key attribute value as the key
    and the complete node as the value.
    """
    return dict([(x["id"], x) for x in lgo["nodeDataArray"]])


def convert_fields(lgo:dict) -> dict:
    """Convert fields of all logical graph nodes to node attributes

    Args:
        lgo: The logical graph object

    Returns:
        converted logical graph object
    """
    logger.debug("Converting fields")
    nodes = lgo["nodeDataArray"]
    for node in nodes:
        fields = node["fields"]
        node["inputPorts"] = {}
        node["outputPorts"] = {}
        for field in fields:
            name = field.get("name", "")
            if name != "":
                node[name] = field.get("value", "")
                if node[name] == "":
                    node[name] = field.get("defaultValue", "")
            port = field.get("usage", "")
            if port in ["InputPort", "OutputPort", "InputOutput"]:
                node[name] = f"<port>: {Port(port, name, field['id'])}"
                if port in ["InputPort", "InputOutput"]:
                    node["inputPorts"][field["id"]] = {"type":port, "name": name, "source_id": ""}
                elif port in ["OutputPort", "InputOutput"]:
                    node["outputPorts"][field["id"]] = {"type":port, "name": name, "target_id": ""}
    return lgo


def _build_node_index(lgo):
    ret = dict()
    for node in lgo["nodeDataArray"]:
        ret[node["id"]] = node

    return ret


def _check_MKN(m, k, n):
    """
    Need to work with python 2 as well
    """
    m, k, n = float(m), float(k), float(n)
    ratio1 = m / k if m >= k else k / m
    ratio2 = k / n if k >= n else n / k

    if ratio1.is_integer() and ratio2.is_integer():
        return int(ratio1), int(ratio2)
    else:
        raise GraphException("M-K and k-N must be pairs of multiples")


def _make_unique_port_key(port_key, node_key):
    return "%s+++%s" % (port_key, node_key)


def convert_mkn(lgo):
    """
    convert MKN into scatters and gathers based on "random_thoughts.graph"
    NO hardcoded assumptions (e.g. M > K > N) are needed

    EACH instance of the MKN construct takes M inputs
    """
    keyset = get_keyset(lgo)
    old_new_k2n_to_map = dict()
    old_new_k2n_from_map = dict()
    old_new_parent_map_split_1 = dict()
    old_new_parent_map_split_2 = dict()
    dont_change_group = set()
    n_products_map = dict()
    app_keywords = ["inputApplicationName", "outputApplicationName"]

    for node in lgo["nodeDataArray"]:
        if ConstructTypes.MKN != node["category"]:
            continue
        for ak in app_keywords:
            if ak not in node:
                raise Exception(
                    "MKN construct {0} must specify {1}".format(node["id"], ak)
                )
        mknv_dict = dict()
        for mknv in node["fields"]:
            mknv_dict[mknv["name"]] = int(mknv["value"])
        M, K, N = mknv_dict["m"], mknv_dict["k"], mknv_dict["n"]

        # step 1 - clone the current MKN
        mkn_key = node["id"]
        mkn_local_input_keys = [
            _make_unique_port_key(x["id"], node["id"])
            for x in node["inputAppFields"]
            if x["usage"] == "InputPort"
        ]
        mkn_output_keys = [
            _make_unique_port_key(x["id"], node["id"])
            for x in node["inputAppFields"]
            if x["usage"] == "OutputPort"
        ]
        node_mk = node
        node_mk["mkn"] = [M, K, N]
        node_kn = copy.deepcopy(node_mk)
        node_split_n = copy.deepcopy(node_mk)

        node_mk["application"] = node["inputApplicationName"]
        node_mk["category"] = ConstructTypes.GATHER
        node_mk["categoryType"] = ConstructTypes.GATHER
        ipan = node_mk.get("inputApplicationName", "")
        if len(ipan) == 0:
            node_mk["name"] = node_mk["name"] + "_InApp"
        else:
            node_mk["name"] = ipan
        #        del node_mk["inputApplicationName"]
        #        del node_mk["outputApplicationName"]
        #        del node_mk["outputAppFields"]
        new_field = {
            "name": "num_of_inputs",
            "value": "%d" % (M),
        }
        node_mk["fields"].append(new_field)

        node_kn["category"] = ConstructTypes.SCATTER
        node_kn["categoryType"] = ConstructTypes.SCATTER

        opan = node_kn.get("outputAppName", "")
        if len(opan) == 0:
            node_kn["name"] = node_kn["name"] + "_OutApp"
        else:
            node_kn["name"] = opan
        new_id = str(uuid.uuid4())
        keyset.add(new_id)
        node_kn["id"] = new_id
        node_kn["parentId"] = mkn_key
        dont_change_group.add(new_id)
        old_new_parent_map_split_1[mkn_key] = new_id
        node_kn["application"] = node_kn["outputApplicationName"]
        node_kn["inputAppFields"] = node_kn["outputAppFields"]
        #        del node_kn["inputApplicationName"]
        #        del node_kn["outputApplicationName"]
        #        del node_kn["outputAppFields"]

        new_field_kn = {
            "name": "num_of_copies",
            "value": "%d" % (K),
        }
        node_kn["fields"].append(new_field_kn)
        node_kn["reprodata"] = node.get("reprodata", {}).copy()
        lgo["nodeDataArray"].append(node_kn)

        # for all connections that point to the local input ports of the MKN construct
        # we reconnect them to the "new" scatter
        for mlik in mkn_local_input_keys:
            old_new_k2n_to_map[mlik] = new_id

        # for all connections that go from the outputPorts of the MKN construct
        # we reconnect them from the new scatter
        for mok in mkn_output_keys:
            old_new_k2n_from_map[mok] = new_id

        node_split_n["category"] = ConstructTypes.SCATTER
        node_split_n["categoryType"] = ConstructTypes.SCATTER
        node_split_n["name"] = "Nothing"
        new_id = str(uuid.uuid4())
        keyset.add(new_id)
        node_split_n["id"] = new_id
        node_split_n["parentId"] = mkn_key
        dont_change_group.add(new_id)
        old_new_parent_map_split_2[mkn_key] = new_id

        for mok in mkn_output_keys:
            n_products_map[mok] = new_id

        #        del node_split_n["inputApplicationName"]
        #        del node_split_n["outputApplicationName"]
        #        del node_split_n["outputAppFields"]
        # del node_split_n['intputAppFields']

        new_field_kn = {
            "name": "num_of_copies",
            "value": "%d" % (N),
        }
        node_split_n["fields"].append(new_field_kn)
        node_split_n["reprodata"] = node.get("reprodata", {}).copy()
        lgo["nodeDataArray"].append(node_split_n)

    need_to_change_n_products = dict()
    for link in lgo["linkDataArray"]:
        ufpk = _make_unique_port_key(link["fromPort"], link["from"])
        utpk = _make_unique_port_key(link["toPort"], link["to"])
        if ufpk in old_new_k2n_from_map:
            link["from"] = old_new_k2n_from_map[ufpk]
        elif utpk in old_new_k2n_to_map:
            link["to"] = old_new_k2n_to_map[utpk]

        if ufpk in n_products_map:
            need_to_change_n_products[link["to"]] = n_products_map[ufpk]

    # TODO change the parent for K and N data drops
    for node in lgo["nodeDataArray"]:
        if not "parentId" in node:
            continue
        if node["id"] in dont_change_group:
            continue
        if node["parentId"] in old_new_parent_map_split_1:
            node["parentId"] = old_new_parent_map_split_1[node["parentId"]]
        elif node["parentId"] in old_new_parent_map_split_2:
            node["parentId"] = old_new_parent_map_split_2[node["parentId"]]

    for node in lgo["nodeDataArray"]:
        if node["id"] in need_to_change_n_products:
            node["parentId"] = need_to_change_n_products[node["id"]]

    # with open('/Users/chen/Documents/MKN_translate_003.graph', 'w') as f:
    #     json.dump(lgo, f, indent=4)
    return lgo


def convert_mkn_all_share_m(lgo):
    """
    convert MKN into scatters and gathers based on "testMKN.graph"
    hardcode the assumption M > K > N for now

    ALL instances of the MKN construct take M inputs. Thus, each instance takes M // K inputs.

    NB - This function is NOT called by the pg_generator. It is here for the sake of comparison
    and demonstration.
    """
    keyset = get_keyset(lgo)
    old_new_k2n_to_map = dict()
    old_new_k2n_from_map = dict()
    app_keywords = ["inputApplicationName", "outputApplicationName"]

    for node in lgo["nodeDataArray"]:
        if ConstructTypes.MKN != node["category"]:
            continue
        for ak in app_keywords:
            if ak not in node:
                raise Exception(
                    "MKN construct {0} must specify {1}".format(node["id"], ak)
                )
        mknv_dict = dict()
        for mknv in node["fields"]:
            mknv_dict[mknv["name"]] = int(mknv["value"])
        M, K, N = mknv_dict["m"], mknv_dict["k"], mknv_dict["n"]
        ratio_mk, ratio_kn = _check_MKN(M, K, N)

        # step 1 - clone the current MKN
        # mkn_key = node["id"]
        mkn_local_input_keys = [x["Id"] for x in node["inputLocalPorts"]]
        mkn_output_keys = [x["Id"] for x in node["outputPorts"]]
        node_mk = node
        node_kn = copy.deepcopy(node_mk)

        node_mk["application"] = node["inputApplicationName"]
        node_mk["category"] = ConstructTypes.GATHER
        node_mk["name"] = node_mk["name"] + "_InApp"
        del node["inputApplicationName"]
        del node["outputApplicationName"]
        del node["outputAppFields"]
        new_field = {
            "name": "num_of_inputs",
            "value": "%d" % (ratio_mk),
        }
        node_mk["fields"].append(new_field)

        node_kn["category"] = ConstructTypes.GATHER
        node_kn["name"] = node_kn["name"] + "_OutApp"
        new_id = str(uuid.uuid4())
        keyset.add(new_id)
        node_kn["id"] = new_id
        node_kn["application"] = node_kn["outputApplicationName"]
        node_kn["inputAppFields"] = node_kn["outputAppFields"]
        del node_kn["inputApplicationName"]
        del node_kn["outputApplicationName"]
        del node_kn["outputAppFields"]

        new_field_kn = {
            "name": "Number of inputs",
            "value": "%d" % (ratio_kn),
        }
        node_kn["fields"].append(new_field_kn)
        node_kn["reprodata"] = node.get("reprodata", {}).copy()
        lgo["nodeDataArray"].append(node_kn)

        # for all connections that point to the local input ports of the MKN construct
        # we reconnect them to the "new" gather
        for mlik in mkn_local_input_keys:
            old_new_k2n_to_map[mlik] = new_id

        for mok in mkn_output_keys:
            old_new_k2n_from_map[mok] = new_id

    for link in lgo["linkDataArray"]:
        if link["fromPort"] in old_new_k2n_from_map:
            link["from"] = old_new_k2n_from_map[link["fromPort"]]
        elif link["toPort"] in old_new_k2n_to_map:
            link["to"] = old_new_k2n_to_map[link["toPort"]]

    # with open('/tmp/MKN_translate.graph', 'w') as f:
    #    json.dump(lgo, f)
    return lgo


def convert_construct(lgo):
    """
    1. for each scatter/gather, create a "new" application drop, which shares
       the same "id" as the construct
    2. reset the key of the scatter/gather construct to 'new_id'
    3. reset the "parentId" keyword of each drop inside the construct to 'new_id'
    """
    # print('%d nodes in lg' % len(lgo['nodeDataArray']))
    keyset = get_keyset(lgo)
    old_new_grpk_map = dict()
    old_new_gather_map = dict()
    old_newnew_gather_map = dict()
    new_nodes = []

    duplicated_gather_app = dict()  # temmporarily duplicate gather as an extra
    # application drop if a gather has internal input, which will result in
    # a cycle that is not allowed in DAG during graph translation

    # CAUTION: THIS IS VERY LIKELY TO CAUSE ISSUES,
    # SINCE IT IS PICKING THE FIRST ONE FOUND!
    app_keywords = ["inputApplicationType", "outputApplicationType"]
    for node in lgo["nodeDataArray"]:
        if node["category"] not in [
            ConstructTypes.SCATTER,
            ConstructTypes.GATHER,
            ConstructTypes.SERVICE,
        ]:
            continue
        has_app = ""

        # try to find a application using several app_keywords
        # disregard app_keywords that are not present, or have value "None"
        has_app = _has_app_keywords(node, app_keywords)
        if not has_app:
            continue

        # step 1
        app_args = {"fields": "inputAppFields"}
        if node["category"] == ConstructTypes.GATHER:
            app_args["group_start"] = 1

        if node["category"] == ConstructTypes.SERVICE:
            app_args["isService"] = True

        app_node = _create_from_node(node, node[has_app], app_args)

        # step 2
        new_id = str(uuid.uuid4())
        node["id"] = new_id
        keyset.add(new_id)
        old_new_grpk_map[app_node["id"]] = new_id

        if ConstructTypes.GATHER == node["category"]:
            old_new_gather_map[app_node["id"]] = new_id
            app_node["parentId"] = new_id
            app_node["group_start"] = 1

            # extra step to deal with "internal output" from within Gather
            # dup_app_node_k = min(keyset) - 1
            dup_app_node_k = str(uuid.uuid4())
            keyset.add(dup_app_node_k)
            dup_app_args = {
                "id": dup_app_node_k,
                "fields": "appFields" if "appFields" in node else "inputAppFields",
            }
            tmp_node = _create_from_node(
                node=node, category=node[has_app], app_params=dup_app_args
            )
            redundant_keys = ["fields", "reprodata"]
            tmp_node = {k: v for k, v in tmp_node.items() if k not in redundant_keys}
            duplicated_gather_app[new_id] = tmp_node

        new_nodes.append(app_node)

    if new_nodes:
        lgo["nodeDataArray"].extend(new_nodes)

        node_index = _build_node_index(lgo)

        # step 3
        for node in lgo["nodeDataArray"]:
            if "parentId" in node and node["parentId"] in old_new_grpk_map:
                k_old = node["parentId"]
                node["parentId"] = old_new_grpk_map[k_old]

        # step 4
        if old_new_gather_map:
            for link in lgo["linkDataArray"]:
                if link["to"] in old_new_gather_map:
                    k_old = link["to"]
                    new_id = old_new_gather_map[k_old]
                    link["to"] = new_id
                    # TODO Delete everything below this
                    # deal with the internal output from Gather
                    from_node = node_index[link["from"]]
                    # this is an obsolete and awkard way of checking internal output (for backward compatibility)
                    if "parentId" in from_node and from_node["parentId"] == new_id:
                        dup_app_node = duplicated_gather_app[new_id]
                        new_id_new = dup_app_node["id"]
                        link["to"] = new_id_new
                        if new_id_new not in node_index:
                            node_index[new_id_new] = dup_app_node
                            dup_app_node["reprodata"] = (
                                node_index[new_id].get("reprodata", {}).copy()
                            )
                            lgo["nodeDataArray"].append(dup_app_node)
                            old_newnew_gather_map[k_old] = new_id_new

            # step 5
            # relink the connection from gather to its external output if the gather
            # has internal output that has been delt with in Step 4
            for link in lgo["linkDataArray"]:
                if link["from"] in old_new_gather_map:
                    k_old = link["from"]
                    new_id = old_new_gather_map[k_old]
                    to_node = node_index[link["to"]]
                    gather_construct = node_index[new_id]
                    if "parentId" not in to_node and "parentId" not in gather_construct:
                        cond1 = True
                    elif (
                        "parentId" in to_node
                        and "parentId" in gather_construct
                        and to_node["parentId"] == gather_construct["parentId"]
                    ):
                        cond1 = True
                    else:
                        cond1 = False

                    if cond1 and (k_old in old_newnew_gather_map):
                        link["from"] = old_newnew_gather_map[k_old]
                    # print("from %d to %d to %d" % (link['from'], k_old, link['to']))
    # print('%d nodes in lg after construct conversion' % len(lgo['nodeDataArray']))
    return lgo


def _create_from_node(node: dict, category: str, app_params: dict) -> dict:
    """
    Create a new dictionary from the node based on the category of the new drop, and any
    specific attributes for the application

    The follow node attributes will be setup by default for new nodes:
    - 'reprodata'
    - "id"
    - mkn (conditional)
    - group (conditional)
    - fields (conditional)

    Conditional attributes will be based on their existence in the existing node.

    Any alternatives to the defaults above, or non-default attributes, can be addressed
    by the app_params.

    :param node: The node from which we are deriving the new application
    :param category: The category of the new dictionary
    :param app_params: dict, any non-generic
    :return: new_node: dict, node based on the existing node
    """
    new_node = {}
    new_node["reprodata"] = node.get("reprodata", {}).copy()
    new_node["id"] = node["id"]
    new_node["category"] = category

    new_node["name"] = node["text"] if "text" in node else node["name"]
    if "mkn" in node:
        new_node["mkn"] = node["mkn"]

    if "parentId" in node:
        new_node["parentId"] = node["parentId"]

    if "fields" in app_params:
        field = app_params.pop("fields")
        if field in node:
            new_node["fields"] = list(node[field])
            new_node["fields"] += node["fields"]
            for afd in node[field]:
                new_node[afd["name"]] = afd["value"]

    # Construct-specific mapping behaviour
    for key, value in app_params.items():
        new_node[key] = value

    return new_node


def _has_app_keywords(node: dict, keywords: list, requires_all: bool = False) -> str:
    """
    Check if a single or all keywords exist in the Logical Graph node.

    Default behaviour will return the first keyword that exists in the keywords list.

    :param node: Logical Graph construct node
    :param keywords: keywords we want to check
    :param requires_all: bool, If True, this will return the last keyword if it is exists,
    otherwise it will return None.
    :return: app: the application name
    """

    app = ""
    for ak in keywords:
        if ak in node and node[ak] != "None" and node[ak] != "UnknownApplication":
            if not requires_all:
                return ak
            app = ak
        else:
            if requires_all:
                return ""
    return app


def _update_keys(old_new_grpk_map: dict, lgo: dict) -> dict:
    """
    Iterate through the group keys and replace them based on updated
    logical graph structure

    Note: This will update the logical_graph in-place, but we explicitly return
    the logical_graph to make it clear that it is updated.

    :param old_new_grpk_map: old-new map of group keys, where old is the existing group
    key from the original logical graph template construct, and new is the new group key
    based on the InputApp.
    :param lgo: logical graph template
    :return: the updated logical graph template
    """

    for n in lgo["nodeDataArray"]:
        if "parentId" in n and n["parentId"] in old_new_grpk_map:
            k_old = n["parentId"]
            n["parentId"] = old_new_grpk_map[k_old]

    return lgo


def identify_and_connect_output_input(
    input_node: dict, out_node: dict, logical_graph: dict
) -> dict:
    """
    # If the link is to a node that _isn't_ in the subgraph group
    # then it is an output node,  so check that group is either
    # non-existent, or not equal to the subgraph group.

    Note: This will update the logical_graph in-place, but we explicitly return
    the logical_graph to make it clear that it is updated.

    :param input_node: Input Application node to the subgraph
    :param out_node: Output Application node to the subgraph
    :param logical_graph: The input logical graph (template)
    :return: logical_graph: the updated logical_graph
    """

    for link in logical_graph["linkDataArray"]:
        if link["to"] == input_node["id"]:
            for n in logical_graph["nodeDataArray"]:

                if n["id"] == link["from"]:
                    try:
                        if n["parentId"] == input_node["parentId"]:
                            link["to"] = out_node["id"]
                    except KeyError:
                        pass
        if link["from"] == input_node["id"]:
            for n in logical_graph["nodeDataArray"]:
                if n["id"] == link["to"]:
                    try:
                        if n["parentId"] != input_node["parentId"]:
                            link["from"] = out_node["id"]
                    except KeyError:
                        link["from"] = out_node["id"]
    return logical_graph


def _extract_subgraph_nodes(
    input_node: dict, out_node: dict, logical_graph: dict
) -> (dict, dict, dict):
    """
    1. Identify the SubGraph nodes that are not from the Construct
    2. Find the data inputs to the outputApp
    3. Remove the subgraph nodes (besides the input into the outputapp) from the
       main graph
    4. Create links from the subgraph output to input/output applications

    :param input_node: Input Application node to the subgraph
    :param out_node: Output Application node to the subgraph
    :param logical_graph: The input logical graph (template)
    :return: subgraphNodes: Dictionary of nodes in the subgraph
             subgraphLinks: List of links in the subgraph
             logical_graph: The modified logical_graph
    """
    subgraphNodes = {}
    subgraphLinks = []
    construct_apps = {input_node["id"], out_node["id"]}

    # 1. Identifying subgraph nodes that are not the input/ouput app
    for n in logical_graph["nodeDataArray"]:
        if (
            "parentId" in n
            and n["parentId"] == input_node["parentId"]
            and n["id"] not in construct_apps
        ):
            subgraphNodes[n["id"]] = n

    output_links = {}

    for link in logical_graph["linkDataArray"]:
        if link["from"] in subgraphNodes:
            # Find links from inside the SubGraph to the Output App to preserve
            if link["to"] in construct_apps:
                key = subgraphNodes[link["from"]]
                output_links[key["id"]] = {}
                output_links[key["id"]]["node"] = key
                output_links[key["id"]]["link"] = link
            subgraphLinks.append(link)
        if link["to"] in subgraphNodes.keys() and link not in subgraphLinks:
            subgraphLinks.append(link)

    for e in subgraphNodes.values():
        if e["id"] not in output_links:
            logical_graph["nodeDataArray"].remove(e)
    for e in subgraphLinks:
        logical_graph["linkDataArray"].remove(e)

    # Ensure we aren't linking from the Input/Output app into the subgraph
    # Any nodes outside the subgraph won't exist when it is deployed.
    subgraphLinks = [
        link for link in subgraphLinks if (link["from"] not in construct_apps)
    ]
    subgraphLinks = [
        link for link in subgraphLinks if (link["to"] not in construct_apps)
    ]
    # 4. Create links from the subgraph output data to input/output applications

    for n in output_links.values():
        logical_graph["linkDataArray"].append(
            {"to": n["node"]["id"], "from": input_node["id"]}
        )
        logical_graph["linkDataArray"].append(n["link"])

    return subgraphNodes, subgraphLinks, logical_graph


def _build_apps_from_subgraph_construct(subgraph_node: dict) -> (dict, dict):
    """
    Initialise the input and output apps based on the subgraph construct node

    :param subgraph_node: The SubGraph construct node on the graph, that contains the
    input and output nodes.
    :return: The input and output nodes
    """

    input_app_args = {
        "isSubGraphApp": True,
        "isSubGraphConstruct": False,
        "SubGraphGroupKey": subgraph_node["id"],
        "parentId": subgraph_node["id"],
        "group_start": 1,
        "fields": "inputAppFields",
        "inputApp": True,
    }
    input_node = _create_from_node(
        subgraph_node, subgraph_node["inputApplicationType"], input_app_args
    )

    output_app_args = {
        "id": subgraph_node["outputApplicationId"],
        "isSubGraphApp": True,
        "isSubGraphConstruct": False,
        "SubGraphGroupKey": input_node["id"],
        "parentId": input_node["id"],
        "group_start": 1,
        "fields": "outputAppFields",
        "outputApp": True,
    }
    output_node = _create_from_node(
        subgraph_node, subgraph_node["outputApplicationType"], output_app_args
    )

    return input_node, output_node


def convert_subgraphs(lgo: dict) -> dict:
    """
    Identify any first-order SubGraph constructs in the Logical Graph, and exctract the
    InputApp and OutputApp fields.

    The steps involved in converting subgraphs are:
    1. Identify and extract the input and output applications from the subgraph construct
    2. Create nodes for both
    4. Re-order links to ensure output app is connected to final data output of subgraph
    5. Extract the subgraph from the current graph LGT and store in a separate input data
        node to the Input Python Application (SubGraph dropclass)
    6. Remove subgraph from current graph

    Note: This will update the logical_graph in-place, but we explicitly return
    the logical_graph to make it clear that it is updated.

    :param lgo: dict, logical Graph
    :return: dict, modified Logical Graph
    """

    keyset = get_keyset(lgo)
    old_new_grpk_map = dict()
    old_new_subgraph_map = {}
    new_nodes = []

    app_keywords = ["inputApplicationType", "outputApplicationType"]
    for node in lgo["nodeDataArray"]:
        if node["category"] != ConstructTypes.SUBGRAPH:
            continue

        node["isSubGraphConstruct"] = True
        node["hasInputApp"] = True
        if not _has_app_keywords(node, app_keywords, requires_all=True):
            node["hasInputApp"] = False
            continue

        # Construct nodes
        app_node, out_node = _build_apps_from_subgraph_construct(node)

        # Connect output node to rest of graph
        lgo = identify_and_connect_output_input(app_node, out_node, lgo)
        out_node["parentId"] = app_node["id"]
        new_nodes.extend([app_node, out_node])

        # Update group mappings and bump key
        new_id = str(uuid.uuid4())
        node["id"] = new_id
        keyset.add(new_id)
        old_new_grpk_map[app_node["id"]] = new_id

        # Replace the keys based on new input and output apps.
        if new_nodes:
            old_new_subgraph_map[app_node["id"]] = new_id
            lgo["nodeDataArray"].extend(new_nodes)

            lgo = _update_keys(old_new_grpk_map, lgo)

            # Manage SubGraph nodes and links
            subgraphNodes, subgraphLinks, lgo = _extract_subgraph_nodes(
                app_node, out_node, lgo
            )

            # Create SubGraph as InputData to the SubGraph Input App
            new_id = str(uuid.uuid4())
            keyset.add(new_id)
            subgraph = {
                "nodeDataArray": list(subgraphNodes.values()),
                "linkDataArray": subgraphLinks,
                "modelData": lgo["modelData"],
            }
            for n in lgo["nodeDataArray"]:
                if n["id"] == app_node["id"]:
                    app_node["subgraph"] = subgraph

    return lgo


def convert_eagle_to_daliuge_json(lg_name):
    """
    Convert from EAGLE json to DALiuGe Translation json format, i.e., perform pretranslation.
    Returns a path to the created file in new format.
    """
    # Load logical graph's json content.
    file_object = open(lg_name, "r+")
    logical_graph = json.load(file_object)

    ################## PART I ##########################
    # Moving data from the 'fields' array to node properties.

    # Loop over LG nodes and add new node properties from the 'fields' array.
    nodes = logical_graph["nodeDataArray"]
    for node in nodes:
        fields = node["fields"]
        for field in fields:
            name = field.get("name", "")
            if name != "":
                # Add a node property.
                # print("Set %s to %s" % (name, field.get('value', '')))
                node[name] = field.get("value", "")

    ################## PART II #########################
    # For consitency with old graph editor where Gather/Groupby nodes had input ports.
    # Relink the links from nodes to groups for those links
    # that start in a node-in-a-group and end in a node in Gather/Groupby groups.

    relink_to_groups = False

    if relink_to_groups:
        print("Pretranslation: relinking nodes to groups...")

        # Key-to-node map.
        nodes_all = dict()
        # Building nodes_all dictionary.
        nodes = logical_graph["nodeDataArray"]
        for node in nodes:
            node_key = node.get("id", "")
            nodes_all[node_key] = node

        # Candidate nodes for relinking: node_key-to-group_key map.
        nodes_to_relink = dict()
        # Building nodes_to_relink dictionaries.
        for node in nodes:
            node_key = node.get("id", "")
            group_key = node.get("parentId", "")

            if group_key != "":
                # This is a node inside a group.
                group_node = nodes_all.get(group_key, "")
                group_category = group_node.get("category", "")

                if (
                    group_category == ConstructTypes.GATHER
                    or group_category == ConstructTypes.GROUP_BY
                ):
                    # Check if the node is first in that group.
                    fields = node["fields"]
                    for field in fields:
                        name = field.get("name", "")
                        if name == "group_start":
                            group_start = field.get("value", "")
                            if group_start == "1":
                                # This is a first node in the group.
                                nodes_to_relink[node_key] = group_key
                            break

        links = logical_graph["linkDataArray"]
        for link in links:
            key_from = link["from"]
            node_from = nodes_all.get(key_from, "")
            # Only relink the links that start from a node in a group.
            if node_from.get("parentId", "") != "":
                # The link is from a node inside a group.
                key_to = link["to"]
                relink_to = nodes_to_relink.get(key_to, "")
                if relink_to != "":
                    # Relinking the link from the node to the group.
                    print("Relinking " + str(key_to) + " to " + str(relink_to))
                    link["to"] = relink_to
                    # Remove link coordinates: for testing mainly,
                    # so that if one opens the resulting graph the links are automatically relinked to new ports.
                    link.pop("points", None)

    ###################################################

    # Save modified json file.
    try:
        root_dir = TEMP_FILE_FOLDER
        new_path = os.path.join(root_dir, osp.basename(lg_name))

        # Overwrite file on disks.
        with open(new_path, "w") as outfile:
            json.dump(logical_graph, outfile, sort_keys=True, indent=4)
    except Exception as exp:
        raise RuntimeError(
            "Failed to save a pretranslated graph {0}:{1}".format(lg_name, str(exp))
        ) from exp
    finally:
        pass

    return new_path


def load_lg(f):
    if isinstance(f, str):
        if not os.path.exists(f):
            raise GraphException("Logical graph {0} not found".format(f))
        with open(f) as fp:
            lg = json.load(fp)
    elif hasattr(f, "read"):
        lg = json.load(f)
    else:
        lg = f
    return lg
