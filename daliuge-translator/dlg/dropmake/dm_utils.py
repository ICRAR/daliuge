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
import os
import os.path as osp

from ..common import Categories

LG_VER_OLD = 1
LG_VER_EAGLE_CONVERTED = 2
LG_VER_EAGLE = 3
LG_APPREF = "AppRef"

TEMP_FILE_FOLDER = "/tmp"


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
    return set([x["key"] for x in lgo["nodeDataArray"]])


def getNodesKeyDict(lgo):
    """
    Return a dictionary of all nodes with the key attribute value as the key
    and the complete node as the value.
    """
    return dict([(x["key"], x) for x in lgo["nodeDataArray"]])


def convert_fields(lgo):
    nodes = lgo["nodeDataArray"]
    for node in nodes:
        fields = node["fields"]
        for field in fields:
            name = field.get("name", "")
            if name != "":
                # Add a node property.
                # print("Set %s to %s" % (name, field.get('value', '')))
                node[name] = field.get("value", "")
    return lgo


def _build_node_index(lgo):
    ret = dict()
    for node in lgo["nodeDataArray"]:
        ret[node["key"]] = node

    return ret


def _relink_gather(appnode, lgo, gather_newkey, node_index):
    """
    for links whose 'from' is gather, 'to' is gather-internal data,
    relink them such that 'from' is the appnode
    """

    gather_oldkey = appnode["key"]
    for link in lgo["linkDataArray"]:
        if link["from"] == gather_oldkey:
            node = node_index[link["to"]]
            if node["group"] == gather_oldkey:
                pass


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
        from .pg_generator import GraphException

        raise GraphException("M-K and k-N must be pairs of multiples")


def _make_unique_port_key(port_key, node_key):
    return "%s+++%d" % (port_key, node_key)


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
        if Categories.MKN != node["category"]:
            continue
        for ak in app_keywords:
            if ak not in node:
                raise Exception(
                    "MKN construct {0} must specify {1}".format(node["key"], ak)
                )
        mknv_dict = dict()
        for mknv in node["fields"]:
            mknv_dict[mknv["name"]] = int(mknv["value"])
        M, K, N = mknv_dict["m"], mknv_dict["k"], mknv_dict["n"]

        # step 1 - clone the current MKN
        mkn_key = node["key"]
        mkn_local_input_keys = [
            _make_unique_port_key(x["Id"], node["key"]) for x in node["inputLocalPorts"]
        ]
        mkn_output_keys = [
            _make_unique_port_key(x["Id"], node["key"]) for x in node["outputPorts"]
        ]
        node_mk = node
        node_mk["mkn"] = [M, K, N]
        node_kn = copy.deepcopy(node_mk)
        node_split_n = copy.deepcopy(node_mk)

        node_mk["application"] = node["inputApplicationName"]
        node_mk["category"] = Categories.GATHER
        node_mk["type"] = Categories.GATHER
        ipan = node_mk.get("inputApplicationName", "")
        if len(ipan) == 0:
            node_mk["text"] = node_mk["text"] + "_InApp"
        else:
            node_mk["text"] = ipan
        #        del node_mk["inputApplicationName"]
        #        del node_mk["outputApplicationName"]
        #        del node_mk["outputAppFields"]
        new_field = {
            "name": "num_of_inputs",
            "text": "Number of inputs",
            "value": "%d" % (M),
        }
        node_mk["fields"].append(new_field)

        node_kn["category"] = Categories.SCATTER
        node_kn["type"] = Categories.SCATTER

        opan = node_kn.get("outputAppName", "")
        if len(opan) == 0:
            node_kn["text"] = node_kn["text"] + "_OutApp"
        else:
            node_kn["text"] = opan
        k_new = min(keyset) - 1
        keyset.add(k_new)
        node_kn["key"] = k_new
        node_kn["group"] = mkn_key
        dont_change_group.add(k_new)
        old_new_parent_map_split_1[mkn_key] = k_new
        node_kn["application"] = node_kn["outputApplicationName"]
        node_kn["inputAppFields"] = node_kn["outputAppFields"]
        #        del node_kn["inputApplicationName"]
        #        del node_kn["outputApplicationName"]
        #        del node_kn["outputAppFields"]

        new_field_kn = {
            "name": "num_of_copies",
            "text": "Number of copies",
            "value": "%d" % (K),
        }
        node_kn["fields"].append(new_field_kn)
        node_kn["reprodata"] = node.get("reprodata", {}).copy()
        lgo["nodeDataArray"].append(node_kn)

        # for all connections that point to the local input ports of the MKN construct
        # we reconnect them to the "new" scatter
        for mlik in mkn_local_input_keys:
            old_new_k2n_to_map[mlik] = k_new

        # for all connections that go from the outputPorts of the MKN construct
        # we reconnect them from the new scatter
        for mok in mkn_output_keys:
            old_new_k2n_from_map[mok] = k_new

        node_split_n["category"] = Categories.SCATTER
        node_split_n["type"] = Categories.SCATTER
        node_split_n["text"] = "Nothing"
        k_new = min(keyset) - 1
        keyset.add(k_new)
        node_split_n["key"] = k_new
        node_split_n["group"] = mkn_key
        dont_change_group.add(k_new)
        old_new_parent_map_split_2[mkn_key] = k_new

        for mok in mkn_output_keys:
            n_products_map[mok] = k_new

        #        del node_split_n["inputApplicationName"]
        #        del node_split_n["outputApplicationName"]
        #        del node_split_n["outputAppFields"]
        # del node_split_n['intputAppFields']

        new_field_kn = {
            "name": "num_of_copies",
            "text": "Number of copies",
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
        if not "group" in node:
            continue
        if node["key"] in dont_change_group:
            continue
        if node["group"] in old_new_parent_map_split_1:
            node["group"] = old_new_parent_map_split_1[node["group"]]
        elif node["group"] in old_new_parent_map_split_2:
            node["group"] = old_new_parent_map_split_2[node["group"]]

    for node in lgo["nodeDataArray"]:
        if node["key"] in need_to_change_n_products:
            node["group"] = need_to_change_n_products[node["key"]]

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
        if Categories.MKN != node["category"]:
            continue
        for ak in app_keywords:
            if ak not in node:
                raise Exception(
                    "MKN construct {0} must specify {1}".format(node["key"], ak)
                )
        mknv_dict = dict()
        for mknv in node["fields"]:
            mknv_dict[mknv["name"]] = int(mknv["value"])
        M, K, N = mknv_dict["m"], mknv_dict["k"], mknv_dict["n"]
        ratio_mk, ratio_kn = _check_MKN(M, K, N)

        # step 1 - clone the current MKN
        # mkn_key = node['key']
        mkn_local_input_keys = [x["Id"] for x in node["inputLocalPorts"]]
        mkn_output_keys = [x["Id"] for x in node["outputPorts"]]
        node_mk = node
        node_kn = copy.deepcopy(node_mk)

        node_mk["application"] = node["inputApplicationName"]
        node_mk["category"] = Categories.GATHER
        node_mk["text"] = node_mk["text"] + "_InApp"
        del node["inputApplicationName"]
        del node["outputApplicationName"]
        del node["outputAppFields"]
        new_field = {
            "name": "num_of_inputs",
            "text": "Number of inputs",
            "value": "%d" % (ratio_mk),
        }
        node_mk["fields"].append(new_field)

        node_kn["category"] = Categories.GATHER
        node_kn["text"] = node_kn["text"] + "_OutApp"
        k_new = min(keyset) - 1
        keyset.add(k_new)
        node_kn["key"] = k_new
        node_kn["application"] = node_kn["outputApplicationName"]
        node_kn["inputAppFields"] = node_kn["outputAppFields"]
        del node_kn["inputApplicationName"]
        del node_kn["outputApplicationName"]
        del node_kn["outputAppFields"]

        new_field_kn = {
            "name": "num_of_inputs",
            "text": "Number of inputs",
            "value": "%d" % (ratio_kn),
        }
        node_kn["fields"].append(new_field_kn)
        node_kn["reprodata"] = node.get("reprodata", {}).copy()
        lgo["nodeDataArray"].append(node_kn)

        # for all connections that point to the local input ports of the MKN construct
        # we reconnect them to the "new" gather
        for mlik in mkn_local_input_keys:
            old_new_k2n_to_map[mlik] = k_new

        for mok in mkn_output_keys:
            old_new_k2n_from_map[mok] = k_new

    for link in lgo["linkDataArray"]:
        if link["fromPort"] in old_new_k2n_from_map:
            link["from"] = old_new_k2n_from_map[link["fromPort"]]
        elif link["toPort"] in old_new_k2n_to_map:
            link["to"] = old_new_k2n_to_map[link["toPort"]]

    # with open('/tmp/MKN_translate.graph', 'w') as f:
    #    json.dump(lgo, f)
    return lgo


def getAppRefInputs(lgo):
    """
    Function to retrieve the inputs from an App node referenced by a group.
    Used to recover the Gather node inputs for AppRef format graphs.
    """
    for node in lgo["nodeDataArray"]:
        if node["category"] not in [Categories.SCATTER, Categories.GATHER]:
            continue
        has_app = None

    pass


def convert_construct(lgo):
    """
    1. for each scatter/gather, create a "new" application drop, which shares
       the same 'key' as the construct
    2. reset the key of the scatter/gather construct to 'k_new'
    3. reset the "group" keyword of each drop inside the construct to 'k_new'
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

    # CAUTION: THIS IS VERY LIKELY TO CAUSE ISSUES, SINCE IT IS PICKING THE FIRST ONE FOUND!
    #    app_keywords = ["application", "inputApplicationType", "outputApplicationType"]
    app_keywords = ["inputApplicationType", "outputApplicationType"]
    for node in lgo["nodeDataArray"]:
        if node["category"] not in [
            Categories.SCATTER,
            Categories.GATHER,
            Categories.SERVICE,
        ]:
            continue
        has_app = None

        # try to find a application using several app_keywords
        # disregard app_keywords that are not present, or have value "None"
        for ak in app_keywords:
            if ak in node and node[ak] != "None" and node[ak] != "UnknownApplication":
                has_app = ak
                break
        if has_app is None:
            continue
        # step 1
        app_node = dict()
        app_node["reprodata"] = node.get("reprodata", {}).copy()
        app_node["key"] = node["key"]
        app_node["category"] = node[has_app]  # node['application']
        if has_app[0] == "i":
            app_node["text"] = node["text"]
        else:
            app_node["text"] = node["text"]

        if "mkn" in node:
            app_node["mkn"] = node["mkn"]

        if "group" in node:
            app_node["group"] = node["group"]

        INPUT_APP_FIELDS = "inputAppFields"
        if INPUT_APP_FIELDS in node:
            # inputAppFields are converted to fields to be processed like
            # regular application drops
            app_node["fields"] = list(node[INPUT_APP_FIELDS])
            app_node["fields"] += node["fields"]
            # TODO: remove, use fields list
            for afd in node[INPUT_APP_FIELDS]:
                app_node[afd["name"]] = afd["value"]

        if node["category"] == Categories.GATHER:
            app_node["group_start"] = 1

        if node["category"] == Categories.SERVICE:
            app_node["isService"] = True

        new_nodes.append(app_node)

        # step 2
        k_new = min(keyset) - 1
        node["key"] = k_new
        keyset.add(k_new)
        old_new_grpk_map[app_node["key"]] = k_new

        if Categories.GATHER == node["category"]:
            old_new_gather_map[app_node["key"]] = k_new
            app_node["group"] = k_new
            app_node["group_start"] = 1

            # extra step to deal with "internal output" fromo within Gather
            dup_app_node_k = min(keyset) - 1
            keyset.add(dup_app_node_k)
            dup_app_node = dict()
            dup_app_node["key"] = dup_app_node_k
            dup_app_node["category"] = node[has_app]  # node['application']
            dup_app_node["text"] = node["text"]

            if "mkn" in node:
                dup_app_node["mkn"] = node["mkn"]

            if "group" in node:
                dup_app_node["group"] = node["group"]

            for app_fd_name in ["appFields", "inputAppFields"]:
                if app_fd_name in node:
                    for afd in node[app_fd_name]:
                        dup_app_node[afd["name"]] = afd["value"]
                    break
            duplicated_gather_app[k_new] = dup_app_node

    if len(new_nodes) > 0:
        lgo["nodeDataArray"].extend(new_nodes)

        node_index = _build_node_index(lgo)

        # step 3
        for node in lgo["nodeDataArray"]:
            if "group" in node and node["group"] in old_new_grpk_map:
                k_old = node["group"]
                node["group"] = old_new_grpk_map[k_old]

        # step 4
        for link in lgo["linkDataArray"]:
            if link["to"] in old_new_gather_map:
                k_old = link["to"]
                k_new = old_new_gather_map[k_old]
                link["to"] = k_new

                # deal with the internal output from Gather
                from_node = node_index[link["from"]]
                # this is an obsolete and awkard way of checking internal output (for backward compatibility)
                if "group" in from_node and from_node["group"] == k_new:
                    dup_app_node = duplicated_gather_app[k_new]
                    k_new_new = dup_app_node["key"]
                    link["to"] = k_new_new
                    if k_new_new not in node_index:
                        node_index[k_new_new] = dup_app_node
                        dup_app_node["reprodata"] = (
                            node_index[k_new].get("reprodata", {}).copy()
                        )
                        lgo["nodeDataArray"].append(dup_app_node)
                        old_newnew_gather_map[k_old] = k_new_new

        # step 5
        # relink the connection from gather to its external output if the gather
        # has internal output that has been delt with in Step 4
        for link in lgo["linkDataArray"]:
            if link["from"] in old_new_gather_map:
                k_old = link["from"]
                k_new = old_new_gather_map[k_old]
                to_node = node_index[link["to"]]
                gather_construct = node_index[k_new]
                if "group" not in to_node and "group" not in gather_construct:
                    cond1 = True
                elif (
                    "group" in to_node
                    and "group" in gather_construct
                    and to_node["group"] == gather_construct["group"]
                ):
                    cond1 = True
                else:
                    cond1 = False

                if cond1 and (k_old in old_newnew_gather_map):
                    link["from"] = old_newnew_gather_map[k_old]
                # print("from %d to %d to %d" % (link['from'], k_old, link['to']))

    # print('%d nodes in lg after construct conversion' % len(lgo['nodeDataArray']))
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
            node_key = node.get("key", "")
            nodes_all[node_key] = node

        # Candidate nodes for relinking: node_key-to-group_key map.
        nodes_to_relink = dict()
        # Building nodes_to_relink dictionaries.
        for node in nodes:
            node_key = node.get("key", "")
            group_key = node.get("group", "")

            if group_key != "":
                # This is a node inside a group.
                group_node = nodes_all.get(group_key, "")
                group_category = group_node.get("category", "")

                if (
                    group_category == Categories.GATHER
                    or group_category == Categories.GROUP_BY
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
            if node_from.get("group", "") != "":
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
        raise Exception(
            "Failed to save a pretranslated graph {0}:{1}".format(lg_name, str(exp))
        )
    finally:
        pass

    return new_path


if __name__ == "__main__":
    lg_name = "/Users/Chen/proj/daliuge/test/dropmake/logical_graphs/lofar_std.graph"
    # convert_eagle_to_daliuge_json(lg_name)
    print(get_lg_ver_type(lg_name))
