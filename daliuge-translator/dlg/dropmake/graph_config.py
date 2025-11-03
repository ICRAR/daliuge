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
Module containing utility methods for working with GraphConfigs
"""

import logging

logger = logging.getLogger(f"dlg.{__name__}")


ACTIVE_CONFIG_KEY = "activeGraphConfigId"
CONFIG_KEY = "graphConfigurations"
GRAPH_NODES = "nodeDataArray"
GRAPH_CONFIGS = "graphConfigurations"

class GraphConfigException(Exception):
    """
    Base exception for graph configs
    """

class GraphConfigNodeDoesNotExist(GraphConfigException):
    """
    Raised if the Graph Configuration supplies an ID for a Node that does not exist in
    the Logical Graph
    """
    
    def __init__(self, config_id):
        self.msg = (f"Node in graphConfig does not exist in Logical Graph\n"
                    f"id: {config_id}\n")

    def __str__(self):
        return self.msg

class GraphConfigFieldDoesNotExist(GraphConfigException):
    """
    Raised if the Graph Configuration supplies an ID for a field that does not exist in
    the Logical Graph
    """
    def __init__(self, id):
        self.msg = (f"Field in graphConfig does not exist in Logical Graph\n"
                    f"id: {id}\n")

    def __str__(self):
        return self.msg


def apply_active_configuration(logical_graph: dict) -> dict:
    """
    Given a JSON representation of the LogicalGraph Template (LGT), apply the
    "Active Configuration" to the relevant constructs/fields.

    :param: logical_graph, dict representation of JSON LGT

    Currently, this does not raise any exceptions, but logs either warnings or errors.
    If there are missing keys or the configuration cannot be applied, it will return
    the original LGT. See graph_config.is_config_invalid for more details.

    return: dict, the updated LG
    """
    if not is_config_stored_in_graph(logical_graph):
        return logical_graph

    try:
        activeConfigurationID = logical_graph[ACTIVE_CONFIG_KEY]
        activeConfig = logical_graph[CONFIG_KEY][activeConfigurationID]

        logical_graph = apply_configuration(logical_graph, activeConfig)

    except KeyError:
        logger.warning(
            "Graph config key does not exist in logical graph. Using base field values."
        )

    return logical_graph

def change_active_configuration(logical_graph: dict, active_id: str) -> dict:
    """
    :param logical_graph:
    :param active_id:
    :return: dict, updated LG
    """

    logical_graph[ACTIVE_CONFIG_KEY] = active_id
    return logical_graph

def find_config_id_from_name(logical_graph, name):
    """
    For a given name, find the first graph config entry with that name and use it.

    :param logical_graph:
    :param name:
    :return: str, the ID for the name of the graph configuration
    """

    for id, gc  in logical_graph[GRAPH_CONFIGS].items():
        if name == gc['modelData']['name']:
            return id

    return None


def apply_configuration(logical_graph: dict, graph_config) -> dict:
    """
    Given a valid configuration, apply it to the logical graph.

    :param logical_graph: Logical graph (template) to which we are applying a
    configuration
    :param graph_config: Configuration for the current graphs

    :return: updated logical_graph dictionary
    """
    nodeDataArray = logical_graph[GRAPH_NODES]
    for node_id, _ in graph_config["nodes"].items():
        idx = get_key_idx_from_list(node_id, nodeDataArray)
        if idx is None:
            logger.warning(
                "%s present in activeConfig but not available in Logical Graph.",
                node_id,
            )
            continue
        node_name = nodeDataArray[idx]["name"]
        for field_id, cfg_field in graph_config["nodes"][node_id]["fields"].items():
            fieldidx = get_key_idx_from_list(field_id, nodeDataArray[idx]["fields"])
            field = nodeDataArray[idx]["fields"][fieldidx]
            prev_value = field["value"]
            field["value"] = cfg_field["value"]
            field_name = field["name"]
            logger.info(
                "Updating: Node %s, Field %s, from %s to %s",
                node_name,
                field_name,
                str(prev_value),
                str(field["value"]),
            )
            nodeDataArray[idx]["fields"][fieldidx] = field
    logical_graph[GRAPH_NODES] = nodeDataArray
    return logical_graph


def is_config_stored_in_graph(logical_graph: dict) -> bool:
    """
    Given a logical graph, verify that the correct keys are present prior to applying
    the configuration.

    We want to make sure that:
        - "activeGraphConfigId" and "graphConfigurations" exist
        - Both of these store non-empty information

    Note: We do not perform any validation on if the graphConfig is self-consistent; that
    is, if it mentions a graphConfig Key not in the LG, or there is a field/node ID that
    is not in the LG, we report this as an error and continue (this implies an error
    upstream that we cannot resolve).

    :return: True if the config has correct keys and they are present.
    """
    config_stored = True

    checkActiveId = logical_graph.get(ACTIVE_CONFIG_KEY)
    if not checkActiveId:
        logger.warning("No %s data available in Logical Graph.", ACTIVE_CONFIG_KEY)
        config_stored = False
    checkGraphConfig = logical_graph.get(CONFIG_KEY)
    if not checkGraphConfig:
        logger.warning("No %s data available in Logical Graph.", CONFIG_KEY)
        config_stored = False

    return config_stored


def get_key_idx_from_list(key: str, dictList: list) -> int:
    """
    Retrieve the index of the node that exists in the nodeDataArray sub-dict in the
    Logical Graph.

    Note: This is necessary because we store each node in a list of dictionaries, rather
    than a dict of dicts.
    """
    return next(
        (idx for idx, keyDict in enumerate(dictList) if keyDict["id"] == key), None
    )

def crosscheck_ids(logical_graph, graph_config):
    """
    Confirm that the ids provided in the graph_config match the ids for nodes and
    fields in the graph.

    :param logical_graph:
    :param graph_config:
    :return:
    """


    for nid, node in graph_config["nodes"].items():
        idx = get_key_idx_from_list(nid, logical_graph[GRAPH_NODES])
        if idx:
            lg_node = logical_graph[GRAPH_NODES][idx]
            for field in node["fields"]:
                fidx = get_key_idx_from_list(field, lg_node["fields"])
                if not fidx:
                    raise GraphConfigFieldDoesNotExist(field)
        else:
            raise GraphConfigNodeDoesNotExist(nid)


def fill_config(lg, graph_config):
    """
    Given logical graph and a configuration dictionary, attempt to fill the matching
    parameters from the graph_configuration to the logical graph.

    This function attempts to match graph configuration to existing ids in the graph.
    If these exist, then we assume the configuration was produced in EAGLE and apply
    accordingly.

    If there is no existing record of the graph_config ID, then the configuration has been
    generated by a different tool and EAGLE has no knowledge of it. In this instance, we:

        - First, crosscheck ids to ensure that the configuration matches the graph
        - Second, apply the configuration to the graph same as if it were an EAGLE config.

    The returned  logical graph has all configured parameters replaced

    :param lg:
    :param graph_config:
    :return:
    """
    non_eagle = False
    if is_config_stored_in_graph(lg):
        gcid = graph_config["id"]
        if gcid in lg["graphConfigurations"]:
            return apply_configuration(lg, graph_config)
        else:
            non_eagle=True

    if non_eagle:# Do non-eagle things
        try:
            crosscheck_ids(lg, graph_config)
            lg = change_active_configuration(lg, None)
            return apply_configuration(lg, graph_config)
        except GraphConfigException as e:
            logger.error("Graph Config-Logical Graph cross check failed: %s", e)
    logger.warning("If submitting graph, existing values will be applied.")

    return lg
