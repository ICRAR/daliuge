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


def apply_active_configuration(logical_graph: dict) -> dict:
    """
    Given a JSON representation of the LogicalGraph Template (LGT), apply the
    "Active Configuration" to the relevant constructs/fields.

    :param: logical_graph, dict representation of JSON LGT

    Currently, this does not raise any exceptions, but logs either warnings or errors.
    If there are missing keys or the configuration cannot be applied, it will return
    the original LGT. See graph_config.check_config_is_value for more details.

    return: dict, the updated LG
    """
    if is_config_invalid(logical_graph):
        return logical_graph

    try:
        activeConfigurationID = logical_graph[ACTIVE_CONFIG_KEY]
        activeConfig = logical_graph[CONFIG_KEY][activeConfigurationID]
        nodeDataArray = logical_graph[GRAPH_NODES]

        for node_id, _ in activeConfig["nodes"].items():
            idx = get_key_idx_from_list(node_id, nodeDataArray)
            if idx is None:
                logger.warning(
                    "%s present in activeConfig but not available in Logical Graph.",
                    node_id,
                )
                continue
            node_name = nodeDataArray[idx]["name"]
            for field_id, cfg_field in activeConfig["nodes"][node_id]["fields"].items():
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

    except KeyError:
        logger.warning(
            "Graph config key does not exist in logical graph. Using base field values."
        )

    return logical_graph


def is_config_invalid(logical_graph: dict) -> bool:
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

    checkActiveId = logical_graph.get(ACTIVE_CONFIG_KEY)
    if not checkActiveId:
        logger.warning("No %s data available in Logical Graph.", ACTIVE_CONFIG_KEY)
    checkGraphConfig = logical_graph.get(CONFIG_KEY)
    if not checkGraphConfig:
        logger.warning("No %s data available in Logical Graph.", CONFIG_KEY)

    return (not checkActiveId) or (not checkGraphConfig)


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
