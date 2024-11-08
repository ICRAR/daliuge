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

LOGGER = logging.getLogger(__name__)
ACTIVE_CONFIG_KEY = "activeGraphConfigId"
CONFIG_KEY = "graphConfigurations"
GRAPH_NODES = "nodeDataArray"

def apply_active_configuration(logical_graph: dict) -> dict:
    """
    Given a JSON representation of the LogicalGraph template, apply the 
    "Active Configuration" to the relevant constructs/fields.


    """

    if not ACTIVE_CONFIG_KEY in logical_graph:
        LOGGER.warning("No %s available in Logical Graph.", ACTIVE_CONFIG_KEY)
        return logical_graph
    
    if CONFIG_KEY in logical_graph:
        if not logical_graph[CONFIG_KEY]:
            LOGGER.warning("No %s provided to the Logical Graph.", CONFIG_KEY)
            return logical_graph

        activeConfigurationID = logical_graph[ACTIVE_CONFIG_KEY]
        activeConfig = logical_graph[CONFIG_KEY][activeConfigurationID]
         
        nodeDataArray = logical_graph[GRAPH_NODES] 

        for node_id, fields in activeConfig["nodes"].items():
            idx = get_key_idx_from_list(node_id, nodeDataArray) 
            if idx is None:
                LOGGER.warning(
                    "%s present in activeConfig but no available in Logical Graph."
                )
                continue 
            node_name = nodeDataArray[idx]["name"]
            for field_id, config_field in activeConfig["nodes"][node_id]["fields"].items():
                fieldidx = get_key_idx_from_list(field_id, nodeDataArray[idx]["fields"])
                field = nodeDataArray[idx]["fields"][fieldidx]
                prev_value = field["value"]
                field["value"] =  config_field["value"]
                field_name = field["name"]
                LOGGER.info("Updating: Node %s, Field %s, from %s to %s",
                            node_name, field_name, str(prev_value), str(field["value"]))
                nodeDataArray[idx]["fields"][fieldidx] = field
        logical_graph[GRAPH_NODES] = nodeDataArray
    else:
        LOGGER.error("No graph configuration available in Logical Graph.")

    return logical_graph


def get_key_idx_from_list(key: str, dictList: list) -> int: 
    """
    Retrieve the index of the node that exists in the nodeDataArray sub-dict in the
    Logical Graph. 

    Note: This is necessary because we store each node in a list of dictionaries, rather 
    than a dict of dicts. 
    """
    return next((idx for idx, keyDict in enumerate(dictList) if keyDict['id']==key), None)