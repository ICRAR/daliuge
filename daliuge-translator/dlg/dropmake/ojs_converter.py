#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2025
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
Module to convert from schema OJS to v4

Abbreviation reference:

    LG: Logical Graph
    OJS: Original JSON schema (list-of-dicts)

"""
import copy
import uuid

import dlg.dropmake.schema as sc

def node_list_to_dict(nodes):
    """
    Nodes are derived from nodeDataArray

    :param nodes:
    :return:
    """

    deprecated_node_attributes = [
        "color",
        "comment",
        "drawOrderHint",
        "inputAppFields",
        "inputApplicationComment",
        "inputApplicationDescription",
        "inputApplicationName",
        "inputApplicationType",
        "isGroup",
        "outputAppFields",
        "outputApplicationComment",
        "outputApplicationDescription",
        "outputApplicationName",
        "outputApplicationType",
    ]

    required_node_attributes = {
        "parentId": None,
        "embedId": None
    }

    nd = {}
    for node in nodes:
        fields = field_list_to_dict(node[sc.FIELDS])
        n = {k: v for k, v in node.items() if k not in deprecated_node_attributes}
        for rna, v in required_node_attributes.items():
            if rna not in n:
                n[rna] = v # Set default required node attribues
        nd[node["id"]] = n
        nd[node["id"]][sc.FIELDS] = fields

    return nd

def edge_list_to_dict(edges):
    """
    Edges are derived from the linkDataArray, and contain each individual port-port
    relationship.

    Given LGs have parallel edges (multiple ports-port connections), we
    have individual IDs for these connects.

    :param edges: the OJS edges derived from parameter
    :return:
    """

    v4edges = {}
    for edge in edges:
        id = str(uuid.uuid4())
        v4edges[id] = {
            "id": id,
            sc.V4_DEST_NODE: edge[sc.OJS_DEST_NODE],
            sc.V4_SRC_NODE: edge[sc.OJS_SRC_NODE],
            sc.V4_DEST_PORT: edge[sc.OJS_DEST_PORT],
            sc.V4_SRC_PORT: edge[sc.OJS_SRC_PORT],
            sc.V4_LOOP_AWARE: edge[sc.OJS_LOOP_AWARE],
            sc.CLOSES_LOOP: edge[sc.CLOSES_LOOP]
        }

    return v4edges

def field_list_to_dict(fields):
    """
    Fields are nice and simple: we just need to extract IDs from the existing
    dictionary and use them as keys.

    :param fields:
    :return:
    """

    deprecated_params_replacements = ['Parameter', 'Argument']

    v4field = {}
    for field in  fields:
        v4field[field["id"]] = {k: v for k, v in field.items()}
        # Add edges
        v4field[field["id"]]["edgeIds"] = []
        # Remove deprecated 'parametersuffix'
        for param in deprecated_params_replacements:
            pt = v4field[field["id"]]["parameterType"]
            v4field[field["id"]]["parameterType"] = pt.replace(param, '')
    return v4field


def convert_ojs_to_v4(ojs_graph: dict):
    """
    Convert a LG that uses the OJS schema into the new 'dict-of-dicts' schema.

    The OJS schema is a dictionary of node and edge lists of node and edge attributes.
    This function traverses each list separately to construct a new JSON that conforms to
    the new schema structure.

    The modelData of the OJS graph is copied directly across and unmodified.

    Converstion happens in the following order:

    - Fields, which are subsets of nodes
    - Nodes
    - Edges

    :param ojs_graph: the OJS-versioned graph we are convering to the newer format.
    :return:
    """

    lg_ver = sc.get_lg_ver_type(ojs_graph)
    if lg_ver != sc.LG_VER_EAGLE:
        raise RuntimeError("Cannot convert graph version %s to v4 graph!", lg_ver)

    lgo = {
        sc.MODEL_DATA: {k: v for k, v in ojs_graph[sc.MODEL_DATA].items()},
        sc.GRAPH_CONFIG: {k:v for k, v in ojs_graph[sc.GRAPH_CONFIG].items()},
        sc.ACTIVE_CONFIG:  ojs_graph[sc.ACTIVE_CONFIG] if ojs_graph[sc.ACTIVE_CONFIG]
        else "",
        sc.V4_EDGES: {},
        sc.V4_NODES: {}
    }

    lgo[sc.V4_NODES] = node_list_to_dict(ojs_graph[sc.OJS_NODES])
    lgo[sc.V4_NODES] = edge_list_to_dict(ojs_graph[sc.OJS_EDGES])

    return lgo