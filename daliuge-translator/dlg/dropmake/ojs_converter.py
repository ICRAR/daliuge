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

import dlg.dropmake.schema as sc

def node_list_to_dict(nodes):
    """
    Nodes are derived from nodeDataArray

    :param nodes:
    :return:
    """

def edge_list_to_dict(edges):
    """
    Edges are derived from the linkDataArray, and contain each individual port-port
    relationship.

    Given LGs have parallel edges (multiple ports-port connections), we
    have individual IDs for these connects.

    :param edges:
    :return:
    """


def field_list_to_dict(fields):
    """
    Fields are nice and simple: we just need to extract IDs and move them
    into the new dictionary

    :param fields:
    :return:
    """


def convert_ojs_to_v4(ojs_graph: dict):
    """
    Convert a LG that uses the OJS schema into the new 'dict-of-dicts' schema.

    The OJS schema is a dictionary of node and edge lists of node and edge attributes.
    This function traverses each list separately to construct a new JSON that conforms to
    the new schema structure.

    The modelData of the OJS graph is copied directly across and unmodified.

    :param ojs_graph: the OJS-versioned graph we are convering to the newer format.
    :return:
    """

    lg_ver = sc.get_lg_ver_type(ojs_graph)
    if lg_ver != sc.LG_VER_EAGLE:
        raise RuntimeError("Cannot convert graph version %s to v4 graph!", lg_ver)

    lgo = {
        sc.MODEL_DATA: {k: v for k, v in ojs_graph[sc.MODEL_DATA].items()},
        sc.V4_EDGES: {},
        sc.V4_NODES: {}
    }

    lgo[sc.V4_NODES] = edge_list_to_dict(ojs_graph[sc.OJS_EDGES])
    lgo[sc.V4_NODES] = node_list_to_dict(ojs_graph[sc.OJS_NODES])