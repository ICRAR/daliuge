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

import json
import os
import os.path as osp

LG_VER_OLD = 1
LG_VER_EAGLE_CONVERTED = 2
LG_VER_EAGLE = 3

TEMP_FILE_FOLDER = '/tmp'

def get_lg_ver_type(lg_name):
    """
    Get the version type of this logical graph
    """
    with open(lg_name, 'r+') as file_object:
        lgo = json.load(file_object)

    nodes = lgo['nodeDataArray']
    if (len(nodes) == 0):
        raise Exception("Invalid LG, nodes not found")

    if ('fields' in nodes[0]):
        return LG_VER_EAGLE

    if ('linkDataArray' in lgo and len(lgo['linkDataArray']) > 0):
        lnk = lgo['linkDataArray'][0]
        if (lnk['fromPort'] in ['B', 'T', 'R', 'L']):
            # only the old lg links can be Bottom, Top, Right and Left
            return LG_VER_OLD
        else:
            return LG_VER_EAGLE_CONVERTED
    else:
        if ('copiesArrayObjects' in lgo or 'copiesArrays' in lgo):
            return LG_VER_EAGLE_CONVERTED
        else:
            return LG_VER_OLD

def convert_eagle_to_daliuge_json(lg_name):
    """
    Convert from EAGLE json to DALiuGe Translation json format, i.e., perform pretranslation.
    Returns a path to the created file in new format.
    """
    # Load logical graph's json content.
    file_object = open(lg_name, 'r+')
    logical_graph = json.load(file_object)

    ################## PART I ##########################
    # Moving data from the 'fields' array to node properties.

    # Loop over LG nodes and add new node properties from the 'fields' array.
    nodes = logical_graph['nodeDataArray']
    for node in nodes:
        fields = node['fields']
        for field in fields:
            name = field.get('name', '')
            if (name != ''):
                # Add a node property.
                node[name] = field.get('value', '')

    ################## PART II #########################
    # For consitency with old graph editor where Gather/Groupby nodes had input ports.
    # Relink the links from nodes to groups for those links
    # that start in a node-in-a-group and end in a node in Gather/Groupby groups.

    relink_to_groups = False

    if (relink_to_groups):
        print('Pretranslation: relinking nodes to groups...')

        # Key-to-node map.
        nodes_all = dict()
        # Building nodes_all dictionary.
        nodes = logical_graph['nodeDataArray']
        for node in nodes:
            node_key = node.get('key', '')
            nodes_all[node_key] = node

        # Candidate nodes for relinking: node_key-to-group_key map.
        nodes_to_relink = dict()
        # Building nodes_to_relink dictionaries.
        for node in nodes:
            node_key = node.get('key', '')
            group_key = node.get('group', '')

            if (group_key != ''):
            # This is a node inside a group.
                group_node = nodes_all.get(group_key, '')
                group_category = group_node.get('category', '')

                if (group_category == 'DataGather' or group_category == 'GroupBy'):
                    # Check if the node is first in that group.
                    fields = node['fields']
                    for field in fields:
                        name = field.get('name', '')
                        if (name == 'group_start'):
                            group_start = field.get('value', '')
                            if (group_start == '1'):
                            # This is a first node in the group.
                                nodes_to_relink[node_key] = group_key
                            break

        links = logical_graph['linkDataArray']
        for link in links:
            key_from = link['from']
            node_from = nodes_all.get(key_from, '')
            # Only relink the links that start from a node in a group.
            if (node_from.get('group', '') != ''):
            # The link is from a node inside a group.
                key_to = link['to']
                relink_to = nodes_to_relink.get(key_to, '')
                if (relink_to != ''):
                    # Relinking the link from the node to the group.
                    print('Relinking ' + str(key_to) + ' to ' + str(relink_to))
                    link['to'] = relink_to
                    # Remove link coordinates: for testing mainly,
                    # so that if one opens the resulting graph the links are automatically relinked to new ports.
                    link.pop('points', None)

    ###################################################

    # Save modified json file.
    try:
        root_dir = TEMP_FILE_FOLDER
        new_path = os.path.join(root_dir, osp.basename(lg_name))

        # Overwrite file on disks.
        with open(new_path, "w") as outfile:
            json.dump(logical_graph, outfile, sort_keys=True, indent=4,)
    except Exception as exp:
        raise GraphException("Failed to save a pretranslated graph {0}:{1}".format(lg_name, str(exp)))
    finally:
        pass

    return new_path

if __name__ == '__main__':
    lg_name = '/Users/Chen/proj/daliuge/test/dropmake/logical_graphs/eagle_test01.json'
    convert_eagle_to_daliuge_json(lg_name)
