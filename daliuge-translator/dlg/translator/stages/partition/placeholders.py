#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2026
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#

"""Placeholder handling for deferred physical graph mapping."""


def prepare_node_list(node_list, tpl_nodes_len, num_islands):
    """Prepare the temporary node list used for deferred mapping."""
    if len(node_list) == 0 and tpl_nodes_len > 0:
        return (
            [
                x
                for x in range(
                    tpl_nodes_len + num_islands
                )
            ],
            True,
        )

    return node_list, False


def placeholder_list(length):
    """Return legacy ``#<index>`` placeholders."""
    return [
        "#%s" % index
        for index in range(length)
    ]  # so that placeholder_list(n)[i] == '#i'


def stamp_placeholders(
    drops,
    num_node_managers,
    num_island_managers,
    oid_gid_map,
    gid_island_id_map,
    num_islands,
    form_island,
):
    """Stamp ``#<index>`` node/island placeholders onto partitioned DROPs."""
    node_placeholders = placeholder_list(num_node_managers)
    island_placeholders = placeholder_list(num_island_managers)

    for drop in drops:
        # For now, simply round robin, but need to consider
        # nodes cross COMPUTE islands which has
        # TODO consider distance between a pair of nodes
        gid = oid_gid_map[drop["oid"]]
        drop["node"] = node_placeholders[gid]

        island_index = (
            gid_island_id_map[gid] % num_islands
            if form_island
            else 0
        )
        drop["island"] = island_placeholders[island_index]
