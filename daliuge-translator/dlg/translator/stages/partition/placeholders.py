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
    ]
