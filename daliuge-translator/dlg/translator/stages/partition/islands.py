#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2026
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#

"""Island preparation for partitioned physical graphs."""


def split_island_resources(node_list, num_islands):
    """Split island-manager and node-manager resources."""
    return (
        node_list[0:num_islands],
        node_list[num_islands:],
    )


def merge_for_islands(
    num_islands,
    form_island,
    num_parts_done,
    node_managers,
    merge_partitions,
):
    """Apply the legacy partition merging required for resource mapping."""
    if form_island:
        merge_partitions(
            num_islands,
            form_island=True,
        )
    elif len(node_managers) < num_parts_done:
        merge_partitions(
            len(node_managers),
            form_island=False,
        )
