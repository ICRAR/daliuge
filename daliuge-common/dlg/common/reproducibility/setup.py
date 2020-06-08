from dlg.common.reproducibility.constants import ReproduciblityFlags, REPRO_DEFAULT, rmode_supported
from merklelib import MerkleTree


#  ------ Drop-Based Functionality ------
def accumulate_drop_data(drop: dict, level: ReproduciblityFlags):
    """
    Accumulates relevant reproducibility fields for a single drop.
    TODO: Implement alternative level functionality.
    :param drop:
    :param level:
    :return: A dictionary containing accumulated reproducibility data for a given drop.
    """
    data = {}
    if level == ReproduciblityFlags.NOTHING:
        return data

    category_type = drop['categoryType']

    data['category_type'] = category_type
    data['category'] = drop['category']

    if category_type == "Data":
        data['streaming'] = drop['streaming']
        pass
    elif category_type == "Application":
        data['streaming'] = drop['streaming']
        pass
    elif category_type == "Group":
        pass
    elif category_type == "Control":
        pass
    elif category_type == "Other":
        pass

    return data


def init_lgt_repro_drop_data(drop: dict, level: ReproduciblityFlags):
    """
    Creates and appends per-drop reproducibility information at the logical template stage.
    :param drop:
    :param level:
    :return: The same drop with appended reproducibility information.
    """
    data = accumulate_drop_data(drop, level)
    merkledata = []
    for key, value in data.items():
        temp = [key, value]
        merkledata.append(temp)
    merkletree = MerkleTree(merkledata)
    data['merkleroot'] = merkletree.merkle_root
    drop['reprodata'] = data
    return drop


def init_lg_repro_drop_data(drop: dict, level: ReproduciblityFlags):
    """
    Creates and appends per-drop reproducibility information at the logical graph stage.
    :param drop:
    :param level:
    :return: The same drop with appended reproducibility information
    """
    return drop


def init_pgt_repro_drop_data(drop: dict, level: ReproduciblityFlags):
    """
    Creates and appends per-drop reproducibility information at the physical graph template stage.
    :param drop:
    :param level:
    :return: The same drop with appended reproducibility information
    """
    return drop


def init_pg_repro_drop_data(drop: dict, level: ReproduciblityFlags):
    """
    Creates and appends per-drop reproducibility information at the physical graph stage.
    :param drop:
    :param level:
    :return: The same drop with appended reproducibility information
    """
    return drop


#  ------ Graph-Wide Functionality ------

def topo_sort(lg: dict):
    """
    Uses Kahn's algorithm to topologically sort a logical graph dictionary.
    Exploits that a DAG contains at least one node with in-degree 0.
    Processes nodes in-order.
    O(V + E) time complexity.
    :param lg:
    :return:
    """
    from collections import deque
    dropset = {}  # Also contains in-degree information
    neighbourset = {}
    visited = 0
    q = deque()

    for drop in lg['nodeDataArray']:
        did = int(drop['key'])
        dropset[did] = [drop, 0]
        neighbourset[did] = []

    for edge in lg['linkDataArray']:
        src = int(edge['from'])
        dest = int(edge['to'])
        dropset[dest][1] += 1
        neighbourset[src].append(dest)

    #  did == 'drop id'
    for did in dropset:
        if dropset[did][1] == 0:
            q.append(did)

    while q:
        did = q.pop()
        visited += 1
        for n in neighbourset[did]:
            dropset[n][1] -= 1
            if dropset[n][1] == 0:  # Add drops at the DAG-frontier
                q.append(n)
        # Process TODO: This is where the block-dag will be built.
        print(did)

    if visited != len(dropset):
        print("Not a DAG")
        # TODO: Improve error handling


def init_lgt_repro_data(lgt: dict, rmode: str):
    """
    Creates and appends graph-wide reproducibility data at the logical template stage.
    Currently, this is basically a stub that adds the requested flag to the graph.
    Later, this will contain significantly more information.
    :param lgt: The logical graph data structure (a JSON object (a dict))
    :param rmode: One several values 0-5 defined in constants.py
    :return: The same lgt object with new information appended

    TODO: Cryptographic processing of structure (chaining)
      TODO: Chaining links
      - Takes the merkle_root of a node, it's parents and adds those to a new Merkletree appending that merkle_root
    """
    rmode = ReproduciblityFlags(int(rmode))
    if not rmode_supported(rmode):
        rmode = REPRO_DEFAULT
    reprodata = {'rmode': str(rmode.value)}
    for drop in lgt['nodeDataArray']:
        init_lgt_repro_drop_data(drop, rmode)
    topo_sort(lgt)
    lgt['reprodata'] = reprodata
    return lgt


def init_lg_repro_data(lg: dict, rmode: str):
    """
    Handles adding reproducibility data at the logical graph level.
    :param lg: The logical graph data structure (a JSON object (a dict))
    :param rmode: One several values 0-5 defined in constants.py
    :return: The same lgt object with new information appended
    """
    return lg


def init_pgt_repro_data(pgt: dict, rmode: str):
    """
    Handles adding reproducibility data at the physical graph template level.
    :param pgt: The logical graph data structure (a JSON object (a dict))
    :param rmode: One several values 0-5 defined in constants.py
    :return: The same lgt object with new information appended
    """
    return pgt


def init_pg_repro_data(pg: dict, rmode: str):
    """
    Handles adding reproducibility data at the physical graph template level.
    :param pg: The logical graph data structure (a JSON object (a dict))
    :param rmode: One several values 0-5 defined in constants.py
    :return: The same lgt object with new information appended
    """
    return pg
