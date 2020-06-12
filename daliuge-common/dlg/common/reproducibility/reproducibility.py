from dlg.common.reproducibility.constants import ReproduciblityFlags, REPRO_DEFAULT, rmode_supported
from merklelib import MerkleTree


#  ------ Drop-Based Functionality ------
def accumulate_lgt_drop_data(drop: dict, level: ReproduciblityFlags):
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


def accumulate_lg_drop_data(drop: dict, level: ReproduciblityFlags):
    """
    Accumulates relevant reproducibility fields for a single drop.
    TODO: Implement alternative level functionality.
    :param drop:
    :param level:
    :return: A dictionary containing accumulated reproducibility data for a given drop.
    """
    return {}


def accumulate_pgt_drop_data(drop: dict, level: ReproduciblityFlags):
    """
    Accumulates relevant reproducibility fields for a single drop at the physical template level.
    TODO: Implement alternative level functionality.
    :param drop:
    :param level:
    :return: A dictionary containing accumulated reproducibility data for a given drop.
    """
    data = {}
    if level == ReproduciblityFlags.NOTHING:
        return data
    data["type"] = drop["type"]
    data["rank"] = drop["rank"]
    if data["type"] == 'plain':
        data["storage"] = drop['storage']
    else:
        data["app"] = drop["app"]

    return data


def init_lgt_repro_drop_data(drop: dict, level: ReproduciblityFlags):
    """
    Creates and appends per-drop reproducibility information at the logical template stage.
    :param drop:
    :param level:
    :return: The same drop with appended reproducibility information.
    """
    data = accumulate_lgt_drop_data(drop, level)
    merkledata = []
    for key, value in data.items():
        temp = [key, value]
        merkledata.append(temp)
    merkletree = MerkleTree(merkledata)
    data['merkleroot'] = merkletree.merkle_root
    drop['reprodata'] = {'lgt_data': data, 'lg_parenthashes': []}
    return drop


def init_lg_repro_drop_data(drop: dict, level: ReproduciblityFlags):
    """
    Creates and appends per-drop reproducibility information at the logical graph stage.
    :param drop:
    :param level:
    :return: The same drop with appended reproducibility information
    """
    data = accumulate_lg_drop_data(drop, level)
    merkledata = []
    for key, value in data.items():
        temp = [key, value]
        merkledata.append(temp)
    merkletree = MerkleTree(merkledata)
    data['merkleroot'] = merkletree.merkle_root
    drop['reprodata']['lg_data'] = data
    return drop


def init_pgt_unroll_repro_drop_data(drop: dict, level: ReproduciblityFlags):
    """
    Creates and appends per-drop reproducibility information at the physical graph template stage.
    :param drop:
    :param level:
    :return: The same drop with appended reproducibility information
    """
    data = accumulate_pgt_drop_data(drop, level)
    merkledata = []
    for key, value in data.items():
        temp = [key, value]
        merkledata.append(temp)
    merkletree = MerkleTree(merkledata)
    data['merkleroot'] = merkletree.merkle_root
    #  Separated so chaining can happen on independent elements (or both later)
    drop['reprodata']['pg_parenthashes'] = []
    drop['reprodata']['pgt_data'] = data
    return drop


def init_pgt_partition_repro_drop_data(drop: dict, level: ReproduciblityFlags):
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

def build_lg_block_data(drop: dict):
    block_data = [drop['reprodata']['lgt_data']['merkleroot']]
    for parenthash in sorted(drop['reprodata']['lg_parenthashes']):
        block_data.append(parenthash)
    mtree = MerkleTree(block_data)
    drop['reprodata']['lg_blockhash'] = mtree.merkle_root


def lg_build_blockdag(lg: dict):
    """
    Uses Kahn's algorithm to topologically sort a logical graph dictionary.
    Exploits that a DAG contains at least one node with in-degree 0.
    Processes drops in-order.
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
        # Process
        build_lg_block_data(dropset[did][0])
        visited += 1
        for n in neighbourset[did]:
            dropset[n][1] -= 1
            #  Add our new hash to the parent-hash list
            dropset[n][0]['reprodata']['lg_parenthashes'].append(dropset[did][0]['reprodata']['lg_blockhash'])
            if dropset[n][1] == 0:  # Add drops at the DAG-frontier
                q.append(n)

    if visited != len(dropset):
        print("Not a DAG")
        # TODO: Improve error handling


def pgt_build_blockdag(drops: list):
    """
    Uses Kahn's algorithm to topologically sort a list of physical graph template nodes
    Exploits that a DAG contains at least one node with in-degree 0.
    Processes drops in-order.
    O(V + E) time complexity.
    :param drops: The list of drops
    :return:
    """

    pass


def init_lgt_repro_data(lgt: dict, rmode: str):
    """
    Creates and appends graph-wide reproducibility data at the logical template stage.
    Currently, this is basically a stub that adds the requested flag to the graph.
    Later, this will contain significantly more information.
    :param lgt: The logical graph data structure (a JSON object (a dict))
    :param rmode: One several values 0-5 defined in constants.py
    :return: The same lgt object with new information appended
    """
    rmode = ReproduciblityFlags(int(rmode))
    if not rmode_supported(rmode):
        rmode = REPRO_DEFAULT
    reprodata = {'rmode': str(rmode.value)}
    for drop in lgt['nodeDataArray']:
        init_lgt_repro_drop_data(drop, rmode)
    lgt['reprodata'] = reprodata
    return lgt


def init_lg_repro_data(lg: dict):
    """
    Handles adding reproducibility data at the logical graph level.
    Also builds the logical data blockdag over the entire structure.
    :param lg: The logical graph data structure (a JSON object (a dict))
    :return: The same lgt object with new information appended
    """
    rmode = ReproduciblityFlags(int(lg['reprodata']['rmode']))
    if not rmode_supported(rmode):
        rmode = REPRO_DEFAULT
        lg['reprodata']["rmode"] = str(rmode.value)
    for drop in lg['nodeDataArray']:
        init_lg_repro_drop_data(drop, rmode)
    lg_build_blockdag(lg)
    return lg


def init_pgt_unroll_repro_data(pgt: list):
    """
    Handles adding reproducibility data at the physical graph template level.
    :param pgt: The physical graph template structure (a list of drops + reprodata dictionary)
    :return: The same pgt object with new information appended
    """
    reprodata = pgt.pop()
    rmode = ReproduciblityFlags(int(reprodata["rmode"]))
    if not rmode_supported(rmode):
        rmode = REPRO_DEFAULT
        reprodata["rmode"] = str(rmode.value)
    for drop in pgt:
        init_pgt_unroll_repro_drop_data(drop, rmode)
    pgt_build_blockdag(pgt)
    pgt.append(reprodata)
    return pgt


def init_pgt_partition_repro_data(pgt: list):
    """
    Handles adding reproducibility data at the physical graph template level after resource partitioning.
    :param pgt: The physical graph template structure (a list of drops + reprodata dictionary)
    :return: The same pgt object with new information recorded
    """
    #  Check if pg-blockhash is none
    return pgt


def init_pg_repro_data(pg: dict):
    """
    Handles adding reproducibility data at the physical graph template level.
    :param pg: The logical graph data structure (a list of drops + reprodata dictionary)
    :return: The same pg object with new information appended
    """
    return pg
