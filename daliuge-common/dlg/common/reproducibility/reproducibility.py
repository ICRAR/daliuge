import logging

from dlg.common.reproducibility.constants import ReproduciblityFlags, REPRO_DEFAULT, PROTOCOL_VERSION, HASHING_ALG, \
    rmode_supported
from merklelib import MerkleTree

logger = logging.getLogger(__name__)


def common_hash(value):
    return HASHING_ALG(value).hexdigest()


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


def accumulate_pgt_unroll_drop_data(drop: dict):
    """
    Accumulates relevant reproducibility fields for a single drop at the physical template level.
    TODO: Implement alternative level functionality.
    :param drop:
    :return: A dictionary containing accumulated reproducibility data for a given drop.
    """
    data = {}
    rmode = ReproduciblityFlags(int(drop['reprodata']["rmode"]))
    if not rmode_supported(rmode):
        logger.warning("Requested reproducibility mode %s not yet implemented", str(rmode))
        rmode = REPRO_DEFAULT
        drop['reprodata']["rmode"] = str(rmode.value)
    if rmode == ReproduciblityFlags.NOTHING:
        return data
    data["type"] = drop["type"]
    data["rank"] = drop["rank"]
    if data["type"] == 'plain':
        data["storage"] = drop['storage']
    else:
        data["app"] = drop["app"]

    return data


def accumulate_pgt_partition_drop_data(drop: dict):
    """
    Is as combination of unroll drop data
    :param drop:
    :return:
    """
    rmode = ReproduciblityFlags(int(drop['reprodata']["rmode"]))
    if not rmode_supported(rmode):
        logger.warning("Requested reproducibility mode %s not yet implemented", str(rmode))
        rmode = REPRO_DEFAULT
        drop['reprodata']["rmode"] = str(rmode.value)
    data = accumulate_pgt_unroll_drop_data(drop)
    # This is the only piece of new information added at the partition level
    # It is only pertinent to Repetition and Computational replication
    if rmode == ReproduciblityFlags.REPEAT or rmode == ReproduciblityFlags.REPLICATE_COMP:
        data["node"] = drop["node"][1:]
        data["island"] = drop["island"][1:]
    return data


def accumulate_pg_drop_data(drop: dict):
    """
    Accumulate relevant reproducibility fields for a single drop at the physical graph level.
    TODO: Implement alternative level functionality.
    :param drop:
    :return: A dictionary containing accumulated reproducibility data for a given drop.
    """
    rmode = ReproduciblityFlags(int(drop['reprodata']["rmode"]))
    if not rmode_supported(rmode):
        logger.warning("Requested reproducibility mode %s not yet implemented", str(rmode))
        rmode = REPRO_DEFAULT
        drop['reprodata']["rmode"] = str(rmode.value)
    data = {}
    if rmode == ReproduciblityFlags.REPEAT or rmode == ReproduciblityFlags.REPLICATE_COMP:
        data['node'] = drop['node']
        data['island'] = drop['island']
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
    merkletree = MerkleTree(merkledata, common_hash)
    data['merkleroot'] = merkletree.merkle_root
    drop['reprodata'] = {'rmode': str(level.value), 'lgt_data': data, 'lg_parenthashes': []}
    return drop


def init_lg_repro_drop_data(drop: dict):
    """
    Creates and appends per-drop reproducibility information at the logical graph stage.
    :param drop:
    :return: The same drop with appended reproducibility information
    """
    rmode = ReproduciblityFlags(int(drop['reprodata']['rmode']))
    if not rmode_supported(rmode):
        logger.warning("Requested reproducibility mode %s not yet implemented", str(rmode))
        rmode = REPRO_DEFAULT
        drop['reprodata']["rmode"] = str(rmode.value)
    data = accumulate_lg_drop_data(drop, rmode)
    merkledata = []
    for key, value in data.items():
        temp = [key, value]
        merkledata.append(temp)
    merkletree = MerkleTree(merkledata, common_hash)
    data['merkleroot'] = merkletree.merkle_root
    drop['reprodata']['lg_data'] = data
    drop['reprodata']['lg_parenthashes'] = []
    return drop


def append_pgt_repro_data(drop: dict, data: dict):
    merkledata = []
    for key, value in data.items():
        temp = [key, value]
        merkledata.append(temp)
    merkletree = MerkleTree(merkledata, common_hash)
    data['merkleroot'] = merkletree.merkle_root
    #  Separated so chaining can happen on independent elements (or both later)
    drop['reprodata']['pgt_parenthashes'] = []
    drop['reprodata']['pgt_data'] = data
    return drop


def init_pgt_unroll_repro_drop_data(drop: dict):
    """
    Creates and appends per-drop reproducibility information at the physical graph template stage when unrolling.
    :param drop:
    :return: The same drop with appended reproducibility information
    """
    data = accumulate_pgt_unroll_drop_data(drop)
    append_pgt_repro_data(drop, data)
    return drop


def init_pgt_partition_repro_drop_data(drop: dict):
    """
    Creates and appends per-drop reproducibility information at the physical graph template stage when partitioning.
    :param drop:
    :return: The same drop with appended reproducibility information
    """
    data = accumulate_pgt_partition_drop_data(drop)
    append_pgt_repro_data(drop, data)
    return drop


def init_pg_repro_drop_data(drop: dict):
    """
    Creates and appends per-drop reproducibility information at the physical graph stage.
    :param drop:
    :return: The same drop with appended reproducibility information
    """
    data = accumulate_pg_drop_data(drop)
    merkledata = []
    for key, value in data.items():
        temp = [key, value]
        merkledata.append(temp)
    merkletree = MerkleTree(merkledata, common_hash)
    data['merkleroot'] = merkletree.merkle_root
    #  Separated so chaining can happen on independent elements (or both later)
    drop['reprodata']['pg_parenthashes'] = []
    drop['reprodata']['pg_data'] = data
    return drop


def init_runtime_repro_drop_data(drop: dict):
    """
    Merges runtime and PG graph data on a per-drop basis.
    :param drop:
    :return:
    """
    pg_data = accumulate_pg_drop_data(drop)
    merkledata = []
    for key, value in pg_data.items():
        temp = [key, value]
        merkledata.append(temp)
    merkledata.append(drop['reprodata']['pg_data']['run_merkleroot'])
    merkletree = MerkleTree(merkledata, common_hash)
    pg_data['run_data'] = drop['reprodata']['pg_data']['run_data'].copy()
    pg_data['run_merkleroot'] = drop['reprodata']['pg_data']['run_merkleroot']
    pg_data['merkleroot'] = merkletree.merkle_root
    drop['reprodata']['pg_data'] = pg_data
    return drop


#  ------ Graph-Wide Functionality ------

def accumulate_meta_data():
    """
    WARNING: Relies on naming convention in hashlib.
    """
    data = {'repro_protocol': PROTOCOL_VERSION, 'hashing_alg': str(HASHING_ALG)[8:-2]}
    return data


def build_lg_block_data(drop: dict):
    block_data = [drop['reprodata']['lgt_data']['merkleroot']]
    lg_hash = drop['reprodata']['lg_data']['merkleroot']
    if lg_hash is not None:
        block_data.append(lg_hash)
    for parenthash in sorted(drop['reprodata']['lg_parenthashes']):
        block_data.append(parenthash)
    mtree = MerkleTree(block_data, common_hash)
    drop['reprodata']['lg_blockhash'] = mtree.merkle_root


def build_pgt_block_data(drop: dict):
    block_data = [drop['reprodata']['pgt_data']['merkleroot'], drop['reprodata']['lg_blockhash']]
    for parenthash in sorted(drop['reprodata']['pgt_parenthashes']):
        block_data.append(parenthash)
    mtree = MerkleTree(block_data, common_hash)
    drop['reprodata']['pgt_blockhash'] = mtree.merkle_root


def build_pg_block_data(drop: dict):
    block_data = [drop['reprodata']['pg_data']['merkleroot'],
                  drop['reprodata']['pgt_blockhash'],
                  drop['reprodata']['lg_blockhash']]
    for parenthash in sorted(drop['reprodata']['pg_parenthashes']):
        block_data.append(parenthash)
    mtree = MerkleTree(block_data, common_hash)
    drop['reprodata']['pg_blockhash'] = mtree.merkle_root


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
            parenthash = dropset[did][0]['reprodata']['lg_blockhash']
            dropset[n][0]['reprodata']['lg_parenthashes'].append(parenthash)
            if dropset[n][1] == 0:  # Add drops at the DAG-frontier
                q.append(n)

    if visited != len(dropset):
        raise Exception("Not a DAG")

    logger.info("BlockDAG Generated at LG/T level")


def build_blockdag(drops: list, abstraction: str = 'pgt'):
    blockstr = 'pgt'
    parentstr = 'pgt_parenthashes'
    block_builder = build_pgt_block_data
    if abstraction == "pg":
        blockstr = 'pg'
        parentstr = 'pg_parenthashes'
        block_builder = build_pg_block_data

    from collections import deque
    dropset = {}
    neighbourset = {}
    visited = 0
    q = deque()

    for drop in drops:
        did = drop['oid']
        dropset[did] = [drop, 0]  # To guarantee all nodes have entries
    for drop in drops:
        did = drop['oid']
        neighbourset[did] = []
        if 'outputs' in drop:
            for dest in drop['outputs']:
                dropset[dest][1] += 1
                neighbourset[did].append(dest)
        if 'consumers' in drop:  # There may be some bizarre scenario when a drop has both
            for dest in drop['consumers']:
                dropset[dest][1] += 1
                neighbourset[did].append(dest)

    for did in dropset:
        if dropset[did][1] == 0:
            q.append(did)

    while q:
        did = q.pop()
        block_builder(dropset[did][0])
        visited += 1
        for n in neighbourset[did]:
            dropset[n][1] -= 1
            # Add our new hash to the parest-hash list
            parenthash = dropset[did][0]['reprodata'][blockstr]
            dropset[n][0]['reprodata'][parentstr].append(parenthash)
            if dropset[n][1] == 0:
                q.append(n)

    if visited != len(dropset):
        raise Exception("Not a DAG")

    # logger.info("BlockDAG Generated at" + abstraction + " level")


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
        logger.warning("Requested reproducibility mode %s not yet implemented", str(rmode))
        rmode = REPRO_DEFAULT
    reprodata = {'rmode': str(rmode.value), 'meta_data': accumulate_meta_data()}
    meta_tree = MerkleTree(reprodata, common_hash)
    reprodata['merkleroot'] = meta_tree.merkle_root
    for drop in lgt['nodeDataArray']:
        init_lgt_repro_drop_data(drop, rmode)
    lgt['reprodata'] = reprodata
    logger.info("Reproducibility data finished at LGT level")
    return lgt


def init_lg_repro_data(lg: dict):
    """
    Handles adding reproducibility data at the logical graph level.
    Also builds the logical data blockdag over the entire structure.
    :param lg: The logical graph data structure (a JSON object (a dict))
    :return: The same lgt object with new information appended
    """
    for drop in lg['nodeDataArray']:
        init_lg_repro_drop_data(drop)
    lg_build_blockdag(lg)
    logger.info("Reproducibility data finished at LG level")
    return lg


def init_pgt_unroll_repro_data(pgt: list):
    """
    Handles adding reproducibility data at the physical graph template level.
    :param pgt: The physical graph template structure (a list of drops + reprodata dictionary)
    :return: The same pgt object with new information appended
    """
    reprodata = pgt.pop()
    for drop in pgt:
        init_pgt_unroll_repro_drop_data(drop)
    build_blockdag(pgt, 'pgt')
    pgt.append(reprodata)
    logger.info("Reproducibility data finished at PGT unroll level")
    return pgt


def init_pgt_partition_repro_data(pgt: list):
    """
    Handles adding reproducibility data at the physical graph template level after resource partitioning.
    :param pgt: The physical graph template structure (a list of drops + reprodata dictionary)
    :return: The same pgt object with new information recorded
    """
    reprodata = pgt.pop()
    for drop in pgt:
        init_pgt_partition_repro_drop_data(drop)
    build_blockdag(pgt, 'pgt')
    pgt.append(reprodata)
    logger.info("Reproducibility data finished at PGT partition level")
    return pgt


def init_pg_repro_data(pg: list):
    """
    Handles adding reproducibility data at the physical graph template level.
    :param pg: The logical graph data structure (a list of drops + reprodata dictionary)
    :return: The same pg object with new information appended
    """
    reprodata = pg.pop()
    rmode = ReproduciblityFlags(int(reprodata["rmode"]))
    if not rmode_supported(rmode):
        logger.warning("Requested reproducibility mode %s not yet implemented", str(rmode))
        rmode = REPRO_DEFAULT
        reprodata["rmode"] = str(rmode.value)
    for drop in pg:
        init_pg_repro_drop_data(drop)
    build_blockdag(pg, 'pg')
    pg.append(reprodata)
    logger.info("Reproducibility data finished at PG level")
    return pg


def init_runtime_repro_data(pg: dict, reprodata: dict):
    """
    Adds reproducibility data at the runtime level to graph-wide values.
    :param pg:
    :param reprodata:
    :return:
    """
    rmode = ReproduciblityFlags(int(reprodata["rmode"]))
    if not rmode_supported(rmode):
        # TODO: Logging needs sessionID at this stage
        # logger.warning("Requested reproducibility mode %s not yet implemented", str(rmode))
        rmode = REPRO_DEFAULT
        reprodata["rmode"] = str(rmode.value)
    for drop in pg.items():
        init_runtime_repro_drop_data(drop[1])
    build_blockdag(list(pg.values()), 'pg')
    pg['reprodata'] = reprodata
    # logger.info("Reproducibility data finished at runtime level")
    return pg
