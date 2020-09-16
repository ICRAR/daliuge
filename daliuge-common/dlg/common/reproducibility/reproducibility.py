import logging

from dlg.common.reproducibility.constants import ReproducibilityFlags, REPRO_DEFAULT, PROTOCOL_VERSION, HASHING_ALG, \
    rmode_supported, rflag_caster
from .. import Categories
from merklelib import MerkleTree

logger = logging.getLogger(__name__)


def common_hash(value):  # TODO: check type
    return HASHING_ALG(value).hexdigest()


#  ------ Drop-Based Functionality ------
def accumulate_lgt_drop_data(drop: dict, level: ReproducibilityFlags):
    """
    Accumulates relevant reproducibility fields for a single drop.
    :param drop:
    :param level:
    :return: A dictionary containing accumulated reproducibility data for a given drop.
    """
    data = {}
    if level == ReproducibilityFlags.NOTHING:
        return data

    category_type = drop['categoryType']
    category = drop['category']

    if not rmode_supported(level):
        raise NotImplementedError("Reproducibility level %s not yet supported" % level.name)

    if level == ReproducibilityFlags.REPRODUCE:
        data['category_type'] = category_type
        data['category'] = category
        return data  # Early return to avoid next conditional

    if level.value >= ReproducibilityFlags.RERUN.value:
        data['category_type'] = category_type
        data['category'] = category
        data['numInputPorts'] = len(drop['inputPorts'])
        data['numOutputPorts'] = len(drop['outputPorts'])
        data['streaming'] = drop['streaming']
    return data


def accumulate_lg_drop_data(drop: dict, level: ReproducibilityFlags):
    """
    Accumulates relevant reproducibility fields for a single drop.
    :param drop:
    :param level:
    :return: A dictionary containing accumulated reproducibility data for a given drop.
    """
    data = {}
    if level == ReproducibilityFlags.NOTHING:
        return data

    category_type = drop['categoryType']
    category = drop['category']

    # Cheeky way to get field list into dicts. map(dict, drop...) makes a copy
    fields = {e.pop('name'): e['value'] for e in map(dict, drop['fields'])}

    if not rmode_supported(level):
        raise NotImplementedError("Reproducibility level %s not yet supported" % level.name)
    if level == ReproducibilityFlags.RERUN:
        pass
    elif level == ReproducibilityFlags.REPEAT or level == ReproducibilityFlags.REPLICATE_COMP:
        if category_type == 'Application':
            data['execution_time'] = fields['execution_time']
            data['num_cpus'] = fields['num_cpus']
            if category == Categories.BASH_SHELL_APP:
                data['command'] = fields['Arg01']
            elif category == Categories.DYNLIB_APP:  # TODO: Deal with DYNLIB_PROC
                data['libpath'] = fields['libpath']
            elif category == Categories.MPI:
                data['num_of_procs'] = fields['num_of_procs']
            elif category == Categories.DOCKER:
                data['image'] = fields['image']
                data['command'] = fields['commnad']
                data['user'] = fields['user']
                data['ensureUserAndSwitch'] = fields['ensureUserAndSwitch']
                data['removeContainer'] = fields['removeContainer']
                data['additionalBindings'] = fields['additionalBindings']
            elif category == Categories.COMPONENT:
                data['appclass'] = fields['appclass']
        elif category_type == Categories.DATA:
            data['data_volume'] = fields['data_volume']
            if category == Categories.MEMORY:
                pass
            elif category == Categories.FILE:
                data['filepath'] = fields['filepath']
                data['dirname'] = fields['dirname']
                data['check_filepath_exists'] = fields['check_filepath_exists']
            elif category == Categories.S3:
                pass
            elif category == Categories.NGAS:
                pass
            elif category == Categories.JSON:
                pass
            elif category == Categories.NULL:
                pass
        elif category_type == 'Group':
            data['exitAppName'] = drop['exitAppName']
            if category == Categories.GROUP_BY:
                data['group_key'] = fields['group_key']
                data['group_axis'] = fields['group_axis']
            elif category == Categories.GATHER:
                data['num_of_inputs'] = fields['num_of_inputs']
                data['gather_axis'] = fields['gather_axis']
            elif category == Categories.SCATTER:
                data['num_of_copies'] = fields['num_of_copies']
                data['scatter_axis'] = fields['scatter_axis']
            elif category == Categories.LOOP:
                data['num_of_iter'] = fields['num_of_iter']
        elif category_type == 'Control':
            pass
        elif category_type == 'Other':
            pass
    elif level == ReproducibilityFlags.REPRODUCE:
        pass

    return data


def accumulate_pgt_unroll_drop_data(drop: dict):
    """
    Accumulates relevant reproducibility fields for a single drop at the physical template level.
    :param drop:
    :return: A dictionary containing accumulated reproducibility data for a given drop.
    """
    data = {}
    rmode = rflag_caster(drop['reprodata']['rmode'])
    if not rmode_supported(rmode):
        logger.warning('Requested reproducibility mode %s not yet implemented', str(rmode))
        rmode = REPRO_DEFAULT
        drop['reprodata']['rmode'] = str(rmode.value)
    if rmode == ReproducibilityFlags.NOTHING:
        return data
    if rmode.value >= ReproducibilityFlags.RERUN.value:
        data['type'] = drop['type']
        data['rank'] = drop['rank']
        if data['type'] == 'plain':
            data['storage'] = drop['storage']
        else:
            data['app'] = drop['app']

    return data


def accumulate_pgt_partition_drop_data(drop: dict):
    """
    Is as combination of unroll drop data
    :param drop:
    :return:
    """
    rmode = rflag_caster(drop['reprodata']['rmode'])
    if not rmode_supported(rmode):
        logger.warning("Requested reproducibility mode %s not yet implemented", str(rmode))
        rmode = REPRO_DEFAULT
        drop['reprodata']['rmode'] = str(rmode.value)
    data = accumulate_pgt_unroll_drop_data(drop)
    # This is the only piece of new information added at the partition level
    # It is only pertinent to Repetition and Computational replication
    if rmode == ReproducibilityFlags.REPEAT or rmode == ReproducibilityFlags.REPLICATE_COMP:
        data['node'] = drop['node'][1:]
        data['island'] = drop['island'][1:]
    return data


def accumulate_pg_drop_data(drop: dict):
    """
    Accumulate relevant reproducibility fields for a single drop at the physical graph level.
    :param drop:
    :return: A dictionary containing accumulated reproducibility data for a given drop.
    """
    rmode = rflag_caster(drop['reprodata']['rmode'])
    if not rmode_supported(rmode):
        logger.warning("Requested reproducibility mode %s not yet implemented", str(rmode))
        rmode = REPRO_DEFAULT
        drop['reprodata']['rmode'] = str(rmode.value)
    data = {}
    if rmode == ReproducibilityFlags.REPEAT or rmode == ReproducibilityFlags.REPLICATE_COMP:
        data['node'] = drop['node']
        data['island'] = drop['island']
    return data


def init_lgt_repro_drop_data(drop: dict, level: ReproducibilityFlags):
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
    rmode = rflag_caster(drop['reprodata']['rmode'])
    if not rmode_supported(rmode):
        logger.warning("Requested reproducibility mode %s not yet implemented", str(rmode))
        rmode = REPRO_DEFAULT
        drop['reprodata']['rmode'] = str(rmode.value)
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


def init_rg_repro_drop_data(drop: dict):
    """
    Creates and appends per-drop reproducibility information at the runtime graph stage.
    :param drop:
    :return: The same drop with appended reproducibility information
    """
    drop['reprodata']['rg_parenthashes'] = []
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
    if 'merkleroot' in drop['reprodata']['lg_data']:
        lg_hash = drop['reprodata']['lg_data']['merkleroot']
        block_data.append(lg_hash)
    hashset = set(drop['reprodata']['lg_parenthashes'])
    drop['reprodata']['lg_parenthashes'] = list(hashset)
    for parenthash in sorted(drop['reprodata']['lg_parenthashes']):
        block_data.append(parenthash)
    mtree = MerkleTree(block_data, common_hash)
    drop['reprodata']['lg_blockhash'] = mtree.merkle_root


def build_pgt_block_data(drop: dict):
    block_data = [drop['reprodata']['pgt_data']['merkleroot'], drop['reprodata']['lg_blockhash']]
    hashset = set(drop['reprodata']['pgt_parenthashes'])
    drop['reprodata']['pgt_parenthashes'] = list(hashset)
    for parenthash in sorted(drop['reprodata']['pgt_parenthashes']):
        block_data.append(parenthash)
    mtree = MerkleTree(block_data, common_hash)
    drop['reprodata']['pgt_blockhash'] = mtree.merkle_root


def build_pg_block_data(drop: dict):
    block_data = [drop['reprodata']['pg_data']['merkleroot'],
                  drop['reprodata']['pgt_blockhash'],
                  drop['reprodata']['lg_blockhash']]
    hashset = set(drop['reprodata']['pg_parenthashes'])
    drop['reprodata']['pg_parenthashes'] = list(hashset)
    for parenthash in sorted(drop['reprodata']['pg_parenthashes']):
        block_data.append(parenthash)
    mtree = MerkleTree(block_data, common_hash)
    drop['reprodata']['pg_blockhash'] = mtree.merkle_root


def build_rg_block_data(drop: dict):
    block_data = [drop['reprodata']['rg_data']['merkleroot'],
                  drop['reprodata']['pg_blockhash'],
                  drop['reprodata']['pgt_blockhash'],
                  drop['reprodata']['lg_blockhash']]
    hashset = set(drop['reprodata']['rg_parenthashes'])
    drop['reprodata']['rg_parenthashes'] = list(hashset)
    for parenthash in sorted(drop['reprodata']['rg_parenthashes']):
        block_data.append(parenthash)
    mtree = MerkleTree(block_data, common_hash)
    drop['reprodata']['rg_blockhash'] = mtree.merkle_root


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
    leaves = []
    visited = []
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
        if not neighbourset[did]:  # Leaf node
            leaves.append(did)

    while q:
        did = q.pop()
        # Process
        build_lg_block_data(dropset[did][0])
        visited.append(did)
        rmode = rflag_caster(dropset[did][0]['reprodata']['rmode']).value
        for n in neighbourset[did]:
            dropset[n][1] -= 1
            parenthash = []
            if rmode >= ReproducibilityFlags.REPRODUCE.value:
                if dropset[did][0]['categoryType'] == Categories.DATA:
                    # Add my new hash to the parent-hash list
                    parenthash.append(dropset[did][0]['reprodata']['lg_blockhash'])
                else:
                    # Add my parenthashes to the parent-hash list
                    parenthash.extend(dropset[did][0]['reprodata']['lg_parenthashes'])
            if rmode != ReproducibilityFlags.REPRODUCE.value:  # Non-compressing behaviour
                parenthash.append(dropset[did][0]['reprodata']['lg_blockhash'])

            #  Add our new hash to the parent-hash list
            dropset[n][0]['reprodata']['lg_parenthashes'].extend(parenthash)  # We deal with duplicates later
            if dropset[n][1] == 0:  # Add drops at the DAG-frontier
                q.append(n)

    if len(visited) != len(dropset):
        raise Exception("Not a DAG")

    logger.info("BlockDAG Generated at LG/T level")

    for i in range(len(leaves)):
        leaf = leaves[i]
        leaves[i] = dropset[leaf][0]['reprodata']['lg_blockhash']
    return leaves


def build_blockdag(drops: list, abstraction: str = 'pgt'):
    """
    Uses Kahn's algorithm to topologically sort a logical graph dictionary.
    Exploits that a DAG contains at least one node with in-degree 0.
    Processes drops in-order.
    O(V + E) time complexity.
    :param drops: The list of drops
    :param abstraction: The level of graph abstraction 'pgt' || 'pg'
    :return:
    """
    blockstr = 'pgt'
    parentstr = 'pgt_parenthashes'
    block_builder = build_pgt_block_data
    if abstraction == 'pg':
        blockstr = 'pg'
        parentstr = 'pg_parenthashes'
        block_builder = build_pg_block_data
    if abstraction == 'rg':
        blockstr = 'rg'
        parentstr = 'rg_parenthashes'
        block_builder = build_rg_block_data

    from collections import deque
    dropset = {}
    neighbourset = {}
    leaves = []
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
        if not neighbourset[did]:  # Leaf node
            leaves.append(did)

    while q:
        did = q.pop()
        block_builder(dropset[did][0])
        visited += 1
        rmode = int(dropset[did][0]['reprodata']['rmode'])
        for n in neighbourset[did]:
            dropset[n][1] -= 1
            parenthash = []
            if rmode >= ReproducibilityFlags.REPRODUCE.value:
                # TODO: Hack! may break later, proceed with caution
                if dropset[did][0]['reprodata']['lgt_data']['category_type'] == Categories.DATA:
                    # Add my new hash to the parent-hash list
                    parenthash.append(dropset[did][0]['reprodata'][blockstr + "_blockhash"])
                else:
                    # Add my parenthashes to the parent-hash list
                    parenthash.extend(dropset[did][0]['reprodata'][parentstr])
            if rmode != ReproducibilityFlags.REPRODUCE.value:
                parenthash.append(dropset[did][0]['reprodata'][blockstr + "_blockhash"])
            # Add our new hash to the parest-hash list
            dropset[n][0]['reprodata'][parentstr].extend(parenthash)
            if dropset[n][1] == 0:
                q.append(n)

    if visited != len(dropset):
        raise Exception("Not a DAG")

    for i in range(len(leaves)):
        leaf = leaves[i]
        leaves[i] = dropset[leaf][0]['reprodata'][blockstr + '_blockhash']
    return leaves

    # logger.info("BlockDAG Generated at" + abstraction + " level")


def agglomerate_leaves(leaves: list):
    """
    Inserts all hash values in `leaves` into a merkleTree in sorted order (ascending).
    Returns the root of this tree
    """
    merkletree = MerkleTree(sorted(leaves))
    return merkletree.merkle_root


def init_lgt_repro_data(lgt: dict, rmode: str):
    """
    Creates and appends graph-wide reproducibility data at the logical template stage.
    Currently, this is basically a stub that adds the requested flag to the graph.
    Later, this will contain significantly more information.
    :param lgt: The logical graph data structure (a JSON object (a dict))
    :param rmode: One several values 0-5 defined in constants.py
    :return: The same lgt object with new information appended
    """
    rmode = rflag_caster(rmode)
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
    leaves = lg_build_blockdag(lg)
    lg['reprodata']['signature'] = agglomerate_leaves(leaves)
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
    leaves = build_blockdag(pgt, 'pgt')
    reprodata['signature'] = agglomerate_leaves(leaves)
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
    leaves = build_blockdag(pgt, 'pgt')
    reprodata['signature'] = agglomerate_leaves(leaves)
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
    rmode = rflag_caster(reprodata['rmode'])
    if not rmode_supported(rmode):
        logger.warning("Requested reproducibility mode %s not yet implemented", str(rmode))
        rmode = REPRO_DEFAULT
        reprodata['rmode'] = str(rmode.value)
    for drop in pg:
        init_pg_repro_drop_data(drop)
    leaves = build_blockdag(pg, 'pg')
    reprodata['signature'] = agglomerate_leaves(leaves)
    pg.append(reprodata)
    logger.info("Reproducibility data finished at PG level")
    return pg


def init_runtime_repro_data(rg: dict, reprodata: dict):
    """
    Adds reproducibility data at the runtime level to graph-wide values.
    :param rg:
    :param reprodata:
    :return:
    """
    rmode = rflag_caster(reprodata['rmode'])
    if not rmode_supported(rmode):
        # TODO: Logging needs sessionID at this stage
        # logger.warning("Requested reproducibility mode %s not yet implemented", str(rmode))
        rmode = REPRO_DEFAULT
        reprodata['rmode'] = str(rmode.value)
    for drop_id, drop in rg.items():
        init_rg_repro_drop_data(drop)
    leaves = build_blockdag(list(rg.values()), 'rg')
    reprodata['signature'] = agglomerate_leaves(leaves)
    rg['reprodata'] = reprodata
    # logger.info("Reproducibility data finished at runtime level")
    return rg
