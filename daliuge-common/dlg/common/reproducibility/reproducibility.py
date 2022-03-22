#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2017
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
This module handles the building of reproducibility information for worklfow components and graphs
at all stages of unrolling and execution.

Functions are organized top-to-bottom as per-drop to whole-graph operations.
Within each level of detail there are several functions to deal with different graph abstractions
arranged top-to-bottom as logical to physical to runtime.
"""
import collections
import logging

from dlg.common import Categories
from dlg.common.reproducibility.constants import ReproducibilityFlags, \
    REPRO_DEFAULT, PROTOCOL_VERSION, HASHING_ALG, \
    rmode_supported, rflag_caster
from dlg.common.reproducibility.reproducibility_fields import lgt_block_fields, lg_block_fields,\
    pgt_unroll_block_fields, pgt_partition_block_fields, pg_block_fields, extract_fields
from merklelib import MerkleTree

logger = logging.getLogger(__name__)


def common_hash(value: bytes):
    """
    Produces a hex digest of the `value` provided.
    Assumes standard hashlib algorithm functionality.
    :param value: Bytes to be hashed
    :return: Hex-digest of the value
    """
    return HASHING_ALG(value).hexdigest()


#  ------ Drop-Based Functionality ------
def accumulate_lgt_drop_data(drop: dict, level: ReproducibilityFlags):
    """
    Accumulates relevant reproducibility fields for a single drop.
    :param drop:
    :param level:
    :return: A dictionary containing accumulated reproducibility data for a given drop.
    """
    if not rmode_supported(level):
        raise NotImplementedError("Reproducibility level %s not yet supported" % level.name)
    relevant_fields = lgt_block_fields(level)
    data = extract_fields(drop, relevant_fields)
    return data


def accumulate_lg_drop_data(drop: dict, level: ReproducibilityFlags):
    """
    Accumulates relevant reproducibility fields for a single drop.
    :param drop:
    :param level:
    :return: A dictionary containing accumulated reproducibility data for a given drop.
    """
    if not rmode_supported(level):
        raise NotImplementedError("Reproducibility level %s not yet supported" % level.name)
    category_type = drop.get('categoryType', "")  # Made conditional to support older graphs
    category = drop.get('category', "")

    # Cheeky way to get field list into dicts. map(dict, drop...) makes a copy
    fields = {e.pop('name'): e['value'] for e in map(dict, drop['fields'])}
    lg_fields = lg_block_fields(category_type, category, level)
    data = extract_fields(fields, lg_fields)
    return data


def accumulate_pgt_unroll_drop_data(drop: dict):
    """
    Accumulates relevant reproducibility fields for a single drop at the physical template level.
    :param drop:
    :return: A dictionary containing accumulated reproducibility data for a given drop.
    """
    if drop.get('reprodata') is None:
        drop['reprodata'] = {'rmode': str(REPRO_DEFAULT.value),
                             'lg_blockhash': None}
    if drop['reprodata'].get('rmode') is None:
        rmode = REPRO_DEFAULT
        drop['reprodata']['rmode'] = str(rmode.value)
    else:
        rmode = rflag_caster(drop['reprodata']['rmode'])
    if not rmode_supported(rmode):
        logger.warning('Requested reproducibility mode %s not yet implemented', str(rmode))
        rmode = REPRO_DEFAULT
        drop['reprodata']['rmode'] = str(rmode.value)
    if drop.get('type') is None:
        return {}
    drop_type = drop['type']
    pgt_fields = pgt_unroll_block_fields(drop_type, rmode)
    data = extract_fields(drop, pgt_fields)
    return data


def accumulate_pgt_partition_drop_data(drop: dict):
    """
    Is as combination of unroll drop data
    :param drop:
    :return:
    """
    if drop.get('reprodata') is None:
        drop['reprodata'] = {'rmode': str(REPRO_DEFAULT.value),
                             'lg_blockhash': None}
    if drop['reprodata'].get('rmode') is None:
        rmode = REPRO_DEFAULT
        drop['reprodata']['rmode'] = str(rmode.value)
    else:
        rmode = rflag_caster(drop['reprodata']['rmode'])
    if not rmode_supported(rmode):
        logger.warning("Requested reproducibility mode %s not yet implemented", str(rmode))
        rmode = REPRO_DEFAULT
        drop['reprodata']['rmode'] = str(rmode.value)
    pgt_fields = pgt_partition_block_fields(rmode)
    data = extract_fields(drop, pgt_fields)
    data.update(accumulate_pgt_unroll_drop_data(drop))
    # This is the only piece of new information added at the partition level
    # It is only pertinent to Repetition and Computational replication
    if rmode in (ReproducibilityFlags.REPLICATE_COMP, ReproducibilityFlags.RECOMPUTE):
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
    pg_fields = pg_block_fields(rmode)
    data = extract_fields(drop, pg_fields)
    return data


def init_lgt_repro_drop_data(drop: dict, level: ReproducibilityFlags):
    """
    Creates and appends per-drop reproducibility information at the logical template stage.
    :param drop:
    :param level:
    :return: The same drop with appended reproducibility information.
    """
    # Catch pre-set per-drop rmode
    if 'reprodata' in drop.keys():
        if 'rmode' in drop['reprodata'].keys():
            level = rflag_caster(drop['reprodata']['rmode'])
    data = accumulate_lgt_drop_data(drop, level)
    merkletree = MerkleTree(data.items(), common_hash)
    data['merkleroot'] = merkletree.merkle_root
    drop['reprodata'] = {'rmode': str(level.value), 'lgt_data': data, 'lg_parenthashes': {}}
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
    merkletree = MerkleTree(data.items(), common_hash)
    data['merkleroot'] = merkletree.merkle_root
    drop['reprodata']['lg_data'] = data
    drop['reprodata']['lg_parenthashes'] = {}
    return drop


def append_pgt_repro_data(drop: dict, data: dict):
    """
    Adds provided data dictionary to drop description at PGT level.
    :param drop: The drop description
    :param data: The data to be added - arbitrary dictionary
    :return:
    """
    merkletree = MerkleTree(data.items(), common_hash)
    data['merkleroot'] = merkletree.merkle_root
    #  Separated so chaining can happen on independent elements (or both later)
    drop['reprodata']['pgt_parenthashes'] = {}
    drop['reprodata']['pgt_data'] = data
    return drop


def init_pgt_unroll_repro_drop_data(drop: dict):
    """
    Creates and appends per-drop reproducibility information
    at the physical graph template stage when unrolling.
    :param drop: The drop description
    :return: The same drop with appended reproducibility information
    """
    data = accumulate_pgt_unroll_drop_data(drop)
    append_pgt_repro_data(drop, data)
    return drop


def init_pgt_partition_repro_drop_data(drop: dict):
    """
    Creates and appends per-drop reproducibility information
    at the physical graph template stage when partitioning.
    :param drop: The drop description
    :return: The same drop with appended reproducibility information
    """
    data = accumulate_pgt_partition_drop_data(drop)
    append_pgt_repro_data(drop, data)
    return drop


def init_pg_repro_drop_data(drop: dict):
    """
    Creates and appends per-drop reproducibility information at the physical graph stage.
    :param drop: The drop description
    :return: The same drop with appended reproducibility information
    """
    data = accumulate_pg_drop_data(drop)
    merkletree = MerkleTree(data.items(), common_hash)
    data['merkleroot'] = merkletree.merkle_root
    #  Separated so chaining can happen on independent elements (or both later)
    drop['reprodata']['pg_parenthashes'] = {}
    drop['reprodata']['pg_data'] = data
    return drop


def init_rg_repro_drop_data(drop: dict):
    """
    Creates and appends per-drop reproducibility information at the runtime graph stage.
    :param drop:
    :return: The same drop with appended reproducibility information
    """
    drop['reprodata']['rg_parenthashes'] = {}
    return drop


#  ------ Graph-Wide Functionality ------

def accumulate_meta_data():
    """
    WARNING: Relies on naming convention in hashlib.
    """
    data = {'repro_protocol': PROTOCOL_VERSION, 'hashing_alg': str(HASHING_ALG)[8:-2]}
    return data


def build_lg_block_data(drop: dict):
    """
    Builds the logical graph reprodata entry for a processed drop description
    :param drop: The drop description
    :return:
    """
    block_data = [drop['reprodata']['lgt_data']['merkleroot']]
    if 'merkleroot' in drop['reprodata']['lg_data']:
        lg_hash = drop['reprodata']['lg_data']['merkleroot']
        block_data.append(lg_hash)
    for parenthash in sorted(drop['reprodata']['lg_parenthashes'].values()):
        block_data.append(parenthash)
    mtree = MerkleTree(block_data, common_hash)
    drop['reprodata']['lg_blockhash'] = mtree.merkle_root


def build_pgt_block_data(drop: dict):
    """
    Builds the physical graph template reprodata entry for a processed drop description
    :param drop: The drop description
    :return:
    """
    block_data = []
    if 'pgt_data' in drop['reprodata']:
        if 'merkleroot' in drop['reprodata']['pgt_data']:
            block_data.append(drop['reprodata']['pgt_data']['merkleroot'])
    if 'lg_blockhash' in drop['reprodata']:
        block_data.append(drop['reprodata']['lg_blockhash'])
    for parenthash in sorted(drop['reprodata']['pgt_parenthashes'].values()):
        block_data.append(parenthash)
    mtree = MerkleTree(block_data, common_hash)
    drop['reprodata']['pgt_blockhash'] = mtree.merkle_root


def build_pg_block_data(drop: dict):
    """
    Builds the physical graph reprodata entry for a processed drop description
    :param drop: The drop description
    :return:
    """
    block_data = [drop['reprodata']['pg_data']['merkleroot'],
                  drop['reprodata']['pgt_blockhash'],
                  drop['reprodata']['lg_blockhash']]
    for parenthash in sorted(drop['reprodata']['pg_parenthashes'].values()):
        block_data.append(parenthash)
    mtree = MerkleTree(block_data, common_hash)
    drop['reprodata']['pg_blockhash'] = mtree.merkle_root


def build_rg_block_data(drop: dict):
    """
    Builds the runtime graph reprodata entry for a processed drop description.
    :param drop: The drop description
    :return:
    """
    block_data = [drop['reprodata']['rg_data']['merkleroot'],
                  drop['reprodata']['pg_blockhash'],
                  drop['reprodata']['pgt_blockhash'],
                  drop['reprodata']['lg_blockhash']]
    for parenthash in sorted(drop['reprodata']['rg_parenthashes'].values()):
        block_data.append(parenthash)
    mtree = MerkleTree(block_data, common_hash)
    drop['reprodata']['rg_blockhash'] = mtree.merkle_root


def lg_build_blockdag(logical_graph: dict):
    """
    Uses Kahn's algorithm to topologically sort a logical graph dictionary.
    Exploits that a DAG contains at least one node with in-degree 0.
    Processes drops in-order.
    O(V + E) time complexity.
    :param logical_graph: The logical graph description (template or actual)
    :return: leaves set and the list of visited components (in order).
    """
    dropset = {}  # Also contains in-degree information
    neighbourset = {}
    leaves = []
    visited = []
    queue = collections.deque()
    for drop in logical_graph.get('nodeDataArray', []):
        did = int(drop['key'])
        dropset[did] = [drop, 0, 0]
        neighbourset[did] = []

    for edge in logical_graph.get('linkDataArray', []):
        src = int(edge['from'])
        dest = int(edge['to'])
        dropset[dest][1] += 1
        dropset[src][2] += 1
        neighbourset[src].append(dest)

    #  did == 'drop id'
    for did in dropset:
        if dropset[did][1] == 0:
            queue.append(did)
        if not neighbourset[did]:  # Leaf node
            leaves.append(did)

    while queue:
        did = queue.pop()
        # Process
        build_lg_block_data(dropset[did][0])
        visited.append(did)
        rmode = rflag_caster(dropset[did][0]['reprodata']['rmode']).value
        for neighbour in neighbourset[did]:
            dropset[neighbour][1] -= 1
            parenthash = {}
            if rmode != ReproducibilityFlags.NOTHING:
                if rmode == ReproducibilityFlags.REPRODUCE.value:
                    if dropset[did][0]['categoryType'] == Categories.DATA \
                            and (dropset[did][1] == 0 or dropset[did][2] == 0):
                        # Add my new hash to the parent-hash list
                        if did not in parenthash.keys():
                            parenthash[did] = dropset[did][0]['reprodata']['lg_blockhash']
                        # parenthash.append(dropset[did][0]['reprodata']['lg_blockhash'])
                    else:
                        # Add my parenthashes to the parent-hash list
                        parenthash.update(dropset[did][0]['reprodata']['lg_parenthashes'])
                        # parenthash.extend(dropset[did][0]['reprodata']['lg_parenthashes'])
                if rmode != ReproducibilityFlags.REPRODUCE.value:  # Non-compressing behaviour
                    parenthash[did] = dropset[did][0]['reprodata']['lg_blockhash']
                    # parenthash.append(dropset[did][0]['reprodata']['lg_blockhash'])
                #  Add our new hash to the parent-hash list
                # We deal with duplicates later
                dropset[neighbour][0]['reprodata']['lg_parenthashes'].update(parenthash)
            if dropset[neighbour][1] == 0:  # Add drops at the DAG-frontier
                queue.append(neighbour)

    if len(visited) != len(dropset):
        raise Exception("Untraversed graph")

    logger.info("BlockDAG Generated at LG/T level")

    for i in range(len(leaves)):
        leaf = leaves[i]
        leaves[i] = dropset[leaf][0]['reprodata']['lg_blockhash']
    return leaves, visited


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

    dropset = {}
    neighbourset = {}
    leaves = []
    visited = []
    queue = collections.deque()
    for drop in drops:
        did = drop['oid']
        dropset[did] = [drop, 0, 0]
    for drop in drops:
        did = drop['oid']
        neighbourset[did] = []
        if 'outputs' in drop:
            # Assumes the model where all edges are defined from source to destination.
            # This may not always be the case.
            for dest in drop['outputs']:
                dropset[dest][1] += 1
                dropset[did][2] += 1
                neighbourset[did].append(dest)
        if 'consumers' in drop:  # There may be some bizarre scenario when a drop has both
            for dest in drop['consumers']:
                dropset[dest][1] += 1
                dropset[did][2] += 1
                neighbourset[did].append(dest)  # TODO: Appending may not be correct behaviour
    for did in dropset:
        if dropset[did][1] == 0:
            queue.append(did)
        if not neighbourset[did]:  # Leaf node
            leaves.append(did)
    while queue:
        did = queue.pop()
        block_builder(dropset[did][0])
        rmode = int(dropset[did][0]['reprodata']['rmode'])
        visited.append(did)
        for neighbour in neighbourset[did]:
            dropset[neighbour][1] -= 1
            parenthash = {}
            if rmode != ReproducibilityFlags.NOTHING:
                if rmode == ReproducibilityFlags.REPRODUCE.value:
                    # WARNING: Hack! may break later, proceed with caution
                    if dropset[did][0]['reprodata']['lgt_data']['categoryType'] == Categories.DATA \
                            and (dropset[did][1] == 0 or dropset[did][2] == 0):
                        # Add my new hash to the parent-hash list
                        if did not in parenthash.keys():
                            parenthash[did] = dropset[did][0]['reprodata'][blockstr + '_blockhash']
                        # parenthash.append(dropset[did][0]['reprodata'][blockstr + "_blockhash"])
                    else:
                        # Add my parenthashes to the parent-hash list
                        parenthash.update(dropset[did][0]['reprodata'][parentstr])
                if rmode != ReproducibilityFlags.REPRODUCE.value:
                    parenthash[did] = dropset[did][0]['reprodata'][blockstr + "_blockhash"]
                # Add our new hash to the parent-hash list if on the critical path
                if rmode == ReproducibilityFlags.RERUN.value:
                    if 'iid' in dropset[did][0].keys():
                        if dropset[did][0]['iid'] == '0/0':
                            dropset[neighbour][0]['reprodata'][parentstr].update(parenthash)
                    else:
                        dropset[neighbour][0]['reprodata'][parentstr].update(parenthash)
                elif rmode != ReproducibilityFlags.RERUN.value:
                    dropset[neighbour][0]['reprodata'][parentstr].update(parenthash)
            if dropset[neighbour][1] == 0:
                queue.append(neighbour)

    if len(visited) != len(dropset):
        logger.warning("Not a DAG")

    for i in range(len(leaves)):
        leaf = leaves[i]
        leaves[i] = dropset[leaf][0]['reprodata'][blockstr + '_blockhash']
    return leaves, visited

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
    meta_tree = MerkleTree(reprodata.items(), common_hash)
    reprodata['merkleroot'] = meta_tree.merkle_root
    for drop in lgt.get('nodeDataArray', []):
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
    for drop in lg.get('nodeDataArray', []):
        init_lg_repro_drop_data(drop)
    leaves, visited = lg_build_blockdag(lg)
    if 'reprodata' not in lg:
        reprodata = {'rmode': str(REPRO_DEFAULT), 'meta_data': accumulate_meta_data()}
        meta_tree = MerkleTree(reprodata.items(), common_hash)
        reprodata['merkleroot'] = meta_tree.merkle_root
        lg['reprodata'] = reprodata
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
    leaves, visited = build_blockdag(pgt, 'pgt')
    reprodata['signature'] = agglomerate_leaves(leaves)
    pgt.append(reprodata)
    logger.info("Reproducibility data finished at PGT unroll level")
    return pgt


def init_pgt_partition_repro_data(pgt: list):
    """
    Handles adding reproducibility data at the physical graph template level
    after resource partitioning.
    :param pgt: The physical graph template structure (a list of drops + reprodata dictionary)
    :return: The same pgt object with new information recorded
    """
    reprodata = pgt.pop()
    for drop in pgt:
        init_pgt_partition_repro_drop_data(drop)
    leaves, visited = build_blockdag(pgt, 'pgt')
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
    for drop in pg:
        init_pg_repro_drop_data(drop)
    leaves, visited = build_blockdag(pg, 'pg')
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
    if reprodata is None:
        return {'reprodata': {}}
    rmode = rflag_caster(reprodata['rmode'])
    if not rmode_supported(rmode):
        # TODO: Logging needs sessionID at this stage
        # logger.warning("Requested reproducibility mode %s not yet implemented", str(rmode))
        rmode = REPRO_DEFAULT
        reprodata['rmode'] = str(rmode.value)
    for drop_id, drop in rg.items():
        init_rg_repro_drop_data(drop)
    leaves, visited = build_blockdag(list(rg.values()), 'rg')
    reprodata['signature'] = agglomerate_leaves(leaves)
    rg['reprodata'] = reprodata
    # logger.info("Reproducibility data finished at runtime level")
    return rg
