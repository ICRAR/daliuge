import os
import logging
import pkg_resources

from dlg import common
from dlg.common.reproducibility.reproducibility import init_lg_repro_data, init_lgt_repro_data, \
    init_pgt_unroll_repro_data, init_pgt_partition_repro_data
from dlg.dropmake.lg import load_lg
from dlg.dropmake.pg_generator import unroll, partition

global lg_dir
global pgt_dir

logger = logging.getLogger(__name__)

ALGO_PARAMS = [
    ("min_goal", int),
    ("ptype", int),
    ("max_load_imb", int),
    ("max_cpu", int),
    ("time_greedy", float),
    ("deadline", int),
    ("topk", int),
    ("swarm_size", int),
    ("max_mem", int),
]  # max_mem is only relevant for the old editor, not used in EAGLE


def lg_path(lg_name):
    return "{0}/{1}".format(lg_dir, lg_name)


def lg_exists(lg_name):
    return os.path.exists(lg_path(lg_name))


def pgt_path(pgt_name):
    return "{0}/{1}".format(pgt_dir, pgt_name)


def pgt_exists(pgt_name):
    return os.path.exists(pgt_path(pgt_name))


def lg_repo_contents():
    return _repo_contents(lg_dir)


def pgt_repo_contents():
    return _repo_contents(pgt_dir)


def _repo_contents(root_dir):
    # We currently allow only one depth level
    b = os.path.basename
    contents = {}
    for dirpath, dirnames, fnames in os.walk(root_dir):
        if ".git" in dirnames:
            dirnames.remove(".git")
        if dirpath == root_dir:
            continue

        # Not great yet -- we should do a full second step pruning branches
        # of the tree that are empty
        files = [f for f in fnames if f.endswith(".graph")]
        if files:
            contents[b(dirpath)] = files

    return contents


def file_as_string(fname, package=__name__, enc="utf8"):
    b = pkg_resources.resource_string(package, fname)  # @UndefinedVariable
    return common.b2s(b, enc)


def prepare_lgt(filename, rmode: str):
    return init_lg_repro_data(init_lgt_repro_data(load_lg(filename), rmode))


def unroll_and_partition_with_params(lgt: dict, test: bool, algorithm: str = "none",
                                     num_partitions: int = 1, num_islands: int = 0,
                                     par_label: str = "Partition", algorithm_parameters=None):
    if algorithm_parameters is None:
        algorithm_parameters = {}
    app = "dlg.apps.simple.SleepApp" if test else None
    pgt = init_pgt_unroll_repro_data(unroll(lgt, app=app))
    algo_params = {}
    for name, typ in ALGO_PARAMS:
        if name in algorithm_parameters:
            algo_params[name] = algorithm_parameters.get(name, type=typ)
    reprodata = pgt.pop()
    # Partition the PGT
    pgt = partition(
        pgt,
        algo=algorithm,
        num_partitions=num_partitions,
        num_islands=num_islands,
        partition_label=par_label,
        show_gojs=True,
        **algo_params,
    )

    pgt_spec = pgt.to_pg_spec(
        [],
        ret_str=False,
        num_islands=num_islands,
        tpl_nodes_len=num_partitions + num_islands,
    )
    pgt_spec.append(reprodata)
    init_pgt_partition_repro_data(pgt_spec)
    reprodata = pgt_spec.pop()
    pgt.reprodata = reprodata
    logger.info(reprodata)
    return pgt