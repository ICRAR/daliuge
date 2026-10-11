import os
from copy import deepcopy
from dataclasses import fields
import logging
import importlib.resources
from urllib.parse import urlparse

from dlg import utils
from dlg.clients import CompositeManagerClient
from dlg.common.reproducibility.reproducibility import (
    init_lg_repro_data,
    init_lgt_repro_data,
    init_pgt_partition_repro_data,
)
from dlg.translator.stages.prepare.loader import load_lg
from dlg.translator.artefacts import (
    LogicalGraphTemplate,
    PhysicalGraphTemplatePartitioned,
)
from dlg.translator.errors import StageException
from dlg.translator.pipeline import Pipeline
from dlg.translator.stages.unroll.stage import (
    UnrollOptions,
    UnrollStage,
)
from dlg.translator.stages.partition.stage import (
    PartitionOptions,
    partition,
)
from dlg.translator.stages.partition.algorithms.registry import get_algorithm
from dlg.restutils import RestClientException

logger = logging.getLogger(f"dlg.{__name__}")

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


def lg_path(lg_dir, lg_name):
    return "{0}/{1}".format(lg_dir, lg_name)


def lg_exists(lg_dir, lg_name):
    return os.path.exists(lg_path(lg_dir, lg_name))


def pgt_path(pgt_dir, pgt_name):
    return "{0}/{1}".format(pgt_dir, pgt_name)


def pgt_exists(pgt_dir, pgt_name):
    return os.path.exists(pgt_path(pgt_dir, pgt_name))


def lg_repo_contents(lg_dir):
    return _repo_contents(lg_dir)


def pgt_repo_contents(pgt_dir):
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

def file_as_string(fname, module, enc="utf8"):
    module_path = importlib.resources.files(module)
    res = module_path / fname
    return utils.b2s(res.read_bytes(), enc)


def prepare_lgt(filename, rmode: str):
    return init_lg_repro_data(init_lgt_repro_data(load_lg(filename), rmode))


def filter_dict_to_algo_params(input_dict: dict, algorithm=None):
    """Filter legacy web parameters to those consumed by the algorithm."""
    allowed = None

    if algorithm is not None:
        allowed = {
            option_field.name
            for option_field in fields(
                get_algorithm(algorithm).options_type
            )
        }

    algo_params = {}
    for name, _ in ALGO_PARAMS:
        if name in input_dict and (
            allowed is None or name in allowed
        ):
            algo_params[name] = input_dict.get(name)

    return algo_params


def get_mgr_deployment_methods(mhost, mport, mprefix):
    try:
        mgr_client = CompositeManagerClient(
            host=mhost, port=mport, url_prefix=mprefix, timeout=15
        )
        response = mgr_client.get_submission_method()
        response = response.get("methods", [])
    except RestClientException:
        logger.debug("Cannot connect to manager object at endpoint %s:%d", mhost, mport)
        response = []
    return response


def parse_mgr_url(mgr_url):
    mport = -1
    mparse = urlparse(mgr_url)
    if mparse.scheme == "http":
        mport = 80
    elif mparse.scheme == "https":
        mport = 443
    if mparse.port is not None:
        mport = mparse.port
    mprefix = mparse.path
    if mprefix is not None:
        if mprefix.endswith("/"):
            mprefix = mprefix[:-1]
    else:
        mprefix = ""
    return mparse.hostname, mport, mprefix


def make_algo_param_dict(
    min_goal,
    ptype,
    max_load_imb,
    max_cpu,
    time_greedy,
    deadline,
    topk,
    swam_size,
    max_mem,
):
    return {
        "min_goal": min_goal,
        "ptype": ptype,
        "max_load_imb": max_load_imb,
        "max_cpu": max_cpu,
        "time_greedy": time_greedy,
        "deadline": deadline,
        "topk": topk,
        "swarm_size": swam_size,
        "max_mem": max_mem,
    }



class _LegacyPartitionStage:
    """Partition through Pipeline while preserving the legacy PGT object."""

    name = "partition"

    def __init__(self, opts: PartitionOptions):
        self._opts = opts

    def run(self, pgt):
        legacy_pgt = partition(
            deepcopy(pgt.drops),
            algo=self._opts.algo,
            num_partitions=self._opts.num_partitions,
            num_islands=self._opts.num_islands,
            partition_label=self._opts.partition_label,
            show_gojs=True,
            **self._opts.algo_params,
        )
        legacy_pgt.reprodata = deepcopy(pgt.reprodata)
        return legacy_pgt

    def stamp(self, pgt):
        pgt_spec = pgt.to_pg_spec(
            [],
            ret_str=False,
            num_islands=self._opts.num_islands,
            tpl_nodes_len=(
                self._opts.num_partitions
                + self._opts.num_islands
            ),
        )

        stamped = PhysicalGraphTemplatePartitioned.from_wire(
            init_pgt_partition_repro_data(
                PhysicalGraphTemplatePartitioned(
                    drops=pgt_spec,
                    reprodata=deepcopy(pgt.reprodata),
                ).to_wire()
            )
        )

        pgt.reprodata = stamped.reprodata
        return pgt


def unroll_and_partition_with_params(
    lgt: dict,
    test: bool,
    algorithm: str = "none",
    num_partitions: int = 1,
    num_islands: int = 0,
    par_label: str = "Partition",
    algorithm_parameters=None,
):
    if algorithm_parameters is None:
        algorithm_parameters = {}

    app = "dlg.apps.simple.SleepApp" if test else None
    algo_params = filter_dict_to_algo_params(
        algorithm_parameters,
        algorithm,
    )

    pipeline = Pipeline(
        [
            UnrollStage(
                UnrollOptions(
                    app=app,
                )
            ),
            _LegacyPartitionStage(
                PartitionOptions(
                    algo=algorithm,
                    num_partitions=num_partitions,
                    num_islands=num_islands,
                    partition_label=par_label,
                    algo_params=algo_params,
                )
            ),
        ]
    )
    try:
        pgt = pipeline.run(LogicalGraphTemplate.from_wire(lgt))
    except StageException as e:
        # callers (REST, daliuge-engine) catch GraphException and friends,
        # so hand them the original error rather than the stage wrapper
        if e.__cause__ is None:
            raise
        raise e.__cause__ from None

    logger.info(pgt.reprodata)
    return pgt
