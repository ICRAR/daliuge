"""
Defines constant values for reproduciblity DAG construction and associated utility functions.
"""

import hashlib
import platform
import sys
from enum import Enum

import GPUtil
import psutil
from merklelib import MerkleTree

PROTOCOL_VERSION = 1.0


class ReproducibilityFlags(int, Enum):
    """
    Enum for supported reproducibility modes.
    TODO: Link to more detail description
    """

    ALL = -1  # Builds and maintains all standards (1-8)
    NOTHING = 0
    RERUN = 1
    REPEAT = 2
    RECOMPUTE = 4
    REPRODUCE = 5
    REPLICATE_SCI = 6  # Rerun + Reproduce
    REPLICATE_COMP = 7  # Recompute + Reproduce
    REPLICATE_TOTAL = 8  # Repeat + Reproduce
    EXPERIMENTAL = 9


ALL_RMODES = [
    ReproducibilityFlags.RERUN,
    ReproducibilityFlags.REPEAT,
    ReproducibilityFlags.RECOMPUTE,
    ReproducibilityFlags.REPRODUCE,
    ReproducibilityFlags.REPLICATE_SCI,
    ReproducibilityFlags.REPLICATE_COMP,
    ReproducibilityFlags.REPLICATE_TOTAL,
]
REPRO_DEFAULT = ReproducibilityFlags.NOTHING
HashingAlg = hashlib.sha3_256


def rflag_caster(val, default=REPRO_DEFAULT):
    """
    Function to safely cast strings and ints to their appropriate ReproducibilityFlag
    E.g. rflag_caster(1) -> ReproducibilityFlag.RERUN
    E.g. rlag_caster("4") -> ReproducibilityFlag.RECOMPUTE
    E.g. rflag_caster("two") -> REPRO_DEFAULT
    :param val: The passed value (either int or str)
    :param default: The default value to be returned upon failure
    :return: Appropriate ReproducibilityFlag
    """
    if val is not None:
        out = default
        try:
            out = ReproducibilityFlags(val)
        except ValueError:
            try:
                out = ReproducibilityFlags(int(val))
            except ValueError:
                for rmode in ALL_RMODES:
                    if val == rmode.name or val == "Reproducibility." + rmode.name:
                        out = rmode
        return out
    return default


def rmode_supported(flag: ReproducibilityFlags):
    """
    Determines in a given flag is currently supported.
    A slightly pedantic solution but it does centralize the process.
    There is the possibility that different functionality is possible on a per-install basis.
    Named to be used as a if rmode_supported(flag)
    :param flag: A ReproducibilityFlag enum being queried
    :return: True if supported, False otherwise
    """
    if not isinstance(flag, ReproducibilityFlags):
        raise TypeError("Need to be working with a ReproducibilityFlag enum")
    return flag in (
        ReproducibilityFlags.ALL,
        ReproducibilityFlags.NOTHING,
        ReproducibilityFlags.RERUN,
        ReproducibilityFlags.REPEAT,
        ReproducibilityFlags.RECOMPUTE,
        ReproducibilityFlags.REPRODUCE,
        ReproducibilityFlags.REPLICATE_SCI,
        ReproducibilityFlags.REPLICATE_COMP,
        ReproducibilityFlags.REPLICATE_TOTAL,
        ReproducibilityFlags.EXPERIMENTAL,
    )


def find_loaded_modules():
    """
    :return: A list of all loaded modules
    """
    loaded_mods = []
    for name, module in sorted(sys.modules.items()):
        if hasattr(module, "__version__"):
            loaded_mods.append(name + " " + str(module.__version__))
        else:
            loaded_mods.append(name)
    return loaded_mods


def system_summary():
    """
    Summarises the system this function is run on.
    Includes system, cpu, gpu and module details
    :return: A dictionary of system details
    """
    merkletree = MerkleTree()
    system_info = {}
    uname = platform.uname()
    system_info["system"] = {
        "system": uname.system,
        "release": uname.release,
        "machine": uname.machine,
        "processor": uname.processor,
    }
    cpu_freq = psutil.cpu_freq()
    system_info["cpu"] = {
        "cores_phys": psutil.cpu_count(logical=False),
        "cores_logic": psutil.cpu_count(logical=True),
        "max_frequency": cpu_freq.max,
        "min_frequency": cpu_freq.min,
    }
    sys_mem = psutil.virtual_memory()
    system_info["memory"] = {"total": sys_mem.total}
    gpus = GPUtil.getGPUs()
    system_info["gpu"] = {}
    for gpu in gpus:
        system_info["gpu"][gpu.id] = {"name": gpu.name, "memory": gpu.memoryTotal}
    system_info["modules"] = find_loaded_modules()
    merkletree.append(list(system_info.items()))
    system_info["signature"] = merkletree.merkle_root
    return system_info
