import hashlib
from enum import Enum

PROTOCOL_VERSION = 0.1


class ReproduciblityFlags(Enum):
    NOTHING = 0
    RERUN = 1
    REPEAT = 2
    REPRODUCE = 3
    REPLICATE_SCI = 4  # Rerun + Reproduce (holds numerically)
    REPLICATE_COMP = 5  # Repeat + Reproduce (holds numerically)
    EXPERIMENTAL = 6


REPRO_DEFAULT = ReproduciblityFlags.RERUN
HASHING_ALG = hashlib.sha3_256


def rmode_supported(flag: ReproduciblityFlags):
    """
    Determines in a given flag is currently supported.
    A slightly pedantic solution but it does centralize the process.
    There is the possiblity that different functionality is possible on a per-install basis.
    Named to be used as a if rmode_supported(flag)
    :param flag: A ReproducibilityFlag enum being queried
    :return: True if supported, False otherwise
    """
    if flag == ReproduciblityFlags.NOTHING \
            or flag == ReproduciblityFlags.RERUN \
            or flag == ReproduciblityFlags.EXPERIMENTAL:
        return True
    else:
        return False
