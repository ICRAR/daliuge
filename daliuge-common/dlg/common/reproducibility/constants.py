import hashlib
from enum import Enum

PROTOCOL_VERSION = 0.1


class ReproducibilityFlags(Enum):
    """
    Enum for supported reproducibility modes.
    TODO: Link to more detail description
    """
    NOTHING = 0
    RERUN = 1
    REPEAT = 2
    RECOMPUTE = 4
    REPRODUCE = 5
    REPLICATE_SCI = 6  # Rerun + Reproduce
    REPLICATE_COMP = 7  # Recompute + Reproduce
    REPLICATE_TOTAL = 8  # Repeat + Reproduce
    EXPERIMENTAL = 9


REPRO_DEFAULT = ReproducibilityFlags.NOTHING
HASHING_ALG = hashlib.sha3_256


def rflag_caster(val, default=REPRO_DEFAULT):
    """
    Function to safely cast strings and ints to their appropriate ReproducibilityFlag
    E.g. rflag_caster(1) -> ReproducibilityFlag.RERUN
    E.g. rlag_caster("3") -> ReproducibilityFlag.REPRODUCE
    E.g. rflag_caster("two") -> REPRO_DEFAULT
    :param val: The passed value (either int or str)
    :param default: The default value to be returned upon failure
    :return: Appropriate ReproducibilityFlag
    """
    if type(val) == str:
        try:
            return ReproducibilityFlags(int(val))
        except(ValueError, TypeError):
            return default
    elif type(val) == int:
        try:
            return ReproducibilityFlags(val)
        except(ValueError, TypeError):
            return default
    elif type(val) is None:
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
    if type(flag) != ReproducibilityFlags:
        raise TypeError("Need to be working with a ReproducibilityFlag enum")
    if flag == ReproducibilityFlags.NOTHING \
            or flag == ReproducibilityFlags.RERUN \
            or flag == ReproducibilityFlags.REPEAT \
            or flag == ReproducibilityFlags.RECOMPUTE \
            or flag == ReproducibilityFlags.REPRODUCE \
            or flag == ReproducibilityFlags.REPLICATE_SCI \
            or flag == ReproducibilityFlags.REPLICATE_COMP \
            or flag == ReproducibilityFlags.REPLICATE_TOTAL \
            or flag == ReproducibilityFlags.EXPERIMENTAL:
        return True
    else:
        return False
