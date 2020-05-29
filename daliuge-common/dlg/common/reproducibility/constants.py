from enum import Enum


class ReproduciblityFlags(Enum):
    NOTHING = 0
    RERUN = 1
    REPEAT = 2
    REPRODUCE = 3
    REPLICATE_COMP = 4
    REPLICATE_SCI = 5
    EXPERIMENTAL = 6


REPRO_DEFAULT = ReproduciblityFlags.RERUN
