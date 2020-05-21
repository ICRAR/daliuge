from enum import Enum


class ReproducibilityLevels(Enum):
    # Handle None with the default keyword
    NOTHING = 0
    RERUN = 1
    REPEAT = 2
    REPRODUCE = 3
    REPLICATE_COMP = 4
    REPLICATE_SCI = 5
    REPLICATE_FULL = 6
