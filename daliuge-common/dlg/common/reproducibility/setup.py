from dlg.common.reproducibility.constants import ReproduciblityFlags, REPRO_DEFAULT, rmode_supported


def initialize_lg_data(lg: dict, level: str):
    """
    Creates and appends graph-wide reproducibility data at the logical template stage.
    Currently, this is basically a stub that adds the requested flag to the graph.
    Later, this will contain significantly more information.
    :param lg: The logical graph data structure (a JSON object (a dict))
    :param level: One several values 0-5 defined in constants.py
    :return: The same lg object with new information appended

    TODO: Per-drop initialization
    TODO: Cryptographic processing of structure
    TODO: Definition of behaviour
    """
    level = ReproduciblityFlags(level)
    if not rmode_supported(level):
        level = REPRO_DEFAULT
    reprodata = {
        'level': level
    }
    lg['reproData'] = reprodata

    return lg
