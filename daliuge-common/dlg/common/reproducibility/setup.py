from dlg.common.reproducibility.constants import ReproduciblityFlags, REPRO_DEFAULT, rmode_supported


#  ------ Drop-Based Functionality ------
def accumulate_drop_data(drop: dict, level: ReproduciblityFlags):
    """
    Accumulates relevant reproducibility fields for a given drop.
    :param drop:
    :param level:
    :return: A dictionary containing accumulated reproducibility data for a given drop.
    """
    pass


def init_lgt_repro_drop_data(drop: dict, level: ReproduciblityFlags):
    """
    Creates and appends per-drop reproducibility information at the logical template stage.
    :param drop:
    :param level:
    :return: The same drop with appended reproduciblity information.
    """
    pass


def init_lg_repro_drop_data(drop: dict, level: ReproduciblityFlags):
    """
    Creates and appends per-drop reproducibility information at the logical graph stage.
    :param drop:
    :param level:
    :return: The same drop with appended reproducibility information
    """
    pass


def init_pgt_repro_drop_data(drop: dict, level: ReproduciblityFlags):
    """
    Creates and appends per-drop reproducibility information at the physical graph template stage.
    :param drop:
    :param level:
    :return: The same drop with appended reproducibility information
    """
    pass


def init_pg_repro_drop_data(drop: dict, level: ReproduciblityFlags):
    """
    Creates and appends per-drop reproducibility information at the physical graph stage.
    :param drop:
    :param level:
    :return: The same drop with appended reproducibility information
    """
    pass


#  ------ Graph-Wide Functionality ------

def init_lgt_repro_data(lgt: dict, rmode: str):
    """
    Creates and appends graph-wide reproducibility data at the logical template stage.
    Currently, this is basically a stub that adds the requested flag to the graph.
    Later, this will contain significantly more information.
    :param lgt: The logical graph data structure (a JSON object (a dict))
    :param rmode: One several values 0-5 defined in constants.py
    :return: The same lgt object with new information appended

    TODO: Per-drop initialization
    TODO: Cryptographic processing of structure
    """
    rmode = ReproduciblityFlags(rmode)
    if not rmode_supported(rmode):
        rmode = REPRO_DEFAULT
    reprodata = {
        'rmode': rmode
    }
    lgt['reproData'] = reprodata

    return lgt


def init_lg_repro_data(lg: dict, rmode: str):
    """
    Handles adding reproducibility data at the logical graph level.
    :param lg: The logical graph data structure (a JSON object (a dict))
    :param rmode: One several values 0-5 defined in constants.py
    :return: The same lgt object with new information appended
    """
    pass


def init_pgt_repro_data(pgt: dict, rmode: str):
    """
    Handles adding reproducibility data at the physical graph template level.
    :param pgt: The logical graph data structure (a JSON object (a dict))
    :param rmode: One several values 0-5 defined in constants.py
    :return: The same lgt object with new information appended
    """
    pass


def init_pg_repro_data(pg: dict, rmode: str):
    """
    Handles adding reproducibility data at the physical graph template level.
    :param pg: The logical graph data structure (a JSON object (a dict))
    :param rmode: One several values 0-5 defined in constants.py
    :return: The same lgt object with new information appended
    """
    pass
