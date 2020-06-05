from dlg.common.reproducibility.constants import ReproduciblityFlags, REPRO_DEFAULT, rmode_supported


#  ------ Drop-Based Functionality ------
def accumulate_rerun_drop_data(drop: dict):
    """
    Accumulates relevant reproducibility fields for a given drop at the Rerun level.
    Asserting Rerunning requires relatively little information. We are more interested in the structure between drops.
    :param drop:
    :return: A dictionary containing accumulated reproducibility data for a given drop.
    """
    data = {}
    category_type = drop['categoryType']

    data['category_type'] = category_type
    data['category'] = drop['category']

    if category_type == "Data":
        data['streaming'] = drop['Streaming']
        pass
    elif category_type == "Application":
        data['streaming'] = drop['Streaming']
        pass
    elif category_type == "Group":
        pass
    elif category_type == "Control":
        pass
    elif category_type == "Other":
        pass

    return data


def init_lgt_repro_drop_data(drop: dict, level: ReproduciblityFlags):
    """
    Creates and appends per-drop reproducibility information at the logical template stage.
    :param drop:
    :param level:
    :return: The same drop with appended reproduciblity information.
    """
    return drop


def init_lg_repro_drop_data(drop: dict, level: ReproduciblityFlags):
    """
    Creates and appends per-drop reproducibility information at the logical graph stage.
    :param drop:
    :param level:
    :return: The same drop with appended reproducibility information
    """
    return drop


def init_pgt_repro_drop_data(drop: dict, level: ReproduciblityFlags):
    """
    Creates and appends per-drop reproducibility information at the physical graph template stage.
    :param drop:
    :param level:
    :return: The same drop with appended reproducibility information
    """
    return drop


def init_pg_repro_drop_data(drop: dict, level: ReproduciblityFlags):
    """
    Creates and appends per-drop reproducibility information at the physical graph stage.
    :param drop:
    :param level:
    :return: The same drop with appended reproducibility information
    """
    return drop


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
    rmode = ReproduciblityFlags(int(rmode))
    if not rmode_supported(rmode):
        rmode = REPRO_DEFAULT
    reprodata = {
        'rmode': str(rmode.value)
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
    return lg


def init_pgt_repro_data(pgt: dict, rmode: str):
    """
    Handles adding reproducibility data at the physical graph template level.
    :param pgt: The logical graph data structure (a JSON object (a dict))
    :param rmode: One several values 0-5 defined in constants.py
    :return: The same lgt object with new information appended
    """
    return pgt


def init_pg_repro_data(pg: dict, rmode: str):
    """
    Handles adding reproducibility data at the physical graph template level.
    :param pg: The logical graph data structure (a JSON object (a dict))
    :param rmode: One several values 0-5 defined in constants.py
    :return: The same lgt object with new information appended
    """
    return pg
