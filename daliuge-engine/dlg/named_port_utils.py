import ast
from enum import Enum
import logging
import collections
from typing import Tuple
from dlg import droputils, drop_loaders

logger = logging.getLogger(__name__)


class DropParser(Enum):
    RAW = "raw"
    PICKLE = "pickle"
    EVAL = "eval"
    NPY = "npy"
    # JSON = "json"
    PATH = "path"  # input only
    DATAURL = "dataurl"  # input only


def serialize_kwargs(keyargs, prefix="--", separator=" "):
    kwargs = []
    for name, value in iter(keyargs.items()):
        if prefix == "--" and len(name) == 1:
            kwargs += [f"-{name} {value}"]
        else:
            kwargs += [f"{prefix.strip()}{name.strip()}{separator}{str(value).strip()}"]
    logger.debug("kwargs after serialization: %s", kwargs)
    return kwargs


def clean_applicationArgs(applicationArgs: dict) -> dict:
    """
    Removes arguments with None and False values, if not precious. This
    is in particular used for Bash and Docker app command lines, else
    we would have empty values for command line arguments.

    Args:
        applicationsArgs (dict): the complete set of arguments

    Returns:
        dict: a dictionary with the relevant arguments only.
    """
    cleanedArgs = {}
    if not isinstance(applicationArgs, dict):
        logger.info("applicationArgs are not passed as a dict. Ignored!")
    for name, vdict in applicationArgs.items():
        if vdict in [None, False, ""]:
            continue
        elif isinstance(vdict, bool):
            vdict = {"precious": False, "value": "", "positional": False}
        elif isinstance(vdict, dict):
            precious = vdict["precious"]
            if vdict["value"] in [None, False, ""] and not precious:
                continue
        cleanedArgs.update({name: vdict})
    logger.debug("After clean_applicationArgs: %s", cleanedArgs)
    return cleanedArgs


def serialize_applicationArgs(applicationArgs, prefix="--", separator=" "):
    """
    Unpacks the applicationArgs dictionary and returns two strings, one for
    positional arguments and one for kw arguments that can be used to construct
    the final command line.
    """
    applicationArgs = clean_applicationArgs(
        applicationArgs, prefix=prefix, separator=separator
    )
    pargs = []
    kwargs = {}
    for name, vdict in applicationArgs.items():
        value = vdict["value"]
        positional = vdict["positional"]
        if positional:
            pargs.append(str(value).strip())
        else:
            kwargs.update({name: value})
    skwargs = serialize_kwargs(kwargs, prefix=prefix, separator=separator)
    logger.info("Constructed command line arguments: %s %s", pargs, kwargs)
    return (pargs, skwargs)


def identify_named_ports(
    port_dict: dict,
    posargs: list,
    pargsDict: dict,
    keyargs: dict,
    check_len: int = 0,
    mode: str = "inputs",
    parser: callable = None,
) -> dict:
    """
    Checks port names for matches with arguments and returns mapped ports.

    Args:
        port_dict (dict): ports {uid:name,...}
        posargs (list): available positional arguments (will be modified)
        pargsDict (dict): mapped arguments (will be modified)
        keyargs (dict): keyword arguments
        check_len (int): number of of ports to be checked
        mode (str ["inputs"]): mode, used just for logging messages
        parser (function): parser function for this port

    Returns:
        dict: port arguments

    Side effect:
        modifies pargsDict
    """
    # p_name = [p["name"] for p in port_dict]
    logger.debug(
        "Using named ports to remove %s from arguments port_dict: %s, check_len: %d)",
        mode,
        port_dict,
        check_len,
    )
    logger.debug("Checking against keyargs: %s", keyargs)
    portargs = {}
    posargs = list(posargs)
    keys = list(port_dict.keys())
    logger.debug("Checking ports: %s against %s %s", keys, posargs, keyargs)
    for i in range(check_len):
        try:
            key = port_dict[keys[i]]["name"]
            value = port_dict[keys[i]]["path"]
        except KeyError:
            logger.debug("portDict: %s", port_dict)
            raise KeyError
        if value is None:
            value = ""  # make sure we are passing NULL drop events
        if key in posargs:
            if parser:
                logger.debug("Reading from port using %s", parser.__repr__())
                value = parser(port_dict[keys[i]]["drop"])
            pargsDict.update({key: value})
            logger.debug("Using %s '%s' for parg %s", mode, value, key)
            # portargs.update({key: value}) # TODO identify more permanent fix for updating portargs
            posargs.pop(posargs.index(key))
        elif key in keyargs:
            if parser:
                logger.debug("Reading from port using %s", parser.__repr__())
                value = parser(port_dict[keys[i]]["drop"])
            # if not found in appArgs we don't put them into portargs either
            # pargsDict.update({key: value})
            portargs.update({key: value})
            logger.debug("Using %s of type %s for kwarg %s", mode, type(value), key)
            _ = keyargs.pop(key)  # remove from original arg list
        else:
            logger.debug(
                "No matching argument found for %s key %s, %s, %s",
                mode,
                key,
                keyargs,
                posargs,
            )

    logger.debug("Returning kw mapped ports: %s", portargs)
    return portargs


def check_ports_dict(ports: list) -> bool:
    """
    Checks whether all ports in ports list are of type dict. This is
    for backwards compatibility.

    Args:
        ports (list):

    Returns:
        bool: True if all ports are dict, else False
    """
    # all returns true if list is empty!
    if len(ports) > 0:
        return all(isinstance(p, dict) for p in ports)
    else:
        return False


def replace_named_ports(
    iitems: dict,
    oitems: dict,
    inport_names: dict,
    outport_names: dict,
    appArgs: dict,
    argumentPrefix: str = "--",
    separator: str = " ",
    parser: callable = None,
) -> Tuple[str, str]:
    """
    Function attempts to identify CLI component arguments that match port names.

    Inputs:
        iitems: itemized input port dictionary
        oitems: itemized output port dictionary
        inport_names: dictionary of input port names (key: uid)
        outport_names: dictionary of output port names (key: uid)
        appArgs: dictionary of all arguments
        argumentPrefix: prefix for keyword arguments
        separator: character used between keyword and value
        parser: reader function for ports



    Returns:
        tuple of serialized keyword arguments and positional arguments
    """
    logger.debug(
        "iitems: %s; inport_names: %s; outport_names: %s",
        iitems,
        inport_names,
        outport_names,
    )
    inputs_dict = collections.OrderedDict()
    for uid, drop in iitems:
        inputs_dict[uid] = {
            "drop": drop,
            "path": drop.path if hasattr(drop, "path") else "",
        }

    outputs_dict = collections.OrderedDict()
    for uid, drop in oitems:
        outputs_dict[uid] = {"path": drop.path if hasattr(drop, "path") else ""}
    posargs = [arg for arg in appArgs if appArgs[arg]["positional"]]
    # get kwargs
    keyargs = {
        arg: appArgs[arg]["value"] for arg in appArgs if not appArgs[arg]["positional"]
    }
    # we will need an ordered dict for all positional arguments
    # thus we create it here and fill it with values
    portPosargsDict = collections.OrderedDict(zip(posargs, [None] * len(posargs)))
    print(portPosargsDict)
    logger.debug(
        "posargs: %s; keyargs: %s, %s",
        posargs,
        keyargs,
        check_ports_dict(inport_names),
    )
    portkeyargs = {}
    ipkeyargs = {}
    opkeyargs = {}
    if check_ports_dict(inport_names):
        for inport in inport_names:
            key = list(inport.keys())[0]
            inputs_dict[key].update({"name": inport[key]})

        ipkeyargs = identify_named_ports(
            inputs_dict,
            posargs,
            portPosargsDict,
            keyargs,
            check_len=len(iitems),
            mode="inputs",
            parser=parser,
        )
        portkeyargs.update(ipkeyargs)
    else:
        for i in range(min(len(iitems), len(posargs))):
            portkeyargs.update({list(posargs)[i]: list(iitems)[i][1]})

    if check_ports_dict(outport_names):
        for outport in outport_names:
            key = list(outport.keys())[0]
            outputs_dict[key].update({"name": outport[key]})
        opkeyargs = identify_named_ports(
            outputs_dict,
            posargs,
            portPosargsDict,
            keyargs,
            check_len=len(oitems),
            mode="outputs",
        )
        portkeyargs.update(opkeyargs)
    else:
        for i in range(min(len(oitems), len(posargs))):
            portkeyargs.update({posargs[i]: list(oitems)[i][1]})
    # now that we have the mapped ports we can cleanup the appArgs
    # and construct the final keyargs and pargs
    logger.debug(
        "Arguments from ports: %s, %s, %s, %s",
        portkeyargs,
        portPosargsDict,
        ipkeyargs,
        opkeyargs,
    )
    appArgs = clean_applicationArgs(appArgs)
    # get cleaned positional args
    posargs = {
        arg: appArgs[arg]["value"] for arg in appArgs if appArgs[arg]["positional"]
    }
    logger.debug("posargs: %s", posargs)
    # get cleaned kwargs
    keyargs = {
        arg: appArgs[arg]["value"] for arg in appArgs if not appArgs[arg]["positional"]
    }
    logger.debug("keyargs: %s", keyargs)
    for k, v in portkeyargs.items():
        if v not in [None, ""]:
            keyargs.update({k: v})
    logger.debug("keyargs: %s", keyargs)
    for k, v in portPosargsDict.items():
        logger.debug("port posarg %s has value %s", k, v)
        # logger.debug("default posarg %s has value %s", k, posargs[k])
        if k == "input_redirection":
            v = f"cat {v} > "
        if k == "output_redirection":
            v = f"> {v}"
        if v not in [None, ""]:
            posargs.update({k: v})
    keyargs = (
        serialize_kwargs(keyargs, prefix=argumentPrefix, separator=separator)
        if len(keyargs) > 0
        else [""]
    )
    pargs = list(posargs.values())
    pargs = [""] if len(pargs) == 0 or None in pargs else pargs
    logger.debug("After port replacement: pargs: %s; keyargs: %s", pargs, keyargs)
    return keyargs, pargs


def get_port_reader_function(input_parser: DropParser):
    """
    Return the function used to read input from a named port
    """
    # Inputs are un-pickled and treated as the arguments of the function
    # Their order must be preserved, so we use an OrderedDict
    if input_parser is DropParser.PICKLE:
        # all_contents = lambda x: pickle.loads(droputils.allDropContents(x))
        reader = drop_loaders.load_pickle
    elif input_parser is DropParser.EVAL:

        def optionalEval(x):
            # Null and Empty Drops will return an empty byte string
            # which should propogate back to None
            content: str = droputils.allDropContents(x).decode("utf-8")
            return ast.literal_eval(content) if len(content) > 0 else None

        reader = optionalEval
    elif input_parser is DropParser.NPY:
        reader = drop_loaders.load_npy
    elif input_parser is DropParser.PATH:
        reader = lambda x: x.path
    elif input_parser is DropParser.DATAURL:
        reader = lambda x: x.dataurl
    else:
        raise ValueError(input_parser.__repr__())
    return reader
