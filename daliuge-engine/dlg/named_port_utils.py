import ast
import logging
import collections
from dlg.data.drops.data_base import DataDROP
import dlg.droputils as droputils
from dlg.data import path_builder
import dlg.drop_loaders as drop_loaders

from dataclasses import dataclass
from enum import Enum, IntEnum, auto
from typing import Tuple, Union

from dlg.drop import AbstractDROP

logger = logging.getLogger(f"dlg.{__name__}")

class ArgType(IntEnum):
    """
    Arguments can be positional or keyword.

    This enum is used when determining what we do with the argument, as POSITIONAL
    arguments will need to be treated differently as the order in which they appear
    is important.
    """
    POSITIONAL = auto()
    KEYWORD = auto()


class DropParser(Enum):
    RAW = "raw"
    PICKLE = "pickle"
    EVAL = "eval"
    NPY = "npy"
    DILL = "dill"
    # JSON = "json"
    PATH = "path"  # input only
    DATAURL = "dataurl"  # input only
    BINARY = "binary"
    UTF8 = "utf-8"


@dataclass()
class Argument:
    """
    Encapsulate data associated with all arguments.

    There are a few things here that will always be the default,
    but it makes more sense to have the same information always true across all instances
    of an Argument, rather than create special cases for specific dictionary elements
    """

    value: object
    encoding: DropParser = DropParser.DILL
    type: Union [ArgType, None] = None
    precious: bool = False
    positional: bool = False


def serialize_kwargs(keyargs, prefix="--", separator=" "):
    kwargs = {}
    for name, value in iter(keyargs.items()):
        kwargs[name] = value
    #     if prefix == "--" and len(name) == 1:
    #         kwargs += [f"-{name} {value}"]
    #     else:
    #         kwargs += [f"{prefix.strip()}{name.strip()}{separator}{str(value).strip()}"]
    # logger.debug("kwargs after serialization: %s", kwargs)
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
    applicationArgs = clean_applicationArgs(applicationArgs)
    pargs = []
    kwargs = {}
    for name, vdict in applicationArgs.items():
        value = vdict["value"]
        positional = vdict["positional"]
        if positional:
            pargs.append(str(value).strip())
        else:
            kwargs.update({name: value})
    logger.info("Constructed command line arguments: %s %s", pargs, kwargs)
    return (pargs, kwargs)

def identify_named_ports(
    port_dict: dict,
    positionalArgs: list,
    keywordArgs: dict,
    check_len: int = 0,
    mode: str = "inputs",
    parser: callable = None,
    addPositionalToKeyword: bool = False,
) -> tuple[dict, dict]:
    """
    Checks port names for matches with arguments and returns mapped ports.

    Args:
        port_dict (dict): ports {uid:name,...}
        positionalArgs (list): available positional arguments (will be modified)
        positionalPortArgs (dict): mapped arguments (will be modified)
        keywordArgs (dict): keyword arguments
        check_len (int): number of of ports to be checked
        mode (str ["inputs"]): mode, used just for logging messages
        parser (function): parser function for this port
        addPositionalToKeyword (bool): Adds a positional argument to the keyword
            argument dictionary. This is useful when you have arguments
            that can be specified both positionally and as keywords. For example,
            in a Python function, you might have `func(a, b=2)`, where `a` can be
            passed positionally or as a keyword.

    Returns:
        keywordPortArgs: dict of keyword arguments from named ports

    Side effect:
        Modifies:
        - positionalArgs
        - positionalPortArgs
        - keywordArgs

    """
    # p_name = [p["name"] for p in port_dict]
    logger.debug(
        "Using named ports to remove %s from arguments port_dict: %s, check_len: %d)",
        mode,
        port_dict,
        check_len,
    )
    logger.debug("Checking against keyargs: %s", keywordArgs)
    keywordPortArgs = {}
    positionalPortArgs = collections.OrderedDict(positionalArgs)
    positionalArgs = list(positionalArgs)
    keys = list(port_dict.keys())
    logger.debug("Checking ports: %s against %s %s", keys, positionalArgs, keywordArgs)
    for i in range(check_len):
        try:
            key = port_dict[keys[i]]["name"]
            value = port_dict[keys[i]]["path"]
        except KeyError as e:
            logger.debug("portDict: %s does not have key: %s", port_dict, keys[i])
            raise KeyError("")
        if value is None:
            value = ""  # make sure we are passing NULL drop events
        if key in positionalArgs:
            try:
                encoding = DropParser(positionalPortArgs[key].encoding)
            except ValueError:
                logger.warning("No encoding set for %key: possible default")
                continue
            parser = get_port_reader_function(encoding)
            if parser:
                logger.debug("Reading from %s encoded port using %s", encoding, parser.__repr__())
                value = parser(port_dict[keys[i]]["drop"])
            positionalPortArgs[key].value = value
            logger.debug("Using %s '%s' for parg %s", mode, value, key)
            positionalArgs.remove(key)
            # We have positional argument that is also a keyword
            if addPositionalToKeyword:
                keywordPortArgs.update({key: positionalPortArgs[key]})
        elif key in keywordArgs:
            try:
                encoding = DropParser(keywordArgs[key].encoding)
            except ValueError:
                logger.warning("No encoding set for %key: possible default")
                continue
            parser = get_port_reader_function(encoding)
            if parser:
                logger.debug("Reading from %s encoded port using %s", encoding, parser.__repr__())
                value = parser(port_dict[keys[i]]["drop"])
            # if not found in appArgs we don't put them into portargs either
            # pargsDict.update({key: value})
            keywordArgs[key].value = value
            keywordPortArgs.update({key: keywordArgs[key]})
            logger.debug("Using %s of type %s for kwarg %s", mode, type(value), key)
            _ = keywordArgs.pop(key)  # remove from original arg list
        else:
            logger.debug(
                "No matching argument found for %s key %s, %s, %s",
                mode,
                key,
                keywordArgs,
                positionalArgs,
            )

    logger.debug("Returning kw mapped ports: %s", keywordPortArgs)
    return keywordPortArgs, positionalPortArgs


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
    logger.debug("Ports list is: %s", ports)
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
        parser: reader function for ports

    This method is focused on creating two 'sets' of arguments:
    - The arguments that are passed to the application (keyword and
    positional arguments)
    - The arguments that are passed to the application that are derived from
    the ports of the drop (keywordPort and positionalPort arguments).

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
        outputs_dict[uid] = {
            "drop": drop,
            "path": drop.path if hasattr(drop, "path") else "",
        }

    positionalArgs = _get_args(appArgs, positional=True)
    keywordArgs = _get_args(appArgs, positional=False)
    # we will need an ordered dict for all positional arguments
    # thus we create it here and fill it with values

    logger.debug(
        "posargs: %s; keyargs: %s, %s",
        positionalArgs,
        keywordArgs,
        check_ports_dict(inport_names),
    )

    # we will need an ordered dict for all positional arguments
    # thus we create it here and fill it with values
    positionalPortArgs = collections.OrderedDict(positionalArgs)
    keywordPortArgs = {}

    # Update the argument dictionaries in-place based on the port names.
    # This needs to be done for both the input ports and output ports on the drop.
    tmp_key, tmp_port = _process_port(
        inport_names,
        inputs_dict,
        positionalArgs,
        keywordArgs,
        iitems,
        parser,
        "inputs",
    )
    keywordPortArgs.update(tmp_key)
    positionalPortArgs.update(tmp_port)

    tmp_key, tmp_port =_process_port(
        outport_names,
        outputs_dict,
        positionalArgs,
        keywordArgs,
        oitems,
        parser,
        "outputs",
    )
    keywordPortArgs.update(tmp_key)
    positionalPortArgs.update(tmp_port)

    logger.debug("Arguments from ports: %s, %s,", keywordPortArgs, positionalPortArgs)

    # Clean arguments for Docker and Bash applications
    appArgs = clean_applicationArgs(appArgs)
    positionalArgs = _get_args(appArgs, positional=True)
    keywordArgs = _get_args(appArgs, positional=False)

    # Extract values from dictionaries - "encoding" etc. are irrelevant
    positionalArgs = {argstr: argument.value for argstr, argument in (
            positionalArgs.items())}
    keywordArgs = {argstr: argument.value for argstr, argument in keywordArgs.items()}
    keywordPortArgs = {
        argstr: argument.value for argstr, argument in keywordPortArgs.items()
    }
    positionalPortArgs = {argstr: argument.value for argstr, argument in (
        positionalPortArgs.items())}

    #  Construct the final keywordArguments and positionalPortArguments
    for k, v in keywordPortArgs.items():
        if v not in [None, ""]:
            keywordArgs.update({k: v})
    for k, v in positionalPortArgs.items():
        if v not in [None, ""]:
            positionalArgs.update({k: v})

    keywordArgs = serialize_kwargs(keywordArgs)
    pargs = positionalArgs

    logger.debug("After port replacement: pargs: %s; keyargs: %s", pargs, keywordArgs)
    return keywordArgs, pargs


def _process_port(
    port_names,
    ports,
    positionalArgs,
    keywordArgs,
    iitems,
    parser,
    mode,
):
    """
    For the set of port names, perform a backwards compatible update of the:

        - Keyword Arguments (application, and port name)
        - Positional Arguments (application, and port name)

    Note: This performs an IN-PLACE transformation of the dictionaries, through the
    identify_named_ports() method.
    """

    keywordPortArgs = {}
    positionalPortArgs = {}
    if check_ports_dict(port_names):
        for port in port_names:
            key = list(port.keys())[0]
            ports[key].update({"name": port[key]})
        keywordPortArgs, positionalPortArgs = identify_named_ports(
            ports,
            positionalArgs,
            keywordArgs,
            check_len=len(iitems),
            mode=mode,
            parser=parser,
        )

    else:
        for i in range(min(len(iitems), len(positionalArgs))):
            keywordPortArgs.update({list(positionalArgs)[i]: list(iitems)[i][1]})

    return keywordPortArgs, positionalPortArgs

def _get_args(appArgs, positional=False):
    """
    Separate out the arguments dependening on if we want positional or keyword style
    """
    args = {
        arg: Argument(
            value=appArgs[arg]["value"],
            encoding= appArgs[arg].get("encoding", "dill"),
            positional=positional
        )
        for arg in appArgs
        if (appArgs[arg]["positional"] == positional)
    }

    argType = "Positional" if positional else "Keyword"
    logger.debug("%s arguments: %s", argType, args)
    return args


def get_port_reader_function(input_parser: DropParser):
    """
    Return the function used to read input from a named port
    """
    # Inputs are un-pickled and treated as the arguments of the function
    # Their order must be preserved, so we use an OrderedDict
    if input_parser is DropParser.PICKLE:
        # all_contents = lambda x: pickle.loads(droputils.allDropContents(x))
        reader = drop_loaders.load_pickle
    elif input_parser is DropParser.UTF8:
        reader = drop_loaders.load_utf8
    elif input_parser is DropParser.EVAL:

        def optionalEval(x):
            # Null and Empty Drops will return an empty byte string
            # which should propogate back to None
            content: str = droputils.allDropContents(x).decode("utf-8")
            logger.debug("Read %s from %s drop.", content, input_parser)
            return ast.literal_eval(content) if len(content) > 0 else None

        reader = optionalEval
    elif input_parser is DropParser.UTF8:

        def utf8decode(drop: "DataDROP"):
            """
            Decode utf8
            Not stored in drop_loaders to avoid cyclic imports
            """
            return droputils.allDropContents(drop).decode("utf-8")

        reader = utf8decode
    elif input_parser is DropParser.NPY:
        reader = drop_loaders.load_npy
    elif input_parser is DropParser.PATH:
        def PathFromData(x: AbstractDROP):
            # Attempt to access path from DROP "x"
            # If not Path, this could be a memory Drop with path information.
            # If so, try and decode accordingly.
            try:
                return x.path
            except AttributeError:
                return drop_loaders.load_utf8(x)
        reader = PathFromData
    elif input_parser is DropParser.DATAURL:
        reader = lambda x: x.dataURL
    elif input_parser is DropParser.DILL:
        reader = drop_loaders.load_dill
    elif input_parser is DropParser.BINARY:
         reader = drop_loaders.load_binary
    else:
        raise ValueError("Invalid input parser specified: %s", input_parser.__repr__())
    return reader
