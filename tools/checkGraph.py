#!/usr/bin/env python
"""
checkGraph.py - checks a graph file against the graph schema, reports issues
"""

import argparse
import json
import logging
import os
import sys

from jsonschema import validate, ValidationError

LG_SCHEMA_FILENAME = "../daliuge-translator/dlg/dropmake/lg.graph.schema"


def get_args():
    """
    Deal with the command line arguments

    :returns args.ifile:str
    """
    # inputdir, tag, outputfile, allow_missing_eagle_start, module_path, language
    parser = argparse.ArgumentParser(
        epilog=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("ifile", help="input file name")
    parser.add_argument(
        "-v", "--verbose", help="increase output verbosity", action="store_true"
    )
    args = parser.parse_args()
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    logger.debug("DEBUG logging switched on")
    return args.ifile


if __name__ == "__main__":
    """
    Main method

    """
    logger = logging.getLogger(f"dlg.{__name__}")
    FORMAT = "%(asctime)s [  %(filename)s  ] [  %(lineno)s  ] [  %(funcName)s  ] || %(message)s ||"
    logging.basicConfig(
        format=FORMAT,
        datefmt="%d-%b-%yT%H:%M:%S",
        level=logging.INFO,
    )

    # read command line arguments
    input_filename = get_args()
    logger.info("Input Filename:" + input_filename)

    # load graph
    with open(input_filename, "r") as file:
        graph = json.loads(file.read())

    # check graph is LG, if not, abort
    if "modelData" not in graph:
        logger.info("Not an LG")
        sys.exit(0)

    # load schema
    with open(LG_SCHEMA_FILENAME, "r") as file:
        schema = json.loads(file.read())

    # validate
    try:
        validate(graph, schema)
    except ValidationError as ve:
        error = "Validation Error {1}: {0}".format(str(ve), input_filename)
        logger.error(error)
        sys.exit(1)

    logger.info("Valid")
    sys.exit(0)
