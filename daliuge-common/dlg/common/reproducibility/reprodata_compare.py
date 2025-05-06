#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2015
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#
"""
This script allows the comparison of two graphs and their related reprodata files, writing the
comparison results to csv files or the command line.
It is intended to provide a simple way to provide comparison between two or more workflow executions
"""
from datetime import datetime
import os
import pathlib
import argparse
import json
import csv
import logging
import itertools

from dlg.common.reproducibility.constants import (
    ALL_RMODES,
    rflag_caster,
    ReproducibilityFlags,
)

logger = logging.getLogger(f"dlg.{__name__}")


def _unique_filemid():
    today = datetime.now()
    return today.strftime("%d-%m-%Y-%H-%M-%S-%f")


def open_file(path: pathlib.Path):
    """
    Opens the passed filepath, returns a dictionary of the contained rmode signatures
    """
    with path.open("r", encoding="utf-8") as infile:
        data = json.load(infile)
    if isinstance(data, list):
        return data[-1]
    return data


def is_single(data):
    """
    Determines if the passed reprodata contains several signatures, or a single signature.
    """
    if data.get("rmode") == str(ReproducibilityFlags.ALL.value):
        return False
    return True


def process_single(data):
    """
    Processes reprodata containing a single signature.
    Builds a small dictionary mapping the 'rmode' to the signature
    """
    return {rflag_caster(data.get("rmode")).value: data.get("signature")}


def process_multi(data):
    """
    Processes reprodata containing multiple signatures.
    Builds a dictionary mapping rmode.value to the provided signature
    None if not present.
    """
    out_data = {rmode.value: None for rmode in ALL_RMODES}
    for rmode in ALL_RMODES:
        out_data[rmode.value] = data.get(rmode.name, {}).get("signature")
    return out_data


def process_file(filename: pathlib.Path):
    """
    Processes a reprodata file, returning a summary dictionary mapping rmode to signature for
    all rmodes.
    """
    data = open_file(filename)
    out_data = {rmode.value: None for rmode in ALL_RMODES}
    if is_single(data):
        out_data.update(process_single(data))
    else:
        out_data.update(process_multi(data))
    return out_data


def process_directory(dirname: pathlib.Path):
    """
    Processes a directory assuming to contain reprodata.out file(s) referring to the same workflow.
    """
    out_data = {}
    for file in dirname.glob("*.out"):
        new_data = process_file(file)
        for rmode, sig in new_data.items():
            if sig is not None:
                out_data[rmode] = sig
    return out_data


def generate_comparison(data):
    """
    :param: data - a dictionary mapping workflow names to rmode signatures.
    For each possible combination of workflows present in the data dictionary, compares their
    rmode signatures.
    Returns a dictionary mapping each pair to rmode booleans (true if matching, false otherwise)
    """
    outdata = {}
    for combination in itertools.combinations(data.keys(), 2):
        outdata[combination[0] + ":" + combination[1]] = compare_signatures(
            data[combination[0]], data[combination[1]]
        )
    return outdata


def compare_signatures(data1, data2):
    """
    Compares the rmode signatures of two workflow executions.
    """
    output = {rmode.value: False for rmode in ALL_RMODES}
    for rmode in ALL_RMODES:
        if rmode.value in data1 and rmode.value in data2:
            if data1[rmode.value] == data2[rmode.value]:
                output[rmode.value] = True
    return output


def write_outfile(data, outfilepath, outfilesuffix="summary", verbose=False):
    """
    Writes a dictionary to csv file.
    """
    fieldnames = ["workflow"] + [rmode.name for rmode in ALL_RMODES]
    with open(
        outfilepath + f"-{outfilesuffix}.csv", "w+", newline="", encoding="utf-8"
    ) as ofile:
        writer = csv.writer(ofile, delimiter=",")
        writer.writerow(fieldnames)

        for filepath, signature_data in data.items():
            if signature_data == {}:
                continue
            row = [filepath] + [signature_data[rmode.value] for rmode in ALL_RMODES]
            writer.writerow(row)
            if verbose:
                print(row)


def write_comparison(data, outfilepath, verbose=False):
    """
    Writes comparison dictionary to csv file.
    """
    if len(data) > 0:
        write_outfile(data, outfilepath, "comparison", verbose)


def write_outputs(data, comparisons, outfile_root=".", verbose=False):
    """
    Writes reprodata signatures for all workflows to a summary csv and comparison of these
    signatures to a separate comparison csv.
    """
    if verbose:
        print(json.dumps(data, indent=4))
    try:
        write_outfile(data, outfile_root, outfilesuffix="summary", verbose=verbose)
    except IOError:
        logger.debug("Could not write summary csv")
    try:
        write_comparison(comparisons, outfile_root, verbose)
    except IOError:
        logger.debug("Could not write to comparsion csv")


def process_logfiles(pathnames: list):
    """
    Processes all logfiles present in the list of pathnames
    """
    paths = []
    data = {}
    for pathname in pathnames:
        paths.append(pathlib.Path(pathname))
    for path in paths:
        if path.is_dir():
            data[path.name] = process_directory(path)
        elif path.is_file():
            data[path.name] = process_file(path)
        else:
            raise AttributeError(f"{path.name} is not a file or directory")

    comparisons = generate_comparison(data)
    return data, comparisons


def _main(pathnames: list, outfilepath: str, verbose=False):
    outfile_root = os.path.join(outfilepath, _unique_filemid())
    data, comparisons = process_logfiles(pathnames)
    write_outputs(data, comparisons, outfile_root, verbose)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "filename",
        action="store",
        default=None,
        nargs="+",
        type=str,
        help="The first filename or directory to access",
    )
    parser.add_argument(
        "-o",
        "--outfile",
        action="store",
        default=".",
        type=str,
        help="Directory to write output files to",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        default=False,
        action="store_true",
        help="If set, will write output to standard out",
    )
    args = parser.parse_args()
    _main(list(args.filename), args.outfile, args.verbose)
