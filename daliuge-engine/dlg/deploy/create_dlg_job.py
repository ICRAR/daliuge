#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2016
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
Generate the SLURM script, and submit it to the queue based on various paramters

parse the log result, and produce the plot

"""

import json
import optparse # pylint: disable=deprecated-module
import sys
import os

from pathlib import Path

from dlg.deploy.configs import (
    ConfigFactory,
)  # get all available configurations

from dlg.deploy.configs import (
    DEFAULT_MON_PORT,
    DEFAULT_MON_HOST,
)
from dlg.deploy.logparser import LogParser
from dlg.deploy.slurm_client import SlurmClient
from dlg.common.reproducibility.reproducibility import (
    init_pgt_unroll_repro_data,
    init_pgt_partition_repro_data,
)

from dlg.deploy.configs.config_manager import ConfigManager, ConfigType

from dlg.dropmake import pg_generator

FACILITIES = ConfigFactory.available()

def process_bool(param: str):
    """
    Evaluate the boolean value from a string, given bool('False') is True.

    :param param: The parameter string we want to verify
    :return:
    """
    if isinstance(param, bool):
        return param

    if param.lower() == 'false':
        return False
    elif param.lower() == 'true':
        return True
    else:
        raise ValueError("You have likely misspelled True/False in your .ini config")

def process_config(config_file: str):
    """
    Use configparser to process INI file

    Current functionality: 
        - Returns remote environment config (e.g. DLG_ROOT, HOME etc.)

    Future Functionality: 
        - Graph translation parameters
        - Engine parameters

    :returns: dict, config information
    """
    from configparser import ConfigParser, ExtendedInterpolation
    parser = ConfigParser(interpolation=ExtendedInterpolation())
    all_opts = {}
    parser.read(config_file)
    for s in parser.sections():
        all_opts.update(dict(parser[s]))
    all_opts = {key: value for key, value in all_opts.items() if value}
    return all_opts

def process_slurm_template(template_file: str):
    template = Path(template_file)
    with template.open('r') as fp:
        return fp.read()

def create_engine_group(parser: optparse.OptionParser):
    """
    Setup optparse group for all DALiuGE Engine options.

    This includes number of Node and Data-Island managers , logging levels, and
    verbosity settings.

    :param parser:
    :return: parser
    """
    group = optparse.OptionGroup(parser, "Engine options",
                                 "DALiuGE engine configuration and runtime options")


    group.add_option(
        "-l",
        "--log-root",
        action="store",
        dest="log_root",
        help="The root directory of the log file",
    )

    group.add_option(
        "-d",
        "--log-dir",
        action="store",
        dest="log_dir",
        help="The directory of the log file for parsing",
    )

    group.add_option(
        # TODO: num_nodes needs to be >= #partitions in PGT, if provided
        "-n",
        "--num_nodes",
        action="store",
        type="int",
        dest="num_nodes",
        help="Number of compute nodes requested",
    )

    group.add_option(
        "-s",
        "--num_islands",
        action="store",
        type="int",
        dest="num_islands",
        help="The number of Data Islands",
    )

    group.add_option(
        "-u",
        "--all_nics",
        action="store_true",
        dest="all_nics",
        help="Listen on all NICs for a node manager",
        default=False,
    )

    group.add_option(
        "-S",
        "--check_with_session",
        action="store_true",
        dest="check_with_session",
        help="Check for node managers' availability by creating/destroy a session",
        default=False,
    )

    group.add_option(
        "-D",
        "--dlg_root",
        dest="dlg_root",
        action="store",
        type="string",
        help="Overwrite the DLG_ROOT directory provided by the config",
    )

    group.add_option(
        "-v",
        "--verbose-level",
        action="store",
        type="int",
        dest="verbose_level",
        help="Verbosity level (1-3) of the DIM/NM logging",
        default=1,
    )

    return group

def create_slurm_group(parser: optparse.OptionParser):
    group = optparse.OptionGroup(parser, "Slurm options",
                                 "Slurm job script options. "
                                 "Note: These will be overwritten if using "
                                 "--slurm_template")
    group.add_option(
        "-T",
    "--max-threads",
    action="store",
    type="int",
    dest="max_threads",
    help="Max thread pool size used for executing drops. 0 (default) means no pool.",
    default=0,
    )

    group.add_option(
    "-f",
    "--facility",
    dest="facility",
    choices=FACILITIES,
    action="store",
    help=f"The facility for which to create a submission job\nValid options: {FACILITIES}",
    default=None,
    )

    group.add_option(
        "-t",
        "--job-dur",
        action="store",
        type="int",
        dest="job_dur",
        help="job duration in minutes",
        default=30,
    )

    return group

def create_experiment_group(parser: optparse.OptionParser):
    """
    Establish experiment group to separate out experimenatal options

    :param parser: parser that we are updating
    :return: the group for experiments
    """
    group=optparse.OptionGroup(parser, "Experimental Options",
                      "Caution: These are not properly tested and likely to"
                      "be rough around the edges.")

    group.add_option(
        "--config_file",
        dest="config_file",
        type="string",
        action="store",
        help="Use INI configuration file.",
        default=None
    )
    group.add_option(
        "--slurm_template",
        dest="slurm_template",
        type="string",
        action="store",
        help="Use SLURM template file for job submission. WARNING: Using this command will over-write other job-parameters passed here.",
        default=None
    )

    return group

def create_local_graph_group(parser):
    """
    TODO: LIU-424
    """
    group = optparse.OptionGroup(parser, "Local graph options",
                      "Options for locally stored graphs")
    group.add_option(
        "-L",
        "--logical-graph",
        action="store",
        type="string",
        dest="logical_graph",
        help="The filename of the logical graph to deploy",
        default=None,
    )

    group.add_option(
        "-P",
        "--physical-graph",
        action="store",
        type="string",
        dest="physical_graph",
        help="The filename of the physical graph (template) to deploy",
        default=None,
    )

    return group


def create_remote_graph_group(parser):
    """
    TODO: LIU-424
    """
    group = optparse.OptionGroup(parser, "Remote graph options",
                      "Options for graphs stored in remote repositories. "
                               "Currently supported: GitHub, GitLab")

    group.add_option(
        "--github",
        action="store_true",
        dest="github",
        help="Access graph from remote repository",
        default=False,)

    group.add_option("--gitlab",
        action="store_true",
        dest="gitlab",
        help="Access graph from remote repository",
        default=False,)

    group.add_option("--user_org")
    group.add_option("--repo")
    group.add_option("--branch")
    group.add_option("--path")

    return group


def evaluate_graph_options(opts, parser):
    """
    Perform checks on the graph options to ensure mutually exclusive information is not
    stored.
    :param opts:
    :return:
    """

    from dlg.deploy.remote_graph import github_request, gitlab_request

    graph = None
    use_remote_graph = opts.github or opts.gitlab
    use_local_graph = opts.logical_graph or opts.physical_graph

    if use_local_graph and use_remote_graph:
        parser.error("Cannot specify both local and remote graph")

    if use_local_graph:
        if opts.logical_graph:
            return _translate_graph(parser, opts.logical_graph)
        if opts.physical_graph:
            return opts.physical_graph


    if opts.github:
        content = github_request(opts.user_org, opts.repo, opts.branch, opts.path)
        return _translate_graph(parser, opts, content)
    if opts.gitlab:
        content = gitlab_request(opts.user_org, opts.repo, opts.branch, opts.path)
        return _translate_graph(parser, opts, content)

    return graph


def create_monitor_options(parser):
    group = optparse.OptionGroup(parser, "Monitor proxy options",
                                 "Start and configure the monitoring proxy.")


    group.add_option(
        "-p",
        "--run_proxy",
        action="store_true",
        dest="run_proxy",
        help="Whether to attach proxy server for real-time monitoring",
        default=False,
    )
    group.add_option(
        "-m",
        "--monitor_host",
        action="store",
        type="string",
        dest="mon_host",
        help="Monitor host IP (optional)",
        default=DEFAULT_MON_HOST,
    )
    group.add_option(
        "-o",
        "--monitor_port",
        action="store",
        type="int",
        dest="mon_port",
        help="The port to bind DALiuGE monitor",
        default=DEFAULT_MON_PORT,
    )

    group.add_option(
        "-i",
        "--visualise_graph",
        action="store_true",
        dest="visualise_graph",
        help="Whether to visualise graph (poll status)",
        default=False,
    )
    return group


def submit_graph(opts):
    """
    Run submission procedure for graph
    :param opts:
    :return:
    """

def analyse_graph(opts):
    """
    Perform analysis of graph using the LogAnalyser
    :param opts:
    :return:
    """

def create_component_options(parser):
    group = optparse.OptionGroup(parser, "Graph Component options",
                                 "Update component DROPs for testing.")

    group.add_option(
        "-z",
        "--zerorun",
        action="store_true",
        dest="zerorun",
        help="Generate a physical graph that takes no time to run",
        default=False,
    )
    group.add_option(
        "-y",
        "--sleepncopy",
        action="store_true",
        dest="sleepncopy",
        help="Whether include COPY in the default Component drop",
        default=False,
    )
    return group

def _translate_graph(parser, opts, content=""):
    if opts.logical_graph:
        graph_file = os.path.basename(opts.logical_graph)
    else:
        graph_file = opts.path.split("/")[-1]
    pre, ext = os.path.splitext(graph_file)
    if os.path.splitext(pre)[-1] != ".pre":
        pgt_file = ".".join([pre, "pgt", ext[1:]])
    else:
        pgt_file = graph_file

    lg_graph = {}
    if opts.logical_graph:
        with open(opts.logical_graph) as f:
            # logical graph provided, translate first
            lg_graph = json.loads(f.read())
    elif content:
        lg_graph = json.loads(content)

    else:
        parser.error("Incorrect configuration, no graph available to translate")
        sys.exit(1)
    pgt = pg_generator.unroll(lg_graph, zerorun=opts.zerorun)
    pgt = init_pgt_unroll_repro_data(pgt)
    reprodata = pgt.pop()
    pgt = pg_generator.partition(
        pgt=pgt,
        algo=opts.algorithm,
        algo_params=opts.algorithm_params,
        num_islands=int(opts.num_islands),
        num_partitions=int(opts.num_nodes),
    )
    pgt.append(reprodata)
    pgt = init_pgt_partition_repro_data(pgt)
    pgt_name = pgt_file
    pgt_path = Path(f"/tmp/{pgt_file}")
    with pgt_path.open("w") as o:
        json.dump((pgt_name, pgt), o)
        return str(pgt_path)


def run(_, args):
    parser = optparse.OptionParser(
        usage="\n%prog --action [submit|analyse] -f <facility> [options]\n\n%prog -h for further help"
    )

    parser.add_option(
        "-a",
        "--action",
        action="store",
        type="choice",
        choices=["submit", "analyse"],
        dest="action",
        help="**submit** job or **analyse** log",
    )

    # Algorithm options
    parser.add_option(
        "-A",
        "--algorithm",
        action="store",
        type="string",
        dest="algorithm",
        help="The algorithm to be used for the translation",
        default="metis",
    )
    parser.add_option(
        "-O",
        "--algorithm-parameters",
        action="store",
        type="string",
        dest="algorithm_params",
        help="Parameters for the translation algorithm",
    )
    parser.add_option(
        "--submit",
        dest="submit",
        action="store_true",
        help="If set to False, the job is not submitted, but the script is generated",
        default=False,
    )

    parser.add_option(
        "--remote",
        dest="remote",
        action="store_true",
        help="If set to True, the job is submitted/created for a remote submission",
        default=False,
    )
    # Engine options
    parser.add_option_group(create_engine_group(parser))
    parser.add_option_group(create_slurm_group(parser))
    parser.add_option_group(create_monitor_options(parser))
    parser.add_option_group(create_component_options(parser))
    parser.add_option_group(create_remote_graph_group(parser))
    parser.add_option_group(create_local_graph_group(parser))
    parser.add_option_group(create_experiment_group(parser))

    parser.add_option(
        "-c",
        "--csvoutput",
        action="store",
        dest="csv_output",
        help="CSV output file to keep the log analysis result",
    )

    # SSH options
    parser.add_option(
        "-U",
        "--username",
        dest="username",
        type="string",
        action="store",
        help="Remote username, if different from local",
        default=None,
    )

    parser.add_option(
        "--ssh_key",
        action="store",
        help="Path to ssh private key",
        default=None
    )



    (opts, _) = parser.parse_args(sys.argv)
    if len(sys.argv) <=1:
        parser.print_help()
        sys.exit(0)
    cfg_manager = ConfigManager(FACILITIES)

    facility_necessary = True if not opts.config_file else False
    if opts.config_file:
        cfg = process_config(opts.config_file)
        for attr, val in cfg.items():
            setattr(opts, attr, val)

    # graph_name = evaluate_graph_options(opts)
    # sys.exit(0)

    if not opts.action and (facility_necessary and not opts.facility):
        parser.error("Missing required parameters!")
        parser.print_help()
        sys.exit(1)
    if facility_necessary and opts.facility not in FACILITIES:
        parser.error(f"Unknown facility provided. Please choose from {FACILITIES}")
        sys.exit(1)

    if opts.github and opts.gitlab:
        parser.error("Must specify only one of --github or gitlab")

    if opts.action == "analyse":
        if opts.log_dir is None:
            # you can specify:
            # either a single directory
            if opts.log_root is None:
                config = ConfigFactory.create_config(
                    facility=opts.facility, user=opts.username
                )
                log_root = config.getpar("log_root")
            else:
                log_root = opts.log_root
            if log_root is None or (not os.path.exists(log_root)):
                parser.error(
                    "Missing or invalid log directory/facility for log analysis"
                )
            # or a root log directory
            else:
                for log_dir in os.listdir(log_root):
                    log_dir = os.path.join(log_root, log_dir)
                    if os.path.isdir(log_dir):
                        try:
                            log_parser = LogParser(log_dir)
                            log_parser.parse(out_csv=opts.csv_output)
                        except Exception as exp: # pylint: disable=broad-exception-caught
                            parser.error(
                                "Fail to parse {0}: {1}".format(log_dir, exp))
        else:
            log_parser = LogParser(opts.log_dir)
            log_parser.parse(out_csv=opts.csv_output)
    elif opts.action == "submit":
        pgt_file = evaluate_graph_options(opts, parser)

        if opts.config_file:
            config_path = cfg_manager.load_user_config(ConfigType.ENV, opts.config_file)
            if not config_path:
                parser.error("Provided --config_file option that does not exist!")
                sys.exit(1)
            config = process_config(config_path) if config_path else None
        else:
            config = None
        if opts.slurm_template:
            template_path = cfg_manager.load_user_config(ConfigType.SLURM, opts.slurm_template)
            if not template_path:
                parser.error("Provided --slurm_template option that does not exist!")
                sys.exit(1)
            template = process_slurm_template(template_path)  if template_path else None
        else:
            template = None

        client = SlurmClient(
            dlg_root=opts.dlg_root,
            facility=opts.facility,
            job_dur=int(opts.job_dur),
            num_nodes=int(opts.num_nodes),
            logv=int(opts.verbose_level),
            zerorun=opts.zerorun,
            max_threads=int(opts.max_threads),
            run_proxy=opts.run_proxy,
            mon_host=opts.mon_host,
            mon_port=opts.mon_port,
            num_islands=int(opts.num_islands),
            all_nics=process_bool(opts.all_nics),
            check_with_session=opts.check_with_session,
            physical_graph_template_file=pgt_file,
            submit=process_bool(opts.submit),
            remote=process_bool(opts.remote),
            username=opts.username,
            ssh_key=opts.ssh_key,
            config=config,
            slurm_template=template
        )

        client.visualise_graph = opts.visualise_graph
        client.submit_job()
    else:
        parser.print_help()
        parser.error(f"Invalid input from args: {args}!")


if __name__ == "__main__":
    run(None, sys.argv[1:])
