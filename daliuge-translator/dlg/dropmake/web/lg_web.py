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
#    chen.wu@icrar.org

import datetime
import json
import logging
import optparse
import os
import signal
import sys
import threading
import time
import traceback
import warnings
from json import JSONDecodeError
from urllib.parse import parse_qs, urlparse

import bottle
import pkg_resources
from bottle import (
    route,
    request,
    get,
    post,
    static_file,
    template,
    redirect,
    response,
    HTTPResponse,
)
from dlg import common, restutils
from dlg.clients import CompositeManagerClient
from dlg.common.reproducibility.constants import REPRO_DEFAULT
from dlg.common.reproducibility.reproducibility import (
    init_lgt_repro_data,
    init_lg_repro_data,
    init_pgt_unroll_repro_data,
    init_pgt_partition_repro_data,
)
from dlg.dropmake.lg import GraphException, load_lg
from dlg.dropmake.pg_generator import unroll, partition
from dlg.dropmake.pg_manager import PGManager
from dlg.dropmake.scheduler import SchedulerException
from jsonschema import validate, ValidationError

logger = logging.getLogger(__name__)

# Patched to be larger to accomodate large config drops
bottle.BaseRequest.MEMFILE_MAX = 1024 * 512


def file_as_string(fname, package=__name__, enc="utf8"):
    b = pkg_resources.resource_string(package, fname)  # @UndefinedVariable
    return common.b2s(b, enc)


# lg_dir = None
post_sem = threading.Semaphore(1)
gen_pgt_sem = threading.Semaphore(1)

err_prefix = "[Error]"
MAX_PGT_FN_CNT = 300
pgt_fn_count = 0

ALGO_PARAMS = [
    ("min_goal", int),
    ("ptype", int),
    ("max_load_imb", int),
    ("max_cpu", int),
    ("time_greedy", float),
    ("deadline", int),
    ("topk", int),
    ("swarm_size", int),
    ("max_mem", int),
]  # max_mem is only relevant for the old editor, not used in EAGLE


LG_SCHEMA = json.loads(file_as_string("lg.graph.schema", package="dlg.dropmake"))


def lg_path(lg_name):
    return "{0}/{1}".format(lg_dir, lg_name)


def lg_exists(lg_name):
    return os.path.exists(lg_path(lg_name))


def pgt_path(pgt_name):
    return "{0}/{1}".format(pgt_dir, pgt_name)


def pgt_exists(pgt_name):
    return os.path.exists(pgt_path(pgt_name))


def lg_repo_contents():
    return _repo_contents(lg_dir)


def pgt_repo_contents():
    return _repo_contents(pgt_dir)


def _repo_contents(root_dir):
    # We currently allow only one depth level
    b = os.path.basename
    contents = {}
    for dirpath, dirnames, fnames in os.walk(root_dir):
        if ".git" in dirnames:
            dirnames.remove(".git")
        if dirpath == root_dir:
            continue

        # Not great yet -- we should do a full second step pruning branches
        # of the tree that are empty
        files = [f for f in fnames if f.endswith(".graph")]
        if files:
            contents[b(dirpath)] = files

    return contents


@route("/static/<filepath:path>")
def server_static(filepath):
    staticRoot = pkg_resources.resource_filename(
        __name__, resource_name="."
    )  # @UndefinedVariable
    return static_file(filepath, root=staticRoot)


@route("/jsonbody", method="POST")
def jsonbody_post():
    """
    Post graph JSON representation to LG or PG manager
    """
    # see the table in http://bottlepy.org/docs/dev/tutorial.html#html-form-handling
    lg_name = request.forms["lg_name"]
    if lg_exists(lg_name):
        rmode = request.forms.get("rmode", str(REPRO_DEFAULT.value))
        lg_content = request.forms["lg_content"]
        try:
            lg_content = json.loads(lg_content)
        except JSONDecodeError:
            logger.warning(f"Could not decode lgt {lg_content}")
        lg_content = init_lg_repro_data(init_lgt_repro_data(lg_content, rmode))
        lg_path = "{0}/{1}".format(lg_dir, lg_name)
        post_sem.acquire()
        try:
            # overwrite file on disks
            # print "writing to {0}".format(lg_path)
            with open(lg_path, "w") as f:
                if isinstance(lg_content, dict):
                    lg_content = json.dumps(lg_content)
                f.write(lg_content)
        except Exception as excmd2:
            response.status = 500
            return "Fail to save logical graph {0}:{1}".format(lg_name, str(excmd2))
        finally:
            post_sem.release()
    else:
        response.status = 404
        return "{0}: logical graph {1} not found\n".format(err_prefix, lg_name)


@get("/jsonbody")
def jsonbody_get():
    """
    Return JSON representation of the logical graph
    """
    # print "get jsonbody is called"
    lg_name = request.query.get("lg_name")
    if lg_name is None or len(lg_name) == 0:
        all_lgs = lg_repo_contents()
        try:
            first_dir = next(iter(all_lgs))
            first_lg = first_dir + "/" + all_lgs[first_dir][0]
            lg_name = first_lg
        except StopIteration:
            return "Nothing found in dir {0}\n".format(lg_path)
            lg_name = None

    if lg_exists(lg_name):
        # print "Loading {0}".format(lg_name)
        lgp = lg_path(lg_name)
        with open(lgp, "r") as f:
            data = f.read()
        return data
    else:
        response.status = 404
        return "{0}: JSON graph {1} not found\n".format(err_prefix, lg_name)


@get("/pgt_jsonbody")
def pgtjsonbody_get():
    """
    Return JSON representation of the logical graph
    """
    # print "get jsonbody is called"
    pgt_name = request.query.get("pgt_name")
    if pgt_exists(pgt_name):
        # print "Loading {0}".format(lg_name)
        pgt = pgt_path(pgt_name)
        with open(pgt, "r") as f:
            data = f.read()
        return data
    else:
        response.status = 404
        return "{0}: JSON graph {1} not found\n".format(err_prefix, pgt_name)


@get("/pg_viewer")
def load_pg_viewer():
    """
    RESTful interface for loading the Physical Graph Viewer
    """
    pgt_name = request.query.get("pgt_view_name")
    if pgt_name is None or len(pgt_name) == 0:
        all_pgts = pgt_repo_contents()
        # print(all_pgts)
        try:
            first_dir = next(iter(all_pgts))
            pgt_name = first_dir + "/" + all_pgts[first_dir][0]
        except StopIteration:
            pgt_name = None
    if pgt_exists(pgt_name):
        tpl = file_as_string("pg_viewer.html")
        return template(
            tpl,
            pgt_view_json_name=pgt_name,
            title="Physical Graph Template",
            partition_info="",
            error=None
        )
    else:
        response.status = 404
        return "{0}: physical graph template (view) {1} not found {2}\n".format(
            err_prefix, pgt_name, pgt_dir
        )


@get("/show_gantt_chart")
def show_gantt_chart():
    """
    Restful interface to show the gantt chart
    """
    pgt_id = request.query.get("pgt_id")
    tpl = file_as_string("matrix_vis.html")
    return template(tpl, pgt_view_json_name=pgt_id, vis_action="pgt_gantt_chart")


@get("/pgt_gantt_chart")
def get_gantt_chart():
    """
    RESTful interface to retrieve a Gantt Chart matrix associated with a PGT
    """
    pgt_id = request.query.get("pgt_id")
    try:
        ret = pg_mgr.get_gantt_chart(pgt_id)
        return ret
    except GraphException as ge:
        response.status = 500
        return "Failt to get Gantt chart for {0}: {1}".format(pgt_id, ge)


@get("/show_schedule_mat")
def show_schedule_mat():
    """
    Restful interface to show the gantt chart
    """
    pgt_id = request.query.get("pgt_id")
    tpl = file_as_string("matrix_vis.html")
    return template(tpl, pgt_view_json_name=pgt_id, vis_action="pgt_schedule_mat")


@get("/pgt_schedule_mat")
def get_schedule_mat():
    """
    RESTful interface to retrieve a list of schedule matrices
    associated with a PGT
    """
    pgt_id = request.query.get("pgt_id")
    try:
        ret = pg_mgr.get_schedule_matrices(pgt_id)
        return ret
    except GraphException as ge:
        response.status = "500 {0}".format(ge)
        return "Failt to get schedule matrices for {0}: {1}".format(pgt_id, ge)
    except Exception as ex:
        response.status = "500 {0}".format(ex)
        return "Failed to get schedule matrices for {0}: {1}".format(pgt_id, ex)


@get("/gen_pg_helm")
def gen_pg_helm():
    """
    RESTful interface to deploy a PGT as a K8s helm chart.
    """
    # Get pgt_data
    from ...deploy.start_helm_cluster import start_helm

    pgt_id = request.query.get("pgt_id")
    pgtp = pg_mgr.get_pgt(pgt_id)
    if pgtp is None:
        response.status = 404
        return "PGT(P) with id {0} not found in the Physical Graph Manager".format(
            pgt_id
        )

    pgtpj = pgtp._gojs_json_obj
    logger.info("PGTP: %s", pgtpj)
    num_partitions = len(list(filter(lambda n: "isGroup" in n, pgtpj["nodeDataArray"])))
    # Send pgt_data to helm_start
    try:
        start_helm(pgtp, num_partitions, pgt_dir)
    except restutils.RestClientException as ex:
        response.status = 500
        print(traceback.format_exc())
        return "Fail to deploy physical graph: {0}".format(ex)
    # TODO: Not sure what to redirect to yet
    response.status = 200
    return "Inspect your k8s dashboard for deployment status"


@get("/gen_pg")
def gen_pg():
    """
    RESTful interface to convert a PGT(P) into PG by mapping
    PGT(P) onto a given set of available resources
    """
    # if the 'deploy' checkbox is not checked, then the form submission will NOT contain a 'dlg_mgr_deploy' field
    deploy = request.query.get("dlg_mgr_deploy") is not None
    mprefix = ""
    pgt_id = request.query.get("pgt_id")
    pgtp = pg_mgr.get_pgt(pgt_id)
    if pgtp is None:
        response.status = 404
        return "PGT(P) with id {0} not found in the Physical Graph Manager".format(
            pgt_id
        )

    pgtpj = pgtp._gojs_json_obj
    reprodata = pgtp.reprodata
    logger.info("PGTP: %s", pgtpj)
    num_partitions = 0
    num_partitions = len(list(filter(lambda n: "isGroup" in n, pgtpj["nodeDataArray"])))
    surl = urlparse(request.url)

    mhost = ""
    mport = -1
    mprefix = ""
    q = parse_qs(surl.query)
    if "dlg_mgr_url" in q:
        murl = q["dlg_mgr_url"][0]
        mparse = urlparse(murl)
        try:
            (mhost, mport) = mparse.netloc.split(":")
            mport = int(mport)
        except:
            mhost = mparse.netloc
            if mparse.scheme == "http":
                mport = 80
            elif mparse.scheme == "https":
                mport = 443
        mprefix = mparse.path
        if mprefix.endswith("/"):
            mprefix = mprefix[:-1]
    else:
        mhost = request.query.get("dlg_mgr_host")
        if request.query.get("dlg_mgr_port"):
            mport = int(request.query.get("dlg_mgr_port"))

    logger.debug("Manager host: %s", mhost)
    logger.debug("Manager port: %s", mport)
    logger.debug("Manager prefix: %s", mprefix)

    if mhost is None:
        if request.query.get("tpl_nodes_len"):
            nnodes = int(request.query.get("tpl_nodes_len"))
            nnodes = num_partitions
        else:
            response.status = 500
            return "Must specify DALiUGE manager host or tpl_nodes_len"

        pg_spec = pgtp.to_pg_spec([], ret_str=False, tpl_nodes_len=nnodes)
        pg_spec.append(reprodata)
        response.content_type = "application/json"
        response.set_header(
            "Content-Disposition", 'attachment; filename="%s"' % (pgt_id)
        )
        return json.dumps(pg_spec)
    try:
        mgr_client = CompositeManagerClient(
            host=mhost, port=mport, url_prefix=mprefix, timeout=30
        )
        # 1. get a list of nodes
        node_list = mgr_client.nodes()
        # 2. mapping PGTP to resources (node list)
        pg_spec = pgtp.to_pg_spec(node_list, ret_str=False)

        if deploy:
            dt = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S.%f")
            ssid = "{0}_{1}".format(
                pgt_id.split(".graph")[0].split("_pgt")[0].split("/")[-1], dt
            )
            mgr_client.create_session(ssid)
            # print "session created"
            completed_uids = common.get_roots(pg_spec)
            pg_spec.append(reprodata)
            mgr_client.append_graph(ssid, pg_spec)
            # print "graph appended"
            mgr_client.deploy_session(ssid, completed_uids=completed_uids)
            # mgr_client.deploy_session(ssid, completed_uids=[])
            # print "session deployed"
            # 3. redirect to the master drop manager
            redirect(
                "http://{0}:{1}{2}/session?sessionId={3}".format(
                    mhost, mport, mprefix, ssid
                )
            )
        else:
            response.content_type = "application/json"
            return json.dumps(pg_spec)
    except restutils.RestClientException as re:
        response.status = 500
        return "Fail to interact with DALiUGE Drop Manager: {0}".format(re)
    except HTTPResponse:
        raise
    except Exception as ex:
        response.status = 500
        print(traceback.format_exc())
        return "Fail to deploy physical graph: {0}".format(ex)


@post("/gen_pg_spec")
def gen_pg_spec():
    """
    RESTful interface to convert a PGT(P) into pg_spec
    """
    try:
        pgt_id = request.json.get("pgt_id")
        node_list = request.json.get("node_list")
        manager_host = request.json.get("manager_host")
        if manager_host == "localhost":
            manager_host = "127.0.0.1"
        # try:
        #     manager_port   = int(request.json.get("manager_port"));
        # except:
        #     manager_port = None
        # manager_prefix = request.json.get("manager_prefix");
        print("pgt_id:" + str(pgt_id))
        print("node_list:" + str(node_list))
        # print("manager_port:" + str(manager_port))
        # print("manager_prefix:" + str(manager_prefix))
    except Exception as ex:
        response.status = 500
        print(traceback.format_exc())
        return "Unable to parse json body of request for pg_spec: {0}".format(ex)

    pgtp = pg_mgr.get_pgt(pgt_id)
    if pgtp is None:
        response.status = 404
        return "PGT(P) with id {0} not found in the Physical Graph Manager".format(
            pgt_id
        )

    if node_list is None:
        response.status = 500
        return "Must specify DALiUGE nodes list"
    try:
        # mgr_client = CompositeManagerClient(host=manager_host, port=manager_port, url_prefix=manager_prefix, timeout=30)
        # # 1. get a list of nodes
        # node_list = mgr_client.nodes()
        # # 2. mapping PGTP to resources (node list)
        pg_spec = pgtp.to_pg_spec([manager_host] + node_list, ret_str=False)
        # 3. get list of root nodes
        root_uids = common.get_roots(pg_spec)
        response.content_type = "application/json"
        return json.dumps({"pg_spec": pg_spec, "root_uids": list(root_uids)})
    except Exception as ex:
        response.status = 500
        print(traceback.format_exc())
        return "Fail to generate pg_spec: {0}".format(ex)


def prepare_lgt(filename, rmode: str):
    return init_lg_repro_data(init_lgt_repro_data(load_lg(filename), rmode))


@get("/gen_pgt")
def gen_pgt():
    """
    RESTful interface for translating Logical Graphs to Physical Graphs
    """

    query = request.query
    lg_name = query.get("lg_name")
    # Retrieve rmode value
    rmode = query.get("rmode", str(REPRO_DEFAULT.value))
    if not lg_exists(lg_name):
        response.status = 404
        return "{0}: logical graph {1} not found\n".format(err_prefix, lg_name)

    try:
        lgt = prepare_lgt(lg_path(lg_name), rmode)
        # LG -> PGT
        pgt = unroll_and_partition_with_params(lgt, query)

        num_partitions = 0  # pgt._num_parts;

        pgt_id = pg_mgr.add_pgt(pgt, lg_name)

        part_info = " - ".join(
            ["{0}:{1}".format(k, v) for k, v in pgt.result().items()]
        )
        tpl = file_as_string("pg_viewer.html")
        return template(
            tpl,
            pgt_view_json_name=pgt_id,
            partition_info=part_info,
            title="Physical Graph Template%s"
            % ("" if num_partitions == 0 else "Partitioning"),
            error=None
        )
    except GraphException as ge:
        response.status = 500
        return "Invalid Logical Graph {1}: {0}".format(str(ge), lg_name)
    except SchedulerException as se:
        response.status = 500
        return "Graph scheduling exception {1}: {0}".format(str(se), lg_name)
    except Exception:
        response.status = 500
        trace_msg = traceback.format_exc()
        return "Graph partition exception {1}: {0}".format(trace_msg, lg_name)


@route("/gen_pgt", method="POST")
def gen_pgt_post():
    """
    Translating Logical Graphs to Physical Graphs.
    Differs from get_pgt above by the fact that the logical graph data is POSTed
    to this route in a HTTP form, whereas gen_pgt loads the logical graph data
    from a local file
    """
    # Retrieve the graph name.
    reqform = request.forms
    lg_name = reqform.get("lg_name")

    # Retrieve rmode value
    rmode = reqform.get("rmode", str(REPRO_DEFAULT.value))
    # Retrieve json data.
    json_string = reqform.get("json_data")
    try:
        logical_graph = json.loads(json_string)
        error = None

        # validate LG against schema
        try:
            validate(logical_graph, LG_SCHEMA)
        except ValidationError as ve:
            error = "Validation Error {1}: {0}".format(str(ve), lg_name)

        logical_graph = prepare_lgt(logical_graph, rmode)
        # LG -> PGT
        pgt = unroll_and_partition_with_params(logical_graph, reqform)
        par_algo = reqform.get("algo", "none")
        pgt_id = pg_mgr.add_pgt(pgt, lg_name)

        part_info = " - ".join(
            ["{0}:{1}".format(k, v) for k, v in pgt.result().items()]
        )
        tpl = file_as_string("pg_viewer.html")
        return template(
            tpl,
            pgt_view_json_name=pgt_id,
            partition_info=part_info,
            title="Physical Graph Template {}".format(
                "" if par_algo == "none" else "Partitioning"
            ),
            error=error
        )
    except GraphException as ge:
        trace_msg = traceback.format_exc()
        print(trace_msg)
        return "Invalid Logical Graph {1}: {0}".format(str(ge), lg_name)
    except SchedulerException as se:
        return "Graph scheduling exception {1}: {0}".format(str(se), lg_name)
    except Exception:
        trace_msg = traceback.format_exc()
        print(trace_msg)
        raise
        # return "Graph partition exception {1}: {0}".format(trace_msg, lg_name)


def unroll_and_partition_with_params(lg, algo_params_source):
    # Get the 'test' parameter
    # NB: the test parameter is a string, so convert to boolean
    test = algo_params_source.get("test", "false")
    test = test.lower() == "true"

    # Based on 'test' parameter, decide whether to use a replacement app
    app = "dlg.apps.simple.SleepApp" if test else None

    # Unrolling LG to PGT.
    pgt = init_pgt_unroll_repro_data(unroll(lg, app=app))
    # Define partitioning parameters.
    algo = algo_params_source.get("algo", "none")
    num_partitions = algo_params_source.get("num_par", default=1, type=int)
    num_islands = algo_params_source.get("num_islands", default=0, type=int)
    par_label = algo_params_source.get("par_label", "Partition")

    # Build a map with extra parameters, more specific to some par_algoithms.
    algo_params = {}
    for name, typ in ALGO_PARAMS:
        if name in algo_params_source:
            algo_params[name] = algo_params_source.get(name, type=typ)
    reprodata = pgt.pop()
    # Partition the PGT
    pgt = partition(
        pgt,
        algo=algo,
        num_partitions=num_partitions,
        num_islands=num_islands,
        partition_label=par_label,
        show_gojs=True,
        **algo_params,
    )

    pgt_spec = pgt.to_pg_spec(
        [],
        ret_str=False,
        num_islands=num_islands,
        tpl_nodes_len=num_partitions + num_islands,
    )
    pgt_spec.append(reprodata)
    init_pgt_partition_repro_data(pgt_spec)
    reprodata = pgt_spec.pop()
    pgt.reprodata = reprodata
    logger.info(reprodata)
    return pgt


def save(lg_name, logical_graph):
    """
    Saves graph.
    """
    try:
        new_path = os.path.join(lg_dir, lg_name)

        # Overwrite file on disks.
        with open(new_path, "w") as outfile:
            json.dump(logical_graph, outfile, sort_keys=True, indent=4)
    except Exception as exp:
        raise GraphException(
            "Failed to save a pretranslated graph {0}:{1}".format(lg_name, str(exp))
        )
    finally:
        pass

    return new_path


@get("/")
@get("/")
def root():
    tpl = file_as_string("pg_viewer.html")
    return template(
        tpl,
        pgt_view_json_name=None,
        partition_info=None,
        title="Physical Graph Template",
        error=None
    )


def run(parser, args):
    warnings.warn(
        "Running the translator from daliuge is deprecated", DeprecationWarning
    )
    epilog = """
If you have no Logical Graphs yet and want to see some you can grab a copy
of those maintained at:

https://github.com/ICRAR/daliuge-logical-graphs

"""

    class NoFormattedEpilogParser(optparse.OptionParser):
        def format_epilog(self, formatter):
            return self.epilog

    # Ignore the previous parser, it's only passed down for convenience
    # and to avoid duplicate descriptions (which in this case we'll have)
    parser = NoFormattedEpilogParser(
        description="A Web server for the Logical Graph Editor", epilog=epilog
    )
    parser.add_option(
        "-d",
        "--lgdir",
        action="store",
        type="string",
        dest="lg_path",
        help="A path that contains at least one sub-directory, which contains logical graph files",
    )
    parser.add_option(
        "-t",
        "--pgtdir",
        action="store",
        type="string",
        dest="pgt_path",
        help="physical graph template path (output)",
    )
    parser.add_option(
        "-H",
        "--host",
        action="store",
        type="string",
        dest="host",
        default="0.0.0.0",
        help="logical graph editor host (all by default)",
    )
    parser.add_option(
        "-p",
        "--port",
        action="store",
        type="int",
        dest="port",
        default=8084,
        help="logical graph editor port (8084 by default)",
    )
    parser.add_option(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        default=False,
        help="Enable more logging",
    )

    (options, args) = parser.parse_args(args)

    if options.lg_path is None or options.pgt_path is None:
        parser.error("Graph paths missing (-d/-t)")
    elif not os.path.exists(options.lg_path):
        parser.error("{0} does not exist.".format(options.lg_path))

    if options.verbose:
        fmt = logging.Formatter(
            "%(asctime)-15s [%(levelname)5.5s] [%(threadName)15.15s] %(name)s#%(funcName)s:%(lineno)s %(message)s"
        )
        fmt.converter = time.gmtime
        streamHdlr = logging.StreamHandler(sys.stdout)
        streamHdlr.setFormatter(fmt)
        logging.root.addHandler(streamHdlr)
        logging.root.setLevel(logging.DEBUG)

    try:
        os.makedirs(options.pgt_path)
    except:
        pass

    global lg_dir
    lg_dir = options.lg_path
    global pgt_dir
    pgt_dir = options.pgt_path
    global pg_mgr
    pg_mgr = PGManager(pgt_dir)

    # Simple and easy
    def handler(*_args):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)

    bottle.run(
        host=options.host,
        server="wsgiref",
        port=options.port,
        debug=options.verbose,
        server_class=restutils.ThreadingWSGIServer,
        handler_class=restutils.LoggingWSGIRequestHandler,
    )
