import argparse
import datetime
import json
import logging
import os
import pathlib
import signal
import sys
import threading
import time
import traceback
from enum import Enum
from json import JSONDecodeError
from typing import Union
from urllib.parse import urlparse

import uvicorn
from fastapi import FastAPI, Request, Body, Query, HTTPException, Form, Depends
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from jsonschema import validate, ValidationError
from pydantic import BaseModel

import dlg.dropmake.pg_generator
from dlg import restutils, common
from dlg.clients import CompositeManagerClient
from dlg.common.reproducibility.constants import REPRO_DEFAULT, ALL_RMODES, ReproducibilityFlags
from dlg.common.reproducibility.reproducibility import init_lgt_repro_data, init_lg_repro_data, \
    init_pgt_partition_repro_data, init_pgt_unroll_repro_data
from dlg.dropmake.lg import GraphException
from dlg.dropmake.pg_manager import PGManager
from dlg.dropmake.scheduler import SchedulerException
from dlg.dropmake.web.translator_utils import file_as_string, lg_repo_contents, lg_path, lg_exists, \
    pgt_exists, pgt_path, pgt_repo_contents, prepare_lgt, unroll_and_partition_with_params, \
    make_algo_param_dict

file_location = pathlib.Path(__file__).parent.absolute()
templates = Jinja2Templates(directory=file_location)

app = FastAPI()
app.mount("/static", StaticFiles(directory=file_location), name="static")
logger = logging.getLogger(__name__)

post_sem = threading.Semaphore(1)
gen_pgt_sem = threading.Semaphore(1)

global lg_dir
global pgt_dir
global pg_mgr
LG_SCHEMA = json.loads(file_as_string("lg.graph.schema", package="dlg.dropmake"))


@app.post("/jsonbody")
def jsonbody_post_lg(
        lg_name: str = Form(),
        lg_content: str = Form(),
        rmode: str = Form(default=str(REPRO_DEFAULT.value)),
):
    """
    Post a logical graph JSON.
    """
    if not lg_exists(lg_dir, lg_name):
        raise HTTPException(status_code=404,
                            detail="Creating new graphs through this API is not supported")
    try:
        lg_content = json.loads(lg_content)
    except JSONDecodeError:
        logger.warning("Could not decode lgt %s", lg_name)
    lg_content = init_lgt_repro_data(lg_content, rmode)
    lg_path = pathlib.Path(lg_dir, lg_name)
    post_sem.acquire()
    try:
        with open(lg_path, "w") as lg_file:
            lg_file.write(json.dumps(lg_content))
    except Exception as e:
        raise HTTPException(status_code=500,
                            detail="Failed to save logical graph {0}:{1}".format(lg_name, str(e)))
    finally:
        post_sem.release()


@app.get("/jsonbody")
def jsonbody_get_lg(
        lg_name: str = Query(default=None)
):
    """
    Returns JSON representation of saved logical graph.
    """
    if lg_name is None or len(lg_name) == 0:
        all_lgs = lg_repo_contents(lg_dir)
        try:
            first_dir = next(iter(all_lgs))
            first_lg = first_dir + "/" + all_lgs[first_dir][0]
            lg_name = first_lg
        except StopIteration:
            return "Nothing found in dir {0}".format(lg_path)
    if lg_exists(lg_dir, lg_name):
        # print "Loading {0}".format(name)
        lgp = lg_path(lg_dir, lg_name)
        with open(lgp, "r") as f:
            data = json.load(f)
        return JSONResponse(data)
    else:
        raise HTTPException(status_code=404, detail="JSON graph {0} not found\n".format(lg_name))


@app.get("/pgt_jsonbody")
def jsonbody_get_pgt(
        pgt_name: str = Query()
):
    """
    Return JSON representation of a physical graph template
    """
    if pgt_exists(pgt_dir, pgt_name):
        # print "Loading {0}".format(name)
        pgt = pgt_path(pgt_dir, pgt_name)
        with open(pgt, "r") as f:
            data = f.read()
        return data
    else:
        raise HTTPException(status_code=404, detail="JSON graph {0} not found".format(pgt_name))


@app.get("/pg_viewer", response_class=HTMLResponse)
def load_pg_viewer(request: Request,
                   pgt_view_name: str = Query(default=None)
                   ):
    """
    Loads the physical graph viewer
    """
    if pgt_view_name is None or len(pgt_view_name) == 0:
        all_pgts = pgt_repo_contents(pgt_dir)
        try:
            first_dir = next(iter(all_pgts))
            pgt_view_name = first_dir + os.sep + all_pgts[first_dir][0]
        except StopIteration:
            pgt_view_name = None
    if pgt_exists(pgt_dir, pgt_view_name):
        tpl = templates.TemplateResponse("pg_viewer.html", {
            "request": request,
            "pgt_view_json_name": pgt_view_name,
            "partition_info": None,
            "title": "Physical Graph Template",
            "error": None
        })
        return tpl
    else:
        raise HTTPException(status_code=404,
                            detail="Physical graph template view {0} not found {1}".format(
                                pgt_view_name, pgt_dir))


@app.get("/show_gantt_chart", response_class=HTMLResponse)
def show_gantt_chart(
        request: Request,
        pgt_id: str = Query()
):
    """
    Interface to show the gantt chart
    """
    tpl = templates.TemplateResponse("matrix_vis.html", {
        "request": request,
        "pgt_view_json_name": pgt_id,
        "vis_action": "pgt_gantt_chart"
    })
    return tpl


@app.get("/pgt_gantt_chart")
def get_gantt_chart(
        pgt_id: str = Query()
):
    """
    Interface to retrieve a Gantt Chart matrix associated with a PGT
    """
    try:
        ret = pg_mgr.get_gantt_chart(pgt_id)
        return ret
    except GraphException as ge:
        raise HTTPException(status_code=500, detail="Failed to generate Gantt chart for {0}: {1}"
                            .format(pgt_id, ge))


@app.get("/show_schedule_mat", response_class=HTMLResponse)
def show_schedule_matrix(
        request: Request,
        pgt_id: str = Query()
):
    """
    Interface to show the schedule mat
    """
    tpl = templates.TemplateResponse("matrix_vis.html", {
        "request": request,
        "pgt_view_json_name": pgt_id,
        "vis_action": "pgt_schedule_mat"
    })
    return tpl


@app.get("/get_schedule_matrices")
def get_schedule_matrices(
        pgt_id: str = Query()
):
    """
    Interface to return all schedule matrices for a single pgt_id
    """
    try:
        ret = pg_mgr.get_schedule_matrices(pgt_id)
        return ret
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to get schedule matrices for {0}: {1}"
                            .format(pgt_id, e))


# ------ Graph deployment methods ------ #

@app.get("/gen_pgt")
def gen_pgt(
        request: Request,
        lg_name: str = Query(),
        rmode: str = Query(default=str(REPRO_DEFAULT.value)),
        test: str = Query(default="false"),
        num_par: int = Query(default=1),
        algo: str = Query(default="none"),
        num_islands: int = Query(default=0),
        par_label: str = Query(default="Partition"),
):
    if not lg_exists(lg_dir, lg_name):
        raise HTTPException(status_code=404,
                            detail="Logical graph '{0}' not found".format(lg_name))
    try:
        lgt = prepare_lgt(lg_path(lg_dir, lg_name), rmode)
        test = test.lower() == "true"
        pgt = unroll_and_partition_with_params(lgt, test, algo, num_par, num_islands,
                                               par_label, request.query_params.items())
        num_partitions = 0  # pgt._num_parts;

        pgt_id = pg_mgr.add_pgt(pgt, lg_name)

        part_info = " - ".join(
            ["{0}:{1}".format(k, v) for k, v in pgt.result().items()]
        )
        tpl = templates.TemplateResponse("pg_viewer.html", {
            "request": request,
            "pgt_view_json_name": pgt_id,
            "partition_info": part_info,
            "title": "Physical Graph Template%s"
                     % ("" if num_partitions == 0 else "Partitioning"),
            "error": None
        })
        return tpl
    except GraphException as ge:
        logger.info("Graph Exception")
        raise HTTPException(status_code=500,
                            detail="Invalid Logical Graph {1}: {0}".format(str(ge), lg_name))
    except SchedulerException as se:
        logger.info("Schedule Exception")
        raise HTTPException(status_code=500,
                            detail="Graph scheduling exception {1}: {0}".format(str(se), lg_name))
    except Exception:
        logger.info("Partition / Other exception")
        trace_msg = traceback.format_exc()
        raise HTTPException(status_code=500,
                            detail="Graph partition exception {1}: {0}".format(trace_msg, lg_name))


class AlgoParams(BaseModel):
    min_goal: Union[int, None]
    ptype: Union[int, None]
    max_load_imb: Union[int, None]
    max_cpu: Union[int, None]
    time_greedy: Union[int, None]
    deadline: Union[int, None]
    topk: Union[int, None]
    swarm_size: Union[int, None]
    max_mem: Union[int, None]


@app.post("/gen_pgt", response_class=HTMLResponse)
async def gen_pgt_post(
        request: Request,
        lg_name: str = Form(),
        json_data: str = Form(),
        rmode: str = Form(str(REPRO_DEFAULT.value)),
        test: str = Form(default="false"),
        algo: str = Form(default="none"),
        num_par: int = Form(default=1),
        num_islands: int = Form(default=0),
        par_label: str = Form(default="Partition"),
        min_goal: Union[int, None] = Form(default=None),
        ptype: Union[int, None] = Form(default=None),
        max_load_imb: Union[int, None] = Form(default=None),
        max_cpu: Union[int, None] = Form(default=None),
        time_greedy: Union[int, None] = Form(default=None),
        deadline: Union[int, None] = Form(default=None),
        topk: Union[int, None] = Form(default=None),
        swarm_size: Union[int, None] = Form(default=None),
        max_mem: Union[int, None] = Form(default=None)

):
    """
    Translating Logical Graphs to Physical Graphs.
    Differs from get_pgt above by the fact that the logical graph data is POSTed
    to this route in a HTTP form, whereas gen_pgt loads the logical graph data
    from a local file
    """
    test = test.lower() == "true"
    try:
        logical_graph = json.loads(json_data)
        try:
            validate(logical_graph, LG_SCHEMA)
        except ValidationError as ve:
            error = "Validation Error {1}: {0}".format(str(ve), lg_name)
            logger.error(error)
            # raise HTTPException(status_code=500, detail=error)
        logical_graph = prepare_lgt(logical_graph, rmode)
        # LG -> PGT
        # TODO: Warning: I dislike doing it this way with a passion, however without changing the tests/ usage of the api getting all form fields is difficult.
        algo_params = make_algo_param_dict(min_goal, ptype, max_load_imb, max_cpu, time_greedy,
                                           deadline, topk, swarm_size, max_mem)
        pgt = unroll_and_partition_with_params(logical_graph, test, algo, num_par,
                                               num_islands, par_label, algo_params)
        pgt_id = pg_mgr.add_pgt(pgt, lg_name)
        part_info = " - ".join(
            ["{0}:{1}".format(k, v) for k, v in pgt.result().items()]
        )
        tpl = templates.TemplateResponse("pg_viewer.html", {
            "request": request,
            "pgt_view_json_name": pgt_id,
            "partition_info": part_info,
            "title": "Physical Graph Template%s"
                     % ("" if num_par == 0 else "Partitioning"),
            "error": None
        })
        return tpl
    except GraphException as ge:
        logger.info("GRAPH EXCEPTION")
        raise HTTPException(status_code=500,
                            detail="Invalid Logical Graph {1}: {0}".format(str(ge), lg_name))
    except SchedulerException as se:
        logger.info("SCHEDULE EXCEPTION")
        raise HTTPException(status_code=500,
                            detail="Graph scheduling exception {1}: {0}".format(str(se), lg_name))
    except Exception:
        logger.info("OTHER EXCEPTION")
        trace_msg = traceback.format_exc()
        raise HTTPException(status_code=500,
                            detail="Graph partition exception {1}: {0}".format(trace_msg, lg_name))


@app.get("/gen_pg", response_class=StreamingResponse)
def gen_pg(
        request: Request,
        pgt_id: str = Query(),
        dlg_mgr_deploy: Union[str, None] = Query(default=None),
        dlg_mgr_url: Union[str, None] = Query(default=None),
        dlg_mgr_host: Union[str, None] = Query(default=None),
        dlg_mgr_port: Union[int, None] = Query(default=None),
        tpl_nodes_len: int = Query(default=0)
):
    """
    RESTful interface to convert a PGT(P) into PG by mapping
    PGT(P) onto a given set of available resources
    """
    # if the 'deploy' checkbox is not checked,
    # then the form submission will NOT contain a 'dlg_mgr_deploy' field
    deploy = dlg_mgr_deploy is not None
    mprefix = ""
    pgtp = pg_mgr.get_pgt(pgt_id)
    if pgtp is None:
        raise HTTPException(status_code=404,
                            detail="PGT(P) with id {0} not found in the Physical Graph Manager"
                            .format(pgt_id))

    pgtpj = pgtp._gojs_json_obj
    reprodata = pgtp.reprodata
    logger.info("PGTP: %s", pgtpj)
    num_partitions = len(list(filter(lambda n: "isGroup" in n, pgtpj["nodeDataArray"])))
    mport = 443
    if dlg_mgr_url is not None:
        mparse = urlparse(dlg_mgr_url)
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
        mhost = dlg_mgr_host
        if dlg_mgr_port is not None:
            mport = dlg_mgr_port
        else:
            mport = 443

    logger.debug("Manager host: %s", mhost)
    logger.debug("Manager port: %s", mport)
    logger.debug("Manager prefix: %s", mprefix)

    if mhost is None:
        if tpl_nodes_len > 0:
            nnodes = num_partitions
        else:
            raise HTTPException(status_code=500,
                                detail="Must specify DALiuGE manager host or tpl_nodes_len")

        pg_spec = pgtp.to_pg_spec([], ret_str=False, tpl_nodes_len=nnodes)
        pg_spec.append(reprodata)
        response = StreamingResponse(json.dumps(pg_spec))
        response.headers["Content-Disposition"] = "attachment; filename=%s" % pgt_id
        return response
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
            return RedirectResponse("http://{0}:{1}{2}/session?sessionId={3}".format(
                mhost, mport, mprefix, ssid
            ))
        else:
            response = StreamingResponse(json.dumps(pg_spec))
            response.headers["Content-Disposition"] = "attachment; filename=%s" % pgt_id
            return response
    except restutils.RestClientException as re:
        raise HTTPException(status_code=500,
                            detail="Failed to interact with DALiUGE Drop Manager: {0}".format(re))
    except Exception as ex:
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500,
                            detail="Failed to deploy physical graph: {0}".format(ex))


@app.get("/gen_pg_spec")
def gen_pg_spec(
        pgt_id: str = Body(),
        node_list: list = Body(default=[]),
        manager_host: str = Body(),
):
    """
    Interface to convert a PGT(P) into pg_spec
    """
    try:
        if manager_host == "localhost":
            manager_host = "127.0.0.1"
        logger.debug("pgt_id: %s", str(pgt_id))
        logger.debug("node_list: %s", str(node_list))
    except Exception as ex:
        logger.error("%s", traceback.format_exc())
        raise HTTPException(status_code=500,
                            detail="Unable to parse json body of request for pg_spec: {0}".format(
                                ex))
    pgtp = pg_mgr.get_pgt(pgt_id)
    if pgtp is None:
        raise HTTPException(status_code=404,
                            detail="PGT(P) with id {0} not found in the Physical Graph Manager".format(
                                pgt_id
                            ))
    if node_list is None:
        raise HTTPException(status_code=500, detail="Must specify DALiuGE nodes list")

    try:
        pg_spec = pgtp.to_pg_spec([manager_host] + node_list, ret_str=False)
        root_uids = common.get_roots(pg_spec)
        response = StreamingResponse(json.dumps({"pg_spec": pg_spec, "root_uids": list(root_uids)}))
        response.content_type = "application/json"
        return response
    except Exception as ex:
        logger.error("%s", traceback.format_exc())
        raise HTTPException(status_code=500, detail="Failed to generate pg_spec: {0}".format(ex))


@app.get("/gen_pg_helm")
def gen_pg_helm(
        pgt_id: str = Body()
):
    """
    Deploys a PGT as a K8s helm chart.
    """
    # Get pgt_data
    from ...deploy.start_helm_cluster import start_helm
    pgtp = pg_mgr.get_pgt(pgt_id)
    if pgtp is None:
        raise HTTPException(status_code=404,
                            detail="PGT(P) with id {0} not found in the Physical Graph Manager"
                            .format(pgt_id))

    pgtpj = pgtp._gojs_json_obj
    logger.info("PGTP: %s", pgtpj)
    num_partitions = len(list(filter(lambda n: "isGroup" in n, pgtpj["nodeDataArray"])))
    # Send pgt_data to helm_start
    try:
        start_helm(pgtp, num_partitions, pgt_dir)
    except restutils.RestClientException as ex:
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500,
                            detail="Failed to deploy physical graph: {0}".format(ex))
    # TODO: Not sure what to redirect to yet
    return "Inspect your k8s dashboard for deployment status"


# ------ Methods from translator CLI ------ #

def load_graph(graph_content: str, graph_name: str):
    out_graph = {}
    if graph_content is not None and graph_name is not None:
        raise HTTPException(status_code=400,
                            detail="Need to supply either an name or content but not both")
    if not lg_exists(lg_dir, graph_name):
        try:
            out_graph = json.loads(graph_content)
        except JSONDecodeError as jerror:
            logger.error(jerror)
            raise HTTPException(status_code=400,
                                detail="LG content is malformed")
    else:
        lgp = lg_path(lg_dir, graph_name)
        with open(lgp, "r") as f:
            try:
                out_graph = json.load(f)
            except JSONDecodeError as jerror:
                logger.error(jerror)
                raise HTTPException(status_code=500,
                                    detail="LG graph on file cannot be loaded")
    return out_graph


@app.post("/lg_fill", response_class=JSONResponse)
def lg_fill(
        lg_name: str = Form(default=None),
        lg_content: str = Form(default=None),
        parameters: str = Form(default="{}"),
        rmode: str = Form(REPRO_DEFAULT.name, enum=[roption.name for roption in [ReproducibilityFlags.NOTHING] + ALL_RMODES])
):
    """
    Will fill a logical graph (either loaded serverside by name or supplied directly as lg_content).
    """
    lg_graph = load_graph(lg_content, lg_name)
    try:
        params = json.loads(parameters)
    except JSONDecodeError as jerror:
        logger.error(jerror)
        raise HTTPException(status_code=400,
                            detail="Parameter string is invalid")
    output_graph = dlg.dropmake.pg_generator.fill(lg_graph, params)
    output_graph = init_lg_repro_data(init_lgt_repro_data(output_graph, rmode))
    return JSONResponse(output_graph)


@app.post("/unroll", response_class=JSONResponse)
def unroll(
        lg_name: str = Form(default=None),
        lg_content: str = Form(default=None),
        oid_prefix: str = Form(default=None),
        zero_run: bool = Form(default=False),
        default_app: str = Form(default=None)
):
    """
    Will unroll a logical graph (either loaded serverside or posted
    """
    lg_graph = load_graph(lg_content, lg_name)
    pgt = dlg.dropmake.pg_generator.unroll(lg_graph, oid_prefix, zero_run, default_app)
    pgt = init_pgt_unroll_repro_data(pgt)
    return JSONResponse(pgt)


class KnownAlgorithms(str, Enum):
    ALGO_NONE = "none",
    ALGO_METIS = "metis",
    ALGO_MY_SARKAR = "mysarkar",
    ALGO_MIN_NUM_PARTS = "min_num_parts",
    ALGO_PSO = "pso"


@app.post("/partition", response_class=JSONResponse)
def partition(
        pgt_name: str = Form(default=None),
        pgt_content: str = Form(default=None),
        num_partitions: int = Form(default=1),
        num_islands: int = Form(default=1),
        algorithm: KnownAlgorithms = Form(),
        algo_params: AlgoParams = Form()
):
    graph = load_graph(pgt_content, pgt_name)
    reprodata = {}
    if not graph[-1].contains("oid"):
        reprodata = graph.pop()
    pgt = dlg.dropmake.pg_generator.partition(graph, algorithm, num_partitions, num_islands, algo_params.dict())
    pgt.append(reprodata)
    pgt = init_pgt_partition_repro_data(pgt)
    return JSONResponse(pgt)


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    tpl = templates.TemplateResponse("pg_viewer.html", {
        "request": request,
        "pgt_view_json_name": None,
        "partition_info": None,
        "title": "Physical Graph Template",
        "error": None
    })
    return tpl


def run(_, args):
    """
    FastAPI implementation of daliuge translator interface
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--lgdir",
        action="store",
        type=str,
        dest="lg_path",
        help="A path that contains at least one sub-directory, which contains logical graph files",
    )
    parser.add_argument(
        "-t",
        "--pgtdir",
        action="store",
        type=str,
        dest="pgt_path",
        help="physical graph template path (output)",
    )
    parser.add_argument(
        "-H",
        "--host",
        action="store",
        type=str,
        dest="host",
        default="0.0.0.0",
        help="logical graph editor host (all by default)",
    )
    parser.add_argument(
        "-p",
        "--port",
        action="store",
        type=int,
        dest="port",
        default=8084,
        help="logical graph editor port (8084 by default)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        dest="verbose",
        default=False,
        help="Enable more logging",
    )

    options = parser.parse_args(args)

    if options.lg_path is None or options.pgt_path is None:
        parser.error("Graph paths missing (-d/-t)")
    elif not os.path.exists(options.lg_path):
        parser.error(f"{options.lg_path} does not exist")

    if options.verbose:
        fmt = logging.Formatter(
            "%(asctime)-15s [%(levelname)5.5s] [%(threadName)15.15s] "
            "%(name)s#%(funcName)s:%(lineno)s %(message)s"
        )
        fmt.converter = time.gmtime
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(fmt)
        logging.root.addHandler(stream_handler)
        logging.root.setLevel(logging.DEBUG)

    try:
        os.makedirs(options.pgt_path)
    except OSError:
        logging.warning("Cannot create path %s", options.pgt_path)

    global lg_dir
    global pgt_dir
    global pg_mgr

    lg_dir = options.lg_path
    pgt_dir = options.pgt_path
    pg_mgr = PGManager(pgt_dir)

    def handler(*_args):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)

    uvicorn.run(
        app=app,
        host=options.host,
        port=options.port,
        debug=options.verbose
    )
