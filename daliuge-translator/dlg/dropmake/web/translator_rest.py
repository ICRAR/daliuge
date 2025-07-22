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

# pylint: disable=global-at-module-level

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
from fastapi import FastAPI, Request, Body, Query, HTTPException, Form
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from jsonschema import validate, ValidationError
from pydantic import BaseModel

import dlg.constants as constants
import dlg.dropmake.pg_generator
from dlg import restutils, common
from dlg.clients import CompositeManagerClient
from dlg.common.reproducibility.constants import (
    REPRO_DEFAULT,
    ALL_RMODES,
    ReproducibilityFlags,
)
from dlg.common.reproducibility.reproducibility import (
    init_lgt_repro_data,
    init_lg_repro_data,
    init_pgt_partition_repro_data,
    init_pgt_unroll_repro_data,
    init_pg_repro_data,
)

from dlg import utils
from dlg.common.deployment_methods import DeploymentMethods
from dlg.common.k8s_utils import check_k8s_env
from dlg.dropmake.lg import GraphException
from dlg.dropmake.pg_manager import PGManager
from dlg.dropmake.scheduler import SchedulerException
from dlg.dropmake.web.translator_utils import (
    file_as_string,
    lg_repo_contents,
    lg_path,
    lg_exists,
    pgt_exists,
    pgt_path,
    pgt_repo_contents,
    prepare_lgt,
    unroll_and_partition_with_params,
    make_algo_param_dict,
    get_mgr_deployment_methods,
    parse_mgr_url,
)

APP_DESCRIPTION = """
DALiuGE LG Web interface translates and deploys logical graphs.

The interface is split into two parts, refer to the main DALiuGE documentation
[DALiuGE documentation](https://daliuge.readthedocs.io/) for more information

### Original API
A set of endpoints are maintained for backwards compatibility

### New API
The new API mirrors that of the command line interface with a focus on body parameters, rather
than query parameters.

However, a new API for deployment is yet to be implemented

Original author: chen.wu@icrar.org
"""
APP_TAGS_METADATA = [
    {
        "name": "Original",
        "description": "The original DALiuGE LG_web endpoints.",
    },
    {
        "name": "Updated",
        "description": "The new post-centric style mirror of CLI interface.",
    },
]

file_location = pathlib.Path(__file__).parent.absolute()
templates = Jinja2Templates(directory=file_location)

app = FastAPI(
    title="DALiuGE LG Web Interface",
    description=APP_DESCRIPTION,
    openapi_tags=APP_TAGS_METADATA,
    contact={"name": "pritchardn", "email": "nicholas.pritchard@icrar.org"},
    version=dlg.version.version,
    license_info={
        "name": "LGPLv2+",
        "url": "https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html",
    },
)
app.mount("/static", StaticFiles(directory=file_location), name="static")
logger = logging.getLogger(f"dlg.{__name__}")

post_sem = threading.Semaphore(1)
gen_pgt_sem = threading.Semaphore(1)

global lg_dir
global pgt_dir
global pg_mgr
LG_SCHEMA = json.loads(file_as_string("lg.graph.schema", module="dlg.dropmake"))


@app.post("/jsonbody", tags=["Original"])
def jsonbody_post_lg(
    lg_name: str = Form(description="The name of the lg to use"),
    lg_content: str = Form(description="The content of the lg to save to file"),
    rmode: str = Form(default=str(REPRO_DEFAULT.value)),
):
    """
    Post a logical graph JSON.
    """
    if not lg_exists(lg_dir, lg_name):
        raise HTTPException(
            status_code=404,
            detail="Creating new graphs through this API is not supported",
        )
    try:
        lg_content = json.loads(lg_content)
    except JSONDecodeError:
        logger.warning("Could not decode lgt %s", lg_name)
    lg_content = init_lgt_repro_data(lg_content, rmode)
    logical_path = pathlib.Path(lg_dir, lg_name)
    post_sem.acquire()
    try:
        with open(logical_path, "w") as lg_file:
            lg_file.write(json.dumps(lg_content))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail="Failed to save logical graph {0}:{1}".format(lg_name, str(e)),
        ) from e
    finally:
        post_sem.release()


@app.get("/jsonbody", tags=["Original"])
def jsonbody_get_lg(
    lg_name: str = Query(
        default=None, description="The name of the lg to load from file"
    )
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
        raise HTTPException(
            status_code=404,
            detail="JSON graph {0} not found\n".format(lg_name),
        )


@app.get("/pgt_jsonbody", response_class=JSONResponse, tags=["Original"])
def jsonbody_get_pgt(
    pgt_name: str = Query(description="The name of the pgt to load from file"),
):
    """
    Return JSON representation of a physical graph template
    """
    if pgt_exists(pgt_dir, pgt_name):
        # print "Loading {0}".format(name)
        pgt = pgt_path(pgt_dir, pgt_name)
        with open(pgt, "r") as f:
            data = f.read()
        return JSONResponse(data)
    else:
        raise HTTPException(
            status_code=404, detail="JSON graph {0} not found".format(pgt_name)
        )


@app.get("/pg_viewer", response_class=HTMLResponse, tags=["Original"])
def load_pg_viewer(
    request: Request,
    pgt_view_name: str = Query(
        default=None, description="The string of the type of view to provide"
    ),
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
        tpl = templates.TemplateResponse(
            "pg_viewer.html",
            {
                "request": request,
                "pgt_view_json_name": pgt_view_name,
                "partition_info": None,
                "title": "Physical Graph Template",
                "error": None,
            },
        )
        return tpl
    else:
        raise HTTPException(
            status_code=404,
            detail="Physical graph template view {0} not found {1}".format(
                pgt_view_name, pgt_dir
            ),
        )


@app.get("/show_gantt_chart", response_class=HTMLResponse, tags=["Original"])
def show_gantt_chart(
    request: Request,
    pgt_id: str = Query(
        description="The pgt_id used to internally reference this graph"
    ),
):
    """
    Interface to show the gantt chart
    """
    tpl = templates.TemplateResponse(
        "matrix_vis.html",
        {
            "request": request,
            "pgt_view_json_name": pgt_id,
            "vis_action": "pgt_gantt_chart",
        },
    )
    return tpl


@app.get("/pgt_gantt_chart", tags=["Original"])
def get_gantt_chart(
    pgt_id: str = Query(
        description="The pgt_id used to internally reference this graph"
    ),
):
    """
    Interface to retrieve a Gantt Chart matrix associated with a PGT
    """
    try:
        ret = pg_mgr.get_gantt_chart(pgt_id)
        return ret
    except GraphException as ge:
        raise HTTPException(
            status_code=500,
            detail="Failed to generate Gantt chart for {0}: {1}".format(pgt_id, ge),
        ) from ge


@app.get("/show_schedule_mat", response_class=HTMLResponse, tags=["Original"])
def show_schedule_matrix(
    request: Request,
    pgt_id: str = Query(
        description="The pgt_id used to internally reference this graph"
    ),
):
    """
    Interface to show the schedule mat
    """
    tpl = templates.TemplateResponse(
        "matrix_vis.html",
        {
            "request": request,
            "pgt_view_json_name": pgt_id,
            "vis_action": "pgt_schedule_mat",
        },
    )
    return tpl


@app.get("/get_schedule_matrices", tags=["Original"])
def get_schedule_matrices(
    pgt_id: str = Query(
        description="The pgt_id used to internally reference this graph"
    ),
):
    """
    Interface to return all schedule matrices for a single pgt_id
    """
    try:
        ret = pg_mgr.get_schedule_matrices(pgt_id)
        return ret
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail="Failed to get schedule matrices for {0}: {1}".format(pgt_id, e),
        ) from e


# ------ Graph deployment methods ------ #


@app.get("/gen_pgt", tags=["Original"])
def gen_pgt(
    request: Request,
    lg_name: str = Query(
        description="If present, translator will attempt to load this lg from file"
    ),
    rmode: str = Query(
        default=str(REPRO_DEFAULT.value),
        description="Reproducibility mode setting level of provenance tracking. Refer to main documentation for more information",
    ),
    test: str = Query(
        default="false",
        description="If 'true', will replace all apps with sleeps",
    ),
    num_par: int = Query(
        default=1, description="The number of data partitions in the graph"
    ),
    algo: str = Query(
        default="metis",
        description="The scheduling algorithm used when unrolling the graph",
    ),
    num_islands: int = Query(
        default=0, description="The number of data-islands to partition"
    ),
    par_label: str = Query(
        default="Partition",
        description="The label prefixed to each generated partition",
    ),
):
    if not lg_exists(lg_dir, lg_name):
        raise HTTPException(
            status_code=404,
            detail="Logical graph '{0}' not found".format(lg_name),
        )
    try:
        print("TESTING PRINT IN TEST")
        lgt = prepare_lgt(lg_path(lg_dir, lg_name), rmode)
        test = test.lower() == "true"
        pgt = unroll_and_partition_with_params(
            lgt,
            test,
            algo,
            num_par,
            num_islands,
            par_label,
            request.query_params.items(),
        )
        num_partitions = 0  # pgt._num_parts;

        pgt_id = pg_mgr.add_pgt(pgt, lg_name)

        part_info = " - ".join(
            ["{0}:{1}".format(k, v) for k, v in pgt.result().items()]
        )
        tpl = templates.TemplateResponse(
            "pg_viewer.html",
            {
                "request": request,
                "pgt_view_json_name": pgt_id,
                "partition_info": part_info,
                "title": "Physical Graph Template%s"
                % ("" if num_partitions == 0 else "Partitioning"),
                "error": None,
            },
        )
        return tpl
    except GraphException as ge:
        logger.info("Graph Exception")
        raise HTTPException(
            status_code=500,
            detail="Invalid Logical Graph {1}: {0}".format(str(ge), lg_name),
        ) from ge
    except SchedulerException as se:
        logger.info("Schedule Exception")
        raise HTTPException(
            status_code=500,
            detail="Graph scheduling exception {1}: {0}".format(str(se), lg_name),
        ) from se
    except Exception as e:
        logger.info("Partition / Other exception")
        trace_msg = traceback.format_exc()
        raise HTTPException(
            status_code=500,
            detail="Graph partition exception {1}: {0}".format(trace_msg, lg_name),
        ) from e


@app.post("/gen_pgt", response_class=HTMLResponse, tags=["Original"])
async def gen_pgt_post(
    request: Request,
    lg_name: str = Form(
        description="If present, translator will attempt to load this lg from file"
    ),
    json_data: str = Form(description="The graph data used as the graph if supplied"),
    rmode: str = Form(
        str(REPRO_DEFAULT.value),
        description="Reproducibility mode setting level of provenance tracking. Refer to main documentation for more information",
    ),
    test: str = Form(
        default="false",
        description="If 'true', will replace all apps with sleeps",
    ),
    algo: str = Form(
        default="metis",
        description="The scheduling algorithm used when unrolling the graph",
    ),
    num_par: int = Form(
        default=1, description="The number of data partitions in the graph"
    ),
    num_islands: int = Form(
        default=0, description="The number of data-islands to partition"
    ),
    par_label: str = Form(
        default="Partition",
        description="The label prefixed to each generated partition",
    ),
    min_goal: Union[int, None] = Form(default=None),
    ptype: Union[int, None] = Form(default=None),
    max_load_imb: Union[int, None] = Form(default=None),
    max_cpu: Union[int, None] = Form(default=None),
    time_greedy: Union[int, None] = Form(default=None),
    deadline: Union[int, None] = Form(default=None),
    topk: Union[int, None] = Form(default=None),
    swarm_size: Union[int, None] = Form(default=None),
    max_mem: Union[int, None] = Form(default=None),
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
        logical_graph = prepare_lgt(logical_graph, rmode)
        # LG -> PGT
        # TODO: Warning: I dislike doing it this way with a passion, however without changing the tests/ usage of the api getting all form fields is difficult.
        algo_params = make_algo_param_dict(
            min_goal,
            ptype,
            max_load_imb,
            max_cpu,
            time_greedy,
            deadline,
            topk,
            swarm_size,
            max_mem,
        )
        pgt = unroll_and_partition_with_params(
            logical_graph,
            test,
            algo,
            num_par,
            num_islands,
            par_label,
            algo_params,
        )
        pgt_id = pg_mgr.add_pgt(pgt, lg_name)
        part_info = " - ".join(
            ["{0}:{1}".format(k, v) for k, v in pgt.result().items()]
        )
        tpl = templates.TemplateResponse(
            "pg_viewer.html",
            {
                "request": request,
                "pgt_view_json_name": pgt_id,
                "partition_info": part_info,
                "title": "Physical Graph Template%s"
                % ("" if num_par == 0 else "Partitioning"),
                "error": None,
            },
        )
        return tpl
    except GraphException as ge:
        logger.info("GRAPH EXCEPTION")
        raise HTTPException(
            status_code=500,
            detail="Invalid Logical Graph {1}: {0}".format(str(ge), lg_name),
        ) from ge
    except SchedulerException as se:
        logger.info("SCHEDULE EXCEPTION")
        raise HTTPException(
            status_code=500,
            detail="Graph scheduling exception {1}: {0}".format(str(se), lg_name),
        ) from se
    except Exception as e:
        logger.info("OTHER EXCEPTION")
        trace_msg = traceback.format_exc()
        raise HTTPException(
            status_code=500,
            detail="Graph partition exception {1}: {0}".format(trace_msg, lg_name),
        ) from e


@app.get("/gen_pg", response_class=JSONResponse, tags=["Original"])
def gen_pg(
    pgt_id: str = Query(
        description="The pgt_id used to internally reference this graph"
    ),
    dlg_mgr_deploy: Union[str, None] = Query(
        default=None,
        description="If supplied, this endpoint will attempt to deploy the graph is the dlg_pgt_url or dlg_mgr_host/port endpoint",
    ),
    dlg_mgr_url: Union[str, None] = Query(
        default=None, description="The DALiuGE manager to deploy the graph to"
    ),
    dlg_mgr_host: Union[str, None] = Query(
        default=None,
        description="The DALiuGE manager base IP to deploy the graph to",
    ),
    dlg_mgr_port: Union[int, None] = Query(
        default=None,
        description="The DALiuGE manager port to deploy the graph to",
    ),
    tpl_nodes_len: int = Query(
        default=0,
        description="The number of nodes to unroll the graph partition for",
    ),
):
    """
    RESTful interface to convert a PGT(P) into PG by mapping
    PGT(P) onto a given set of available resources
    """
    mprefix = ""
    mport = 443
    if dlg_mgr_url is not None:
        mparse = urlparse(dlg_mgr_url)
        try:
            (mhost, mport) = mparse.netloc.split(":")
            mport = int(mport)
        except ValueError as e:
            logger.debug("URL parsing error of: %s, %s", dlg_mgr_url, str(e))
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
    # if the 'deploy' checkbox is not checked,
    # then the form submission will NOT contain a 'dlg_mgr_deploy' field
    deploy = dlg_mgr_deploy is not None
    pgtp = pg_mgr.get_pgt(pgt_id)
    if pgtp is None:
        raise HTTPException(
            status_code=404,
            detail="PGT(P) with id {0} not found in the Physical Graph Manager".format(
                pgt_id
            ),
        )

    pgtpj = pgtp.gojs_json_obj
    reprodata = pgtp.reprodata
    num_partitions = len(list(filter(lambda n: "isGroup" in n, pgtpj["nodeDataArray"])))

    if mhost is None:
        if tpl_nodes_len > 0:
            nnodes = num_partitions
        else:
            raise HTTPException(
                status_code=500,
                detail="Must specify DALiuGE manager host or tpl_nodes_len",
            )

        pg_spec = pgtp.to_pg_spec([], ret_str=False, tpl_nodes_len=nnodes)
        pg_spec.append(reprodata)
        return JSONResponse(pg_spec)
    try:
        mgr_client = CompositeManagerClient(
            host=mhost, port=mport, url_prefix=mprefix, timeout=30
        )
        # 1. get a list of nodes
        node_list = [f"{mhost}:{mport}"] + mgr_client.nodes()
        logger.debug("Calling mapping to nodes: %s", node_list)
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
            return RedirectResponse(
                "http://{0}:{1}{2}/session?sessionId={3}".format(
                    mhost, mport, mprefix, ssid
                )
            )
        else:
            return JSONResponse(pg_spec)
    except restutils.RestClientException as re:
        raise HTTPException(
            status_code=500,
            detail="Failed to interact with DALiUGE Drop Manager: {0}".format(re),
        ) from re
    except Exception as ex:
        logger.error(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail="Failed to deploy physical graph: {0}".format(ex),
        ) from ex


@app.post("/gen_pg_spec", tags=["Original"])
def gen_pg_spec(
    pgt_id: str = Body(
        description="The pgt_id used to internally reference this graph"
    ),
    node_list: list = Body(
        default=[],
        description="The list of daliuge nodes to submit the graph to",
    ),
    manager_host: str = Body(
        description="The address of the manager host where the graph will be deployed to."
    ),
    tpl_nodes_len: int = Body(
        default=1,
        description="The number of nodes requested by the graph",
    ),
):
    """
    Interface to convert a PGT(P) into pg_spec
    """
    try:
        if manager_host.find(":") == -1:
            manager_host = f"{manager_host}:{constants.ISLAND_DEFAULT_REST_PORT}"
        logger.debug("pgt_id: %s", str(pgt_id))
        # logger.debug("node_list: %s", str(node_list))
    except Exception as ex:
        logger.error("%s", traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail="Unable to parse json body of request for pg_spec: {0}".format(ex),
        ) from ex
    pgtp = pg_mgr.get_pgt(pgt_id)
    if pgtp is None:
        raise HTTPException(
            status_code=404,
            detail="PGT(P) with id {0} not found in the Physical Graph Manager".format(
                pgt_id
            ),
        )
    if node_list is None:
        raise HTTPException(status_code=500, detail="Must specify DALiuGE nodes list")

    try:
        logger.debug("Calling mapping to host: %s", [manager_host] + node_list)
        pg_spec = pgtp.to_pg_spec(
            [manager_host] + node_list,
            tpl_nodes_len=tpl_nodes_len,
            ret_str=False,
        )
        root_uids = common.get_roots(pg_spec)
        logger.debug("Root UIDs: %s", list(root_uids))
        response = JSONResponse(
            json.dumps(
                {"pg_spec": pg_spec, "root_uids": list(root_uids)},
            ),
        )
        return response
    except Exception as ex:
        logger.error("%s", traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail="Failed to generate pg_spec: {0}".format(ex),
        ) from ex


@app.get("/gen_pg_helm", tags=["Original"])
def gen_pg_helm(
    pgt_id: str = Body(
        description="The pgt_id used to internally reference this graph"
    ),
):
    """
    Deploys a PGT as a K8s helm chart.
    """
    # Get pgt_data
    from ...deploy.start_helm_cluster import start_helm

    pgtp = pg_mgr.get_pgt(pgt_id)
    if pgtp is None:
        raise HTTPException(
            status_code=404,
            detail="PGT(P) with id {0} not found in the Physical Graph Manager".format(
                pgt_id
            ),
        )

    pgtpj = pgtp.gojs_json_obj
    logger.info("PGTP: %s", pgtpj)
    num_partitions = len(list(filter(lambda n: "isGroup" in n, pgtpj["nodeDataArray"])))
    # Send pgt_data to helm_start
    try:
        start_helm(pgtp, num_partitions, pgt_dir)
    except restutils.RestClientException as ex:
        logger.error(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail="Failed to deploy physical graph: {0}".format(ex),
        ) from ex
    # TODO: Not sure what to redirect to yet
    return "Inspect your k8s dashboard for deployment status"


# ------ Methods from translator CLI ------ #


class AlgoParams(BaseModel):
    """
    Set of scheduling algorithm parameters, not all apply to all algorithms.
    Refer to main documentation for more information.
    """

    min_goal: Union[int, None] = None
    ptype: Union[int, None] = None
    max_load_imb: Union[int, None] = None
    max_cpu: Union[int, None] = None
    time_greedy: Union[int, None] = None
    deadline: Union[int, None] = None
    topk: Union[int, None] = None
    swarm_size: Union[int, None] = None
    max_mem: Union[int, None] = None


class KnownAlgorithms(str, Enum):
    """
    List of known scheduling algorithms.
    Will need to be updated manually.
    """

    ALGO_NONE = ("none",)
    ALGO_METIS = ("metis",)
    ALGO_MY_SARKAR = ("mysarkar",)
    ALGO_MIN_NUM_PARTS = ("min_num_parts",)
    ALGO_PSO = "pso"


def load_graph(graph_content: str, graph_name: str):
    out_graph = {}
    if graph_content is not None and graph_name is not None:
        raise HTTPException(
            status_code=400,
            detail="Need to supply either an name or content but not both",
        )
    if not lg_exists(lg_dir, graph_name):
        if not graph_content:
            raise HTTPException(status_code=400, detail="LG content is nonexistent")
        else:
            try:
                out_graph = json.loads(graph_content)
            except JSONDecodeError as jerror:
                logger.error(jerror)
                raise HTTPException(status_code=400, detail="LG content is malformed") \
                    from jerror
    else:
        lgp = lg_path(lg_dir, graph_name)
        with open(lgp, "r") as f:
            try:
                out_graph = json.load(f)
            except JSONDecodeError as jerror:
                logger.error(jerror)
                raise HTTPException(
                    status_code=500, detail="LG graph on file cannot be loaded"
                ) from jerror
    return out_graph


@app.post("/lg_fill", response_class=JSONResponse, tags=["Updated"])
def lg_fill(
    lg_name: str = Form(
        default=None,
        description="If present, translator will attempt to load this lg from file",
    ),
    lg_content: str = Form(
        default=None,
        description="If present, translator will use this string as the graph content",
    ),
    parameters: str = Form(
        default="{}", description="JSON key: value store of graph paramter"
    ),
    rmode: str = Form(
        REPRO_DEFAULT.name,
        enum=[roption.name for roption in [ReproducibilityFlags.NOTHING] + ALL_RMODES],
        description="Reproducibility mode setting level of provenance tracking. Refer to main documentation for more information",
    ),
):
    """
    Will fill a logical graph by replacing fields with supplied parameters.

    One of lg_name or lg_content, but not both, must be specified.
    """
    lg_graph = load_graph(lg_content, lg_name)
    try:
        params = json.loads(parameters)
    except JSONDecodeError as jerror:
        logger.error(jerror)
        raise HTTPException(
            status_code=400,
            detail="Parameter string is invalid") from jerror
    output_graph = dlg.dropmake.pg_generator.fill(lg_graph, params)
    output_graph = init_lg_repro_data(init_lgt_repro_data(output_graph, rmode))
    return JSONResponse(output_graph)


@app.post("/unroll", response_class=JSONResponse, tags=["Updated"])
def lg_unroll(
    lg_name: str = Form(
        default=None,
        description="If present, translator will attempt to load this lg from file",
    ),
    lg_content: str = Form(
        default=None,
        description="If present, translator will use this string as the graph content",
    ),
    oid_prefix: str = Form(
        default=None, description="ID prefix appended to unrolled nodes"
    ),
    zero_run: bool = Form(
        default=None,
        description="If true, apps will be replaced with sleep apps",
    ),
    default_app: str = Form(
        default=None,
        description="If set, will change all apps to this app class",
    ),
):
    """
    Will unroll a logical graph into a physical graph template.

    One of lg_name or lg_content, but not both, needs to be specified.
    """
    lg_graph = load_graph(lg_content, lg_name)
    pgt = dlg.dropmake.pg_generator.unroll(lg_graph, oid_prefix, zero_run, default_app)
    pgt = init_pgt_unroll_repro_data(pgt)
    return JSONResponse(pgt)


@app.post("/partition", response_class=JSONResponse, tags=["Updated"])
def pgt_partition(
    pgt_name: str = Form(
        default=None,
        description="If specified, translator will attempt to load graph from file",
    ),
    pgt_content: str = Form(
        default=None,
        description="If present, translator will use this string as the graph content",
    ),
    num_partitions: int = Form(
        default=1,
        description="Number of partitions to unroll the graph across",
    ),
    num_islands: int = Form(
        default=1, description="Number of data islands to partition for"
    ),
    algorithm: KnownAlgorithms = Form(
        default="metis", description="The selected scheduling algorithm"
    ),
    algo_params: AlgoParams = Form(
        default=AlgoParams(),
        description="The parameter values passed to the scheduling algorithm. Required parameters varies per algorithm.",
    ),
):
    """
    Uses scheduling algorithms to partition an unrolled pgt across several partitions and data islands.

    One of pgt_name or pgt_content, but not both, must be specified.
    """
    graph = load_graph(pgt_content, pgt_name)
    reprodata = {}
    if not graph[-1].get("oid"):
        reprodata = graph.pop()
    pgt = dlg.dropmake.pg_generator.partition(
        graph, algorithm, num_partitions, num_islands, algo_params.dict()
    )
    pgt.append(reprodata)
    pgt = init_pgt_partition_repro_data(pgt)
    return JSONResponse(pgt)


@app.post("/unroll_and_partition", response_class=JSONResponse, tags=["Updated"])
def lg_unroll_and_partition(
    lg_name: str = Form(
        default=None,
        description="If present, translator will attempt to load this lg from file",
    ),
    lg_content: str = Form(
        default=None,
        description="If present, translator will use this string as the graph content",
    ),
    oid_prefix: str = Form(
        default=None, description="ID prefix appended to unrolled nodes"
    ),
    zero_run: bool = Form(
        default=None,
        description="If true, apps will be replaced with sleep apps",
    ),
    default_app: str = Form(
        default=None,
        description="If set, will change all apps to this app class",
    ),
    num_partitions: int = Form(
        default=1,
        description="Number of partitions to unroll the graph across",
    ),
    num_islands: int = Form(
        default=1, description="Number of data islands to partition for"
    ),
    algorithm: KnownAlgorithms = Form(
        default="metis", description="The selected scheduling algorithm"
    ),
    algo_params: AlgoParams = Form(
        default=AlgoParams(),
        description="The parameter values passed to the scheduling algorithm. Required parameters varies per algorithm.",
    ),
):
    """
    Unrolls and partitions a logical graph with the provided various parameters.

    One of lg_name and lg_content, but not both, must be specified.
    """
    lg_graph = load_graph(lg_content, lg_name)
    pgt = dlg.dropmake.pg_generator.unroll(lg_graph, oid_prefix, zero_run, default_app)
    pgt = init_pgt_unroll_repro_data(pgt)
    reprodata = pgt.pop()
    pgt = dlg.dropmake.pg_generator.partition(
        pgt, algorithm, num_partitions, num_islands, algo_params.dict()
    )
    pgt.append(reprodata)
    pgt = init_pgt_partition_repro_data(pgt)
    return JSONResponse(pgt)


@app.post("/map", response_class=JSONResponse, tags=["Updated"])
def pgt_map(
    pgt_name: str = Form(
        default=None,
        description="If supplied, this graph will attempted to be loaded from disk on the server",
    ),
    pgt_content: str = Form(
        default=None, description="If supplied, this is the graph content"
    ),
    nodes: str = Form(
        default=None,
        description="Comma separated list of IP addrs e.g. 'localhost, 127.0.0.2'",
    ),
    num_islands: int = Form(
        default=1, description="The number of data islands to launch"
    ),
    co_host_dim: bool = Form(
        default=True,
        description="Whether to launch data island manager processes alongside node-managers",
    ),
    host: str = Form(
        default=None,
        description="If present, will attempt to query this address for node-managers",
    ),
    port: int = Form(
        default=dlg.constants.ISLAND_DEFAULT_REST_PORT,
        description="Port used by HOST manager process",
    ),
):
    """
    Maps physical graph templates to node resources.
    """
    if not nodes:
        client = CompositeManagerClient(host, port, timeout=10)
        nodes = [f"{host}:{port}"] + client.nodes()
    if len(nodes) <= num_islands:
        logger.error("Not enough nodes to fill all islands")
        raise HTTPException(
            status_code=500,
            detail="#nodes (%d) should be larger than the number of islands (%d)"
            % (len(nodes), num_islands),
        )
    pgt = load_graph(pgt_content, pgt_name)
    reprodata = {}
    if not pgt[-1].get("oid"):
        reprodata = pgt.pop()
    logger.info(nodes)
    pg = dlg.dropmake.pg_generator.resource_map(pgt, nodes, num_islands, co_host_dim)
    pg.append(reprodata)
    pg = init_pg_repro_data(pg)
    return JSONResponse(pg)


@app.get("/api/submission_method")
def get_submission_method(
    dlg_mgr_url: str = Query(
        default=None,
        description="If present, translator will query this URL for its deployment options",
    ),
    dlg_mgr_host: str = Query(
        default=None,
        description="If present with mport and mprefix, will be the base host for deployment",
    ),
    dlg_mgr_port: int = Query(default=None, description=""),
):
    logger.debug("Received submission_method request")
    if dlg_mgr_url:
        mhost, mport, mprefix = parse_mgr_url(dlg_mgr_url)
    else:
        mhost = dlg_mgr_host
        mport = dlg_mgr_port
        mprefix = ""
    available_methods = []
    if check_k8s_env():
        available_methods.append(DeploymentMethods.HELM)
    if mhost is not None:
        available_methods = get_mgr_deployment_methods(mhost, mport, mprefix)
    logger.debug("Methods available: %s", available_methods)
    return {"methods": available_methods}


@app.get(
    "/",
    response_class=HTMLResponse,
    description="The page used to view physical graphs",
)
def index(request: Request):
    tpl = templates.TemplateResponse(
        "pg_viewer.html",
        {
            "request": request,
            "pgt_view_json_name": None,
            "partition_info": None,
            "title": "Physical Graph Template",
            "error": None,
        },
    )
    return tpl


@app.head("/")
def liveliness():
    return {}


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
        default="/tmp",
        help="A path that contains at least one sub-directory, which contains logical graph files",
    )
    parser.add_argument(
        "-t",
        "--pgtdir",
        action="store",
        type=str,
        dest="pgt_path",
        default="/tmp",
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
    parser.add_argument(
        "-l",
        "--log-dir",
        action="store",
        type=str,
        dest="logdir",
        help="The directory where the logging files will be stored",
        default=utils.getDlgLogsDir(),
    )

    parser.add_argument(
        "--local-time",
        action="store_true",
        help="Use local system time when logging",
        default=False,
    )

    options = parser.parse_args(args)

    if options.lg_path is None or options.pgt_path is None:
        parser.error("Graph paths missing (-d/-t)")
    elif not os.path.exists(options.lg_path):
        parser.error(f"{options.lg_path} does not exist")

    time_fmt_str = "Local" if options.local_time else "GMT"
    if options.verbose or options.logdir:
        fmt = logging.Formatter(
            "%(asctime)-15s [%(levelname)5.5s] [%(threadName)15.15s] "
            "%(name)s#%(funcName)s:%(lineno)s %(message)s"
        )

        fmt.converter = time.localtime if options.local_time else time.gmtime
        if options.verbose:
            stream_handler = logging.StreamHandler(sys.stdout)
            stream_handler.setFormatter(fmt)
            logging.root.addHandler(stream_handler)
            logging.root.setLevel(logging.DEBUG)
        if options.logdir:
            # This is the logfile we'll use from now on
            logdir = options.logdir
            utils.createDirIfMissing(logdir)
            logfile = os.path.join(logdir, "dlgTranslator.log")
            fileHandler = logging.FileHandler(logfile)
            fileHandler.setFormatter(fmt)
            logging.root.addHandler(fileHandler)

    try:
        os.makedirs(options.pgt_path, exist_ok=True)
    except OSError:
        logging.warning("Cannot create path %s", options.pgt_path)

    global lg_dir # pylint: disable=global-variable-undefined
    global pgt_dir # pylint: disable=global-variable-undefined
    global pg_mgr # pylint: disable=global-variable-undefined

    lg_dir = options.lg_path
    pgt_dir = options.pgt_path
    pg_mgr = PGManager(pgt_dir)

    def handler(*_args):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)

    logging.debug("Starting uvicorn verbose %s", options.verbose)
    logging.warning("Logging using %s time", time_fmt_str)
    ll = 'debug' if options.verbose else 'warning'
    uvicorn.run(app=app, host=options.host, port=options.port, log_level=ll)


if __name__ == "__main__":
    run(None, sys.argv[1:])
