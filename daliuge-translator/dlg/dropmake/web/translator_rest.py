import argparse
import json
import logging
import os
import pathlib
import signal
import sys
import threading
import time
from json import JSONDecodeError

import uvicorn
from fastapi import FastAPI, Request, Body, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from dlg.common.reproducibility.constants import REPRO_DEFAULT
from dlg.common.reproducibility.reproducibility import init_lgt_repro_data
from dlg.dropmake.lg import GraphException
from dlg.dropmake.pg_manager import PGManager
from dlg.dropmake.web.translator_utils import file_as_string, lg_repo_contents, lg_path, lg_exists, \
    pgt_exists, pgt_path, pgt_repo_contents

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


@app.post("/jsonbody/lg")
def jsonbody_post_lg(
        lg_name: str = Body(),
        lg_content: str = Body(),
        rmode: str = Body(default=str(REPRO_DEFAULT.value)),
):
    """
    Post a logical graph JSON.
    """
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
        return HTTPException(status_code=500,
                             detail="Failed to save logical graph {0}:{1}".format(lg_name, str(e)))
    finally:
        post_sem.release()


@app.get("/jsonbody/lg")
def jsonbody_get_lg(
        lg_name: str = Body()
):
    """
    Returns JSON representation of saved logical graph.
    """
    if lg_name is None or len(lg_name) == 0:
        all_lgs = lg_repo_contents()
        try:
            first_dir = next(iter(all_lgs))
            first_lg = first_dir + "/" + all_lgs[first_dir][0]
            lg_name = first_lg
        except StopIteration:
            return "Nothing found in dir {0}".format(lg_path)
    if lg_exists(lg_name):
        # print "Loading {0}".format(lg_name)
        lgp = lg_path(lg_name)
        with open(lgp, "r") as f:
            data = f.read()
        return data
    else:
        return HTTPException(status_code=404, detail="JSON graph {0} not found\n".format(lg_name))


@app.get("/jsonbody/pgt")
def jsonbody_get_pgt(
        pgt_name: str = Body()
):
    """
    Return JSON representation of a physical graph template
    """
    if pgt_exists(pgt_name):
        # print "Loading {0}".format(lg_name)
        pgt = pgt_path(pgt_name)
        with open(pgt, "r") as f:
            data = f.read()
        return data
    else:
        return HTTPException(status_code=404, detail="JSON graph {0} not found".format(pgt_name))


@app.get("/pg_viewer", response_class=HTMLResponse)
def load_pg_viewer(request: Request,
                   pgt_name: str = Body()
                   ):
    """
    Loads the physical graph viewer
    """
    if pgt_name is None or len(pgt_name) == 0:
        all_pgts = pgt_repo_contents()
        try:
            first_dir = next(iter(all_pgts))
            pgt_name = first_dir + os.sep + all_pgts[first_dir][0]
        except StopIteration:
            pgt_name = None
    if pgt_exists(pgt_name):
        tpl = templates.TemplateResponse("pg_viewer.html", {
            "request": request,
            "pgt_view_json_name": pgt_name,
            "partition_info": None,
            "title": "Physical Graph Template",
            "error": None
        })
        return tpl
    else:
        return HTTPException(status_code=404,
                             detail="Physical graph template view {0} not found {1}".format(
                                 pgt_name, pgt_dir))


@app.get("/show_gantt_chart", response_class=HTMLResponse)
def show_gantt_chart(
        request: Request,
        pgt_id: str = Body()
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
        pgt_id: str = Body()
):
    """
    Interface to retrieve a Gantt Chart matrix associated with a PGT
    """
    try:
        ret = pg_mgr.get_gantt_chart(pgt_id)
        return ret
    except GraphException as ge:
        return HTTPException(status_code=500, detail="Failed to generate Gantt chart for {0}: {1}"
                             .format(pgt_id, ge))


@app.get("/show_schedule_matrix", response_class=HTMLResponse)
def show_schedule_matrix(
        request: Request,
        pgt_id: str = Body()
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
        pgt_id: str = Body()
):
    """
    Interface to return all schedule matrices for a single pgt_id
    """
    try:
        ret = pg_mgr.get_schedule_matrices(pgt_id)
        return ret
    except Exception as e:
        return HTTPException(status_code=500, detail="Failed to get schedule matrices for {0}: {1}"
                             .format(pgt_id, e))


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
        port=8084,
        debug=options.verbose
    )
