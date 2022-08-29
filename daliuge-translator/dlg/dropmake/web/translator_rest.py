import argparse
import logging
import os
import signal
import sys
import time
import pathlib

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from dlg.dropmake.pg_manager import PGManager

file_location = pathlib.Path(__file__).parent.absolute()
templates = Jinja2Templates(directory=file_location)

app = FastAPI()
app.mount("/static", StaticFiles(directory=file_location), name="static")
logger = logging.getLogger(__name__)

global lg_dir
global pgt_dir
global pg_mgr


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
