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
import optparse
import os, traceback
import signal
import threading

import bottle
from bottle import route, request, get, static_file, template, redirect,\
 response, HTTPResponse
import pkg_resources

from ..pg_generator import LG, PGT, GraphException, MetisPGTP,\
 MySarkarPGTP, MinNumPartsPGTP, PSOPGTP
from ..pg_manager import PGManager
from ..scheduler import SchedulerException
from ... import droputils, restutils, utils
from ...manager.client import CompositeManagerClient
from ...restutils import RestClientException


def file_as_string(fname, enc='utf8'):
    b = pkg_resources.resource_string(__name__, fname) # @UndefinedVariable
    return utils.b2s(b, enc)

#lg_dir = None
post_sem = threading.Semaphore(1)
gen_pgt_sem = threading.Semaphore(1)

err_prefix = "[Error]"
MAX_PGT_FN_CNT= 300
pgt_fn_count = 0

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
    for dirpath,dirnames,fnames in os.walk(root_dir):
        if '.git' in dirnames:
            dirnames.remove('.git')
        if dirpath == root_dir:
            continue

        # Not great yet -- we should do a full second step pruning branches
        # of the tree that are empty
        files = [f for f in fnames if f.endswith('.json')]
        if files:
            contents[b(dirpath)] = files

    return contents

@route('/static/<filepath:path>')
def server_static(filepath):
    staticRoot = pkg_resources.resource_filename(__name__, resource_name=".")  # @UndefinedVariable
    return static_file(filepath, root=staticRoot)

@route('/jsonbody', method='POST')
def jsonbody_post():
    """
    Post graph JSON representation to LG or PG manager
    """
    # see the table in http://bottlepy.org/docs/dev/tutorial.html#html-form-handling
    lg_name = request.forms['lg_name']
    if (lg_exists(lg_name)):
        lg_content = request.forms['lg_content']
        lg_path = "{0}/{1}".format(lg_dir, lg_name)
        post_sem.acquire()
        try:
            # overwrite file on disks
            # print "writing to {0}".format(lg_path)
            with open(lg_path, "w") as f:
                f.write(lg_content)
        except Exception as excmd2:
            response.status = 500
            return "Fail to save logical graph {0}:{1}".format(lg_name, str(excmd2))
        finally:
            post_sem.release()
    else:
        response.status = 404
        return "{0}: logical graph {1} not found\n".format(err_prefix, lg_name)

@get('/jsonbody')
def jsonbody_get():
    """
    Return JSON representation of the logical graph
    """
    #print "get jsonbody is called"
    lg_name = request.query.get('lg_name')
    if (lg_name is None or len(lg_name) == 0):
        all_lgs = lg_repo_contents()
        first_dir = next(iter(all_lgs))
        first_lg = first_dir + '/' + all_lgs[first_dir][0]
        lg_name = first_lg

    if (lg_exists(lg_name)):
        #print "Loading {0}".format(lg_name)
        lgp = lg_path(lg_name)
        with open(lgp, "r") as f:
            data = f.read()
        return data
    else:
        response.status = 404
        return "{0}: JSON graph {1} not found\n".format(err_prefix, lg_name)

@get('/pgt_jsonbody')
def pgtjsonbody_get():
    """
    Return JSON representation of the logical graph
    """
    #print "get jsonbody is called"
    pgt_name = request.query.get('pgt_name')
    if (pgt_exists(pgt_name)):
        #print "Loading {0}".format(lg_name)
        pgt = pgt_path(pgt_name)
        with open(pgt, "r") as f:
            data = f.read()
        return data
    else:
        response.status = 404
        return "{0}: JSON graph {1} not found\n".format(err_prefix, pgt_name)

@get('/lg_editor')
def load_lg_editor():
    """
    Let the LG editor load the specified logical graph JSON representation
    """
    lg_name = request.query.get('lg_name')
    if (lg_name is None or len(lg_name) == 0):
        all_lgs = lg_repo_contents()
        first_dir = next(iter(all_lgs))
        lg_name = first_dir + '/' + all_lgs[first_dir][0]

    if (lg_exists(lg_name)):
        tpl = file_as_string('lg_editor.html')
        all_lgs = lg_repo_contents();
        return template(tpl, lg_json_name=lg_name, all_lgs=json.dumps(all_lgs))
    else:
        response.status = 404
        return "{0}: logical graph {1} not found\n".format(err_prefix, lg_name)

@get('/pg_viewer')
def load_pg_viewer():
    """
    RESTful interface for loading the Physical Graph Viewer
    """
    pgt_name = request.query.get('pgt_view_name')
    if (pgt_name is None or len(pgt_name) == 0):
        all_pgts = pgt_repo_contents()
        print(all_pgts)
        first_dir = next(iter(all_pgts))
        pgt_name = first_dir + '/' + all_pgts[first_dir][0]

    if pgt_exists(pgt_name):
        tpl = file_as_string('pg_viewer.html')
        return template(tpl, pgt_view_json_name=pgt_name, is_partition_page='', partition_info='')
    else:
        response.status = 404
        return "{0}: physical graph template (view) {1} not found\n".format(err_prefix, pgt_name)

@get('/show_gantt_chart')
def show_gantt_chart():
    """
    Restful interface to show the gantt chart
    """
    pgt_id = request.query.get('pgt_id')
    tpl = file_as_string('matrix_vis.html')
    return template(tpl, pgt_view_json_name=pgt_id, vis_action="pgt_gantt_chart")

@get('/pgt_gantt_chart')
def get_gantt_chart():
    """
    RESTful interface to retrieve a Gantt Chart matrix associated with a PGT
    """
    pgt_id = request.query.get('pgt_id')
    try:
        ret = pg_mgr.get_gantt_chart(pgt_id)
        return ret
    except GraphException as ge:
        response.status = 500
        return "Failt to get Gantt chart for {0}: {1}".format(pgt_id, ge)

@get('/show_schedule_mat')
def show_schedule_mat():
    """
    Restful interface to show the gantt chart
    """
    pgt_id = request.query.get('pgt_id')
    tpl = file_as_string('matrix_vis.html')
    return template(tpl, pgt_view_json_name=pgt_id, vis_action="pgt_schedule_mat")

@get('/pgt_schedule_mat')
def get_schedule_mat():
    """
    RESTful interface to retrieve a list of schedule matrices
    associated with a PGT
    """
    pgt_id = request.query.get('pgt_id')
    try:
        ret = pg_mgr.get_schedule_matrices(pgt_id)
        return ret
    except GraphException as ge:
        response.status = "500 {0}".format(ge)
        return "Failt to get Gantt chart for {0}: {1}".format(pgt_id, ge)
    except Exception:
        response.status = "500 {0}".format(ge)

@get('/gen_pg')
def gen_pg():
    """
    RESTful interface to convert a PGT(P) into PG by mapping
    PGT(P) onto a given set of available resources
    """
    pgt_id = request.query.get('pgt_id')
    pgtp = pg_mgr.get_pgt(pgt_id)
    if (pgtp is None):
        response.status = 404
        return "PGT(P) with id {0} not found in the Physical Graph Manager".format(pgt_id)

    mhost = request.query.get('dlg_mgr_host')
    if (mhost is None):
        response.status = 500
        return "Must specify DALiUGE manager host"
    try:
        mport = int(request.query.get('dlg_mgr_port'))
        mgr_client = CompositeManagerClient(host=mhost, port=mport, timeout=30)
        # 1. get a list of nodes
        node_list = mgr_client.nodes()
        # 2. mapping PGTP to resources (node list)
        pg_spec = pgtp.to_pg_spec([mhost] + node_list, ret_str=False)
        dt = datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%S.%f')
        ssid = "{0}_{1}".format(pgt_id.split('.json')[0].split('_pgt')[0].split('/')[-1], dt)
        mgr_client.create_session(ssid)
        #print "session created"
        mgr_client.append_graph(ssid, pg_spec)
        #print "graph appended"
        completed_uids = droputils.get_roots(pg_spec)
        mgr_client.deploy_session(ssid, completed_uids=completed_uids)
        #mgr_client.deploy_session(ssid, completed_uids=[])
        #print "session deployed"
        # 3. redirect to the master drop manager
        redirect("http://{0}:{1}/session?sessionId={2}".format(mhost, mport, ssid))
    except RestClientException as re:
        response.status = 500
        return "Fail to interact with DALiUGE Drop Manager: {0}".format(re)
    except HTTPResponse:
        raise
    except Exception as ex:
        response.status = 500
        print(traceback.format_exc())
        return "Fail to deploy physical graph: {0}".format(ex)

@get('/gen_pgt')
def gen_pgt():
    """
    RESTful interface for translating Logical Graphs to Physical Graphs
    """
    lg_name = request.query.get('lg_name')
    if (lg_exists(lg_name)):
        try:
            lg = LG(lg_path(lg_name))
            drop_list = lg.unroll_to_tpl()
            part = request.query.get('num_par')
            try:
                #print('num_islands', request.query.get('num_islands'))
                num_islands = int(request.query.get('num_islands'))
            except:
                num_islands = 0
            mpp = num_islands > 0
            if (part is None):
                is_part = ''
                pgt = PGT(drop_list)
            else:
                is_part = 'Partition'
                par_label = request.query.get('par_label')
                algo = request.query.get('algo')
                if ('metis' == algo):
                    min_goal = int(request.query.get('min_goal'))
                    ptype = int(request.query.get('ptype'))
                    ufactor = 100 - int(request.query.get('max_load_imb')) + 1
                    if (ufactor <= 0):
                        ufactor = 1
                    pgt = MetisPGTP(drop_list, int(part), min_goal, par_label, ptype, ufactor, merge_parts=mpp)
                elif ('mysarkar' == algo):
                    pgt = MySarkarPGTP(drop_list, int(part), par_label, int(request.query.get('max_dop')), merge_parts=mpp)
                elif ('min_num_parts' == algo):
                    time_greedy = 1 - float(request.query.get('time_greedy')) / 100.0 # assuming between 1 to 100
                    pgt = MinNumPartsPGTP(drop_list, int(request.query.get('deadline')),
                    int(part), par_label, int(request.query.get('max_dop')),
                    merge_parts=mpp, optimistic_factor=time_greedy)
                elif ('pso' == algo):
                    params = ['deadline', 'topk', 'swarm_size']
                    pars = [None, 30, 40]
                    for i, para in enumerate(params):
                        try:
                            pars[i] = int(request.query.get(para))
                        except:
                            continue
                    pgt = PSOPGTP(drop_list, par_label, int(request.query.get('max_dop')),
                    deadline=pars[0], topk=pars[1], swarm_size=pars[2], merge_parts=mpp)
                else:
                    raise GraphException("Unknown partition algorithm: {0}".format(algo))
            if (mpp):
                pgt_id = pg_mgr.add_pgt(pgt, lg_name, num_islands=num_islands)
                """
                if ('mysarkar' == algo):
                    pgt_id = pg_mgr.add_pgt(pgt, lg_name, num_islands=int(part))
                elif ('metis' == algo):
                """
            else:
                pgt_id = pg_mgr.add_pgt(pgt, lg_name)
            part_info = ' - '.join(['{0}:{1}'.format(k, v) for k, v in pgt.result().items()])
            tpl = file_as_string('pg_viewer.html')
            return template(tpl, pgt_view_json_name=pgt_id, partition_info=part_info, is_partition_page=is_part)
        except GraphException as ge:
            response.status = 500
            return "Invalid Logical Graph {1}: {0}".format(str(ge), lg_name)
        except SchedulerException as se:
            response.status = 500
            return "Graph scheduling exception {1}: {0}".format(str(se), lg_name)
        except Exception as exp:
            response.status = 500
            trace_msg = traceback.format_exc()
            return "Graph partition exception {1}: {0}".format(trace_msg, lg_name)
    else:
        response.status = 404
        return "{0}: logical graph {1} not found\n".format(err_prefix, lg_name)

@get('/')
def root():
    redirect('/lg_editor')

def run(parser, args):

    epilog = \
"""
If you have no Logical Graphs yet and want to see some you can grab a copy
of those maintained at:

https://github.com/ICRAR/daliuge-logical-graphs

"""

    class NoFormattedEpilogParser(optparse.OptionParser):
        def format_epilog(self, formatter):
            return self.epilog

    # Ignore the previous parser, it's only passed down for convenience
    # and to avoid duplicate descriptions (which in this case we'll have)
    parser = NoFormattedEpilogParser(description="A Web server for the Logical Graph Editor", epilog=epilog)
    parser.add_option("-d", "--lgdir", action="store", type="string", dest="lg_path",
                          help="A path that contains at least one sub-directory, which contains logical graph files")
    parser.add_option("-t", "--pgtdir", action="store", type="string", dest="pgt_path",
                          help="physical graph template path (output)")
    parser.add_option("-H", "--host", action="store", type="string", dest="host", default='0.0.0.0',
                      help="logical graph editor host (all by default)")
    parser.add_option("-p", "--port", action="store", type="int", dest="port", default=8084,
                      help="logical graph editor port (8084 by default)")

    (options, args) = parser.parse_args(args)

    if options.lg_path is None or options.pgt_path is None:
        parser.error("Graph paths missing (-d/-t)")
    elif not os.path.exists(options.lg_path):
        parser.error("{0} does not exist.".format(options.lg_path))

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

    # catch SIGTERM as SIGINT
    signal.signal(signal.SIGTERM, lambda x,y: os.kill(os.getpid(), signal.SIGINT))

    bottle.run(host=options.host, server='wsgiref', port=options.port, debug=False,
        server_class=restutils.ThreadingWSGIServer, handler_class=restutils.LoggingWSGIRequestHandler)
