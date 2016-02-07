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
#    chen.wu@icrar.org

import json, decimal, urllib2, time, traceback
import subprocess, commands
import threading
import os, time
from optparse import OptionParser
from bottle import route, run, request, get, static_file, template, redirect, response

from dfms.dropmake.pg_generator import LG, PGT, GraphException, MetisPGTP, PyrrosPGTP, MySarkarPGTP
from dfms.dropmake.scheduler import SchedulerException
from dfms.dropmake.pg_manager import PGManager

#lg_dir = None
post_sem = threading.Semaphore(1)
gen_pgt_sem = threading.Semaphore(1)

err_prefix = "[Error]"
DEFAULT_LG_NAME = "cont_img.json"
DEFAULT_PGT_VIEW_NAME = "lofar_pgt-view.json"
MAX_PGT_FN_CNT= 300
pgt_fn_count = 0

def lg_exists(lg_name):
    return os.path.exists("{0}/{1}".format(lg_dir, lg_name))

@route('/static/<filepath:path>')
def server_static(filepath):
    return static_file(filepath, root='./')

@route('/jsonbody', method='POST')
def jsonbody():
    # see the table in http://bottlepy.org/docs/dev/tutorial.html#html-form-handling
    lg_name = request.POST['lg_name']
    if (lg_exists(lg_name)):
        lg_content = request.POST['lg_content']
        lg_path = "{0}/{1}".format(lg_dir, lg_name)
        post_sem.acquire()
        try:
            # overwrite file on disks
            # print "writing to {0}".format(lg_path)
            with open(lg_path, "w") as f:
                f.write(lg_content)
        except Exception, excmd2:
            response.status = 500
            return "Fail to save logical graph {0}:{1}".format(lg_name, str(excmd2))
        finally:
            post_sem.release()
    else:
        response.status = 404
        return "{0}: logical graph {1} not found\n".format(err_prefix, lg_name)

@get('/jsonbody')
def jsonbody():
    """
    Return JSON representation of the logical graph
    """
    #print "get jsonbody is called"
    lg_name = request.query.get('lg_name')
    if (lg_name is None or len(lg_name) == 0):
        redirect('/jsonbody?lg_name={0}'.format(DEFAULT_LG_NAME))
    if (lg_exists(lg_name)):
        #print "Loading {0}".format(lg_name)
        lg_path = "{0}/{1}".format(lg_dir, lg_name)
        with open(lg_path, "r") as f:
            data = f.read()
        return data
    else:
        response.status = 404
        return "{0}: JSON graph {1} not found\n".format(err_prefix, lg_name)

@get('/lg_editor')
def load_lg_editor():
    """
    Let the LG editor load the specified logical graph JSON representation
    """
    lg_name = request.query.get('lg_name')
    if (lg_name is None or len(lg_name) == 0):
        #lg_name = DEFAULT_LG_NAME
        redirect('/lg_editor?lg_name={0}'.format(DEFAULT_LG_NAME))

    if (lg_exists(lg_name)):
        return template('lg_editor.html', lg_json_name=lg_name)
    else:
        response.status = 404
        return "{0}: logical graph {1} not found\n".format(err_prefix, lg_name)

@get('/pg_viewer')
def load_pg_viewer():
    pgt_name = request.query.get('pgt_view_name')
    if (pgt_name is None or len(pgt_name) == 0):
        redirect('/pg_viewer?pgt_view_name={0}'.format(DEFAULT_PGT_VIEW_NAME))

    if (lg_exists(pgt_name)):
        return template('pg_viewer.html', pgt_view_json_name=pgt_name)
    else:
        response.status = 404
        return "{0}: physical graph template (view) {1} not found\n".format(err_prefix, pgt_name)

@get('/gen_pgt')
def gen_pgt():
    lg_name = request.query.get('lg_name')
    if (lg_exists(lg_name)):
        try:
            lg = LG(lg_name)
            drop_list = lg.unroll_to_tpl()
            part = request.query.get('num_par')
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
                    pgt = MetisPGTP(drop_list, int(part), min_goal, par_label, ptype, ufactor)
                elif ('mysarkar' == algo):
                    mp = request.query.get('merge_par')
                    mpp = True if '1' == mp else False
                    pgt = MySarkarPGTP(drop_list, int(part), par_label, int(request.query.get('max_dop')), merge_parts=mpp)
                elif ('pyrros' == algo):
                    pgt = PyrrosPGTP(drop_list, int(part))
                else:
                    raise GraphException("Unknown partition algorithm: {0}".format(algo))

            pgt_id = pg_mgr.add_pgt(pgt, lg_name)
            part_info = pgt.get_partition_info()
            return template('pg_viewer.html', pgt_view_json_name=pgt_id, partition_info=part_info, is_partition_page=is_part)
        except GraphException, ge:
            response.status = 500
            return "Invalid Logical Graph {1}: {0}".format(str(ge), lg_name)
        except SchedulerException, se:
            response.status = 500
            return "Graph scheduling exception {1}: {0}".format(str(se), lg_name)
        except Exception, exp:
            response.status = 500
            trace_msg = traceback.format_exc()
            print trace_msg
            return "Graph partition exception {1}: {0}".format(str(exp), lg_name)
    else:
        response.status = 404
        return "{0}: logical graph {1} not found\n".format(err_prefix, lg_name)

if __name__ == "__main__":
    """
    e.g. python lg_web -d /tmp/
    """
    parser = OptionParser()
    parser.add_option("-d", "--lgdir", action="store", type="string", dest="lg_path",
                          help="logical graph path (input)")
    parser.add_option("-p", "--port", action="store", type="int", dest="lg_port", default=8084,
                      help="logical graph editor port (8084 by default)")

    (options, args) = parser.parse_args()
    if (None == options.lg_path):
        parser.print_help()
        sys.exit(1)
    elif (not os.path.exists(options.lg_path)):
        print("{0} does not exist.".format(options.lg_path))
        sys.exit(1)

    global lg_dir, pg_mgr
    lg_dir = options.lg_path
    pg_mgr = PGManager(lg_dir)
    # Let's use tornado, since luigi already depends on it
    run(host="0.0.0.0", server='tornado', port=options.lg_port, debug=False)
