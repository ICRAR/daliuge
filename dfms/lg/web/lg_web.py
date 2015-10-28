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

import json, decimal, urllib2
import subprocess, commands
import threading
import os, time
from optparse import OptionParser
from bottle import route, run, request, get, static_file, template, redirect, response

#lg_dir = None
post_sem = threading.Semaphore(1)

err_prefix = "[Error]"
DEFAULT_LG_NAME = "cont_img.json"

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
        return "{0}: logical graph {1} not found\n".format(err_prefix, lg_name)

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

    global lg_dir
    lg_dir = options.lg_path
    # Let's use tornado, since luigi already depends on it
    run(host="0.0.0.0", server='tornado', port=options.lg_port, debug=True)