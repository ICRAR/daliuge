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
import json, decimal
import subprocess
from test import graphsRepository
import threading
import time

from bottle import route, run, request, get, static_file, template, redirect

from dfms import doutils
from dfms.data_object import ContainerDataObject, SocketListener, AppDataObject
from dfms.luigi_int import FinishGraphExecution


def encode_decimal(obj):
    """
    Just a little helper function for JSON serialisation
    """
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError(repr(obj) + " is not JSON serializable")

def valid_pg_name(pg_name):
    if (not hasattr(graphsRepository, pg_name)):
        return False
    return True

def _response_msg(msg):
    """
    show the msg as a response HTML page
    """
    return template('pg_response.html', ret_msg = msg)

@route('/static/<filepath:path>')
def server_static(filepath):
    return static_file(filepath, root='./')

def _get_json(call_nm, formatted=False):
    pg = getattr(graphsRepository, call_nm)()
    if not isinstance(pg, list):
        pg = [pg]

    # Reduce all dictionaries to a single one:
    allDOsDict = {}
    for doLeaf in pg:
        to_json_obj(doLeaf, allDOsDict)

    if (formatted):
        return json.dumps(allDOsDict, default=encode_decimal, sort_keys=True)
    else:
        return json.dumps(allDOsDict, default=encode_decimal)

@get('/show')
def show():
    """
    Setup web page for displaying the physical graph
    """
    pg_name = request.query.get('pg_name')
    pg_layout = request.query.get('pg_layout')

    if (pg_layout == 'TD'):
        pg_rotate = 'LR'
    else:
        pg_layout = 'LR'
        pg_rotate = 'TD'

    if (not valid_pg_name(pg_name)):
        allNames = list(graphsRepository.listGraphFunctions())
        return _response_msg('Invalid physical graph name %s. Valid names are: %s' % (pg_name, allNames))
    jsbody = _get_json(pg_name)
    return template('pg_graph_tpl.html', json_workers=jsbody, pg_name=pg_name, pg_layout=pg_layout, pg_rotate=pg_rotate)

@get('/execute')
def execute():
    pg_name = request.query.get('pg_name')

    if (not valid_pg_name(pg_name)):
        allNames = list(graphsRepository.listGraphFunctions())
        return _response_msg('Invalid physical graph name %s. Valid names are: %s' % (pg_name, allNames))

    luigi_host = request.environ['HTTP_HOST'].split(":")[0] # e.g. 192.168.1.1:8081
    luigi_port = 8082
    from luigi import worker

    # See if the central scheduler is already running; if it's not start it
    from luigi import rpc
    from urllib2 import urlopen
    try:
        urlopen("http://{0}:{1}".format(luigi_host, luigi_port), timeout=2)
    except Exception, exp:
        # The only way to start a luigi server is to do it in a separate process,
        # because the luigi.server.run method sets up signal traps, which can
        # only be done in the main thread. There is a second option for starting
        # the server programmatically through the luigi.cmdline.luigid function
        # with --background, but that kills the current process because of the
        # "daemonization" process
        print str(exp)
        argv = ['luigid', '--background', '--logdir', '.', '--address', '0.0.0.0', '--port', str(luigi_port)]
        subprocess.Popen(argv)
        time.sleep(1)

    # Submit the new task and run it asynchronously so we can redirect
    # the user to luigi's graph visualization
    ssid = time.time()
    pgCreator="test.graphsRepository.%s" % (pg_name)

    # TODO: Not exactly thread-safe here...
    SocketListener._dryRun = False
    task = FinishGraphExecution(pgCreator=pgCreator, sessionId=ssid)
    SocketListener._dryRun = True

    no_workers = 5 # parallel workers
    for i in range(no_workers):
        # default worker id could be duplicated, so create unqiue ones
        worker_id = "worker_{0}_{1}_{2}".format(luigi_host, ssid, i)
        w = worker.Worker(scheduler=rpc.RemoteScheduler(port=luigi_port), worker_id=worker_id)
        w.add(task) # share the same task and its dependent tasks, thus parallel
        t = threading.Thread(None, lambda: w.run(), 'work-runner')
        t.daemon = True
        t.start()

    #my_public_ip = urlopen('http://ip.42.pl/raw').read()
    redirect("http://{0}:{1}/static/visualiser/index.html#FinishGraphExecution(sessionId={2}, pgCreator={3})".format(luigi_host, luigi_port, ssid, pgCreator))

@get('/trigger_sl')
def trigger_socklstn():
    port_num = request.query.get('port_number')
    return "Port {0} is triggered.".format(port_num)

@get('/jsonbody')
def jsonbody():
    """
    Return JSON representation of the physical graph
    """
    pg_name = request.query.get('pg_name')
    if (not valid_pg_name(pg_name)):
        return _response_msg('Invalid physical graph name {0}'.format(pg_name))
    return template('pg_json_tpl.html', json_body=_get_json(pg_name, formatted=True), pg_name=pg_name)

@get('/')
def root():
    """
    Lists all the graphs contained in the graphsRepository module
    """
    return template('graphs_list.html', graphNames=graphsRepository.listGraphFunctions())

#===============================================================================
# DataObject JSON serialization methods, originally found in AbstractDataObject
# class and slightly modified afterwards
#===============================================================================
def get_type_code(dataObject):
    if isinstance(dataObject, AppDataObject):
        return 4
    elif isinstance(dataObject, ContainerDataObject):
        return 2
    elif isinstance(dataObject, SocketListener):
        return 3
    else:
        return 1

def to_json_obj(dataObject, allDOsDict):
    """
    JSON serialisation of a DataObject for displaying with dagreD3. Its
    implementation should be similar to the DataObjectTask for Luigi, since both
    should represent the same dependencies
    """
    # Already visited
    if dataObject.oid in allDOsDict:
        return

    doDict = {
        'type': get_type_code(dataObject),
        'loc': dataObject.location
    }
    inputQueue = [{'oid': uobj.oid} for uobj in doutils.getUpstreamObjects(dataObject)]
    if inputQueue:
        doDict['inputQueue'] = inputQueue

    allDOsDict[dataObject.oid] = doDict

    for dob in doutils.getDownstreamObjects(dataObject):
        to_json_obj(dob, allDOsDict)

def _call_luigi_api(verb, luigi_ip, luigi_port, **kwargs):
    """
    e.g   tasks = _call_luigi_api('task_search', task_str=ssid)
          print len(tasks)
    """
    # se = '{"task_str":"chenwu"}'
    url = "http://{0}:{1}/api/{2}".format(luigi_ip, luigi_port, verb)
    if (len(kwargs) > 0):
        url += "?data={0}".format(urllib2.quote(json.dumps(kwargs)))
    try:
        re = urllib2.urlopen(url).read()
        jre = json.loads(re)
        if (jre.has_key('response')):
            return jre['response']
        else:
            raise Exception("Invalid reply from Luigi: {0}".format(re))
    except urllib2.URLError, urlerr:
        raise Exception("Luigi server at {0} is down".format(luigi_ip))
    except urllib2.HTTPError, httperr:
        raise Exception("Luigi API error: {0}".format(str(httperr)))
    except Exception, ex:
        raise Exception("Fail to query Luigi: {0}".format(str(ex)))


if __name__ == "__main__":
    """
    e.g. http://localhost:8081/show?pg_name=chiles
    """
    SocketListener._dryRun = True
    # Let's use tornado, since luigi already depends on it
    run(host="0.0.0.0", server='tornado', port=8081, debug=True)
