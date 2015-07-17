from bottle import route, run, request, get, static_file, template, redirect

from ngas_dm import PGEngine
import json, decimal, urllib2
import sys, os, time, subprocess
from dfms.data_object import AppConsumer, ContainerDataObject
from dfms import doutils

pg_engine = PGEngine()

#my_public_ip = "localhost"
luigi_port = "8082"


from urllib2 import urlopen
my_public_ip = urlopen('http://ip.42.pl/raw').read()


def encode_decimal(obj):
    """
    Just a little helper function for JSON serialisation
    """
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError(repr(obj) + " is not JSON serializable")

def valid_pg_name(pg_name):
    call_nm = "create_{0}_pg".format(pg_name).lower()
    if (not hasattr(pg_engine, call_nm)):
        return False
    return True

def _response_msg(msg):
    """
    show the msg as a response HTML page
    """
    return template('pg_response.html', ret_msg = msg)

def _call_luigi_api(verb, **kwargs):
    # se = '{"task_str":"chenwu"}'
    url = "http://{0}:{1}/api/{2}".format(my_public_ip, luigi_port, verb)
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
        raise Exception("Luigi server at {0} is down".format(my_public_ip))
    except urllib2.HTTPError, httperr:
        raise Exception("Luigi API error: {0}".format(str(httperr)))
    except Exception, ex:
        raise Exception("Fail to query Luigi: {0}".format(str(ex)))

@route('/static/<filepath:path>')
def server_static(filepath):
    return static_file(filepath, root='./')

def _get_json(call_nm, formatted=False):
    pg = getattr(pg_engine, call_nm)()
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
def show_pg():
    """
    Setup web page for displaying the physical graph
    """
    pg_name = request.query.get('pg_name')
    if (not valid_pg_name(pg_name)):
        return _response_msg('Invalid physical graph name {0}'.format(pg_name))
    call_nm = "create_{0}_pg".format(pg_name).lower()
    jsbody = _get_json(call_nm)
    return template('pg_graph_tpl.html', json_workers=jsbody, pg_name=pg_name)

@get('/deploy')
def deploy_pg():
    pg_name = request.query.get('pg_name')
    if (not valid_pg_name(pg_name)):
        return _response_msg('Invalid physical graph name {0}'.format(pg_name))
    ppath = sys.executable
    fpath = os.path.dirname(os.path.abspath(__file__))
    ssid = "{0}-{1}".format(request.remote_addr.replace(".","-"), int(time.time()))
    cmd = "{0} {1}/ngas_dm.py PGDeployTask --PGDeployTask-pg-name {2} --PGDeployTask-session-id {3} --workers 10".format(ppath,
                                                                                                                         fpath,
                                                                                                                         pg_name,
                                                                                                                         ssid)
    #re = commands.getstatusoutput(cmd)
    worker = subprocess.Popen(cmd.split())
    """
    if (re[0] != 0):
        return _response_msg('Fail to deploy pg as Luigi tasks: {0}'.format(re[1]))
    """
    timeout = 60
    for i in range(timeout): # wait until tasks are accepted by the scheduler
        time.sleep(1.0)
        tasks = _call_luigi_api('task_search', task_str=ssid)
        if (len(tasks) > 0):
            break
    redirect("http://{0}:{1}/static/visualiser/index.html#PGDeployTask(session_id={2}, pg_name={3})".format(my_public_ip, luigi_port, ssid, pg_name))

@get('/jsonbody')
def get_json():
    """
    Return JSON representation of the physical graph
    """
    pg_name = request.query.get('pg_name')
    if (not valid_pg_name(pg_name)):
        return _response_msg('Invalid physical graph name {0}'.format(pg_name))
    call_nm = "create_{0}_pg".format(pg_name).lower()
    return template('pg_json_tpl.html', json_body=_get_json(call_nm, formatted=True), pg_name=pg_name)


#===============================================================================
# DataObject JSON serialization methods, originally found in AbstractDataObject
# class and slightly modified afterwards
#===============================================================================
def get_type_code(dataObject):
    if isinstance(dataObject, AppConsumer):
        return 1
    elif isinstance(dataObject, ContainerDataObject):
        return 2
    else:
        return 4

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


if __name__ == "__main__":
    """
    e.g. http://localhost:8081/show?pg_name=chiles
    """
    run(host="0.0.0.0", server='paste', port=8081, debug=True)
    """
    run(host = config.get('Web Server', 'IpAddress'),
        server = 'paste',
        port = config.getint('Web Server', 'Port'),
        debug = config.getboolean('Web Server', 'Debug'))
    """
