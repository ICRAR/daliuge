from bottle import route, run, request, get, post, static_file, template, redirect

from ngas_dm import PGEngine, DataFlowException
import json, decimal
import sys, commands, os, time

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

@route('/static/<filepath:path>')
def server_static(filepath):
    return static_file(filepath, root='./')

def _get_json(call_nm):
    pg = getattr(pg_engine, call_nm)()
    return json.dumps(pg.to_json_obj(), default=encode_decimal)

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
    cmd = "{0} {1}/ngas_dm.py PGDeployTask --PGDeployTask-pg-name {2} --PGDeployTask-session-id {3}".format(ppath,
                                                                                             fpath,
                                                                                             pg_name,
                                                                                             ssid)
    re = commands.getstatusoutput(cmd)
    if (re[0] != 0):
        return _response_msg('Fail to deploy pg as Luigi tasks: {0}'.format(re[1]))
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
    return _get_json(call_nm)
    """
    pg = getattr(pg_engine, call_nm)()
    return json.dumps(pg.to_json_obj(), default=encode_decimal)
    """

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