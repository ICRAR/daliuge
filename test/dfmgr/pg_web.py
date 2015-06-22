from bottle import route, run, request, get, post, static_file, template, redirect

from ngas_dm import PGEngine, DataFlowException
import json, decimal

pg_engine = PGEngine()

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

@get('/show')
def show_pg():
    """
    Setup web page for displaying the physical graph
    """
    pg_name = request.query.get('pg_name')
    if (not valid_pg_name(pg_name)):
        return _response_msg('Invalid physical graph name {0}'.format(pg_name))
    return template('pg_vis.html', pg_name=pg_name)

@get('/jsonbody')
def get_json():
    """
    Return JSON representation of the physical graph
    """
    pg_name = request.query.get('pg_name')
    if (not valid_pg_name(pg_name)):
        return _response_msg('Invalid physical graph name {0}'.format(pg_name))
    call_nm = "create_{0}_pg".format(pg_name).lower()
    pg = getattr(pg_engine, call_nm)()
    return json.dumps(pg.to_json_obj(), default=encode_decimal)

if __name__ == "__main__":
    run(host="localhost", server='paste', port=8081, debug=True)
    """
    run(host = config.get('Web Server', 'IpAddress'),
        server = 'paste',
        port = config.getint('Web Server', 'Port'),
        debug = config.getboolean('Web Server', 'Debug'))
    """