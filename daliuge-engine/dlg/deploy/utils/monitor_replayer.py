#!/usr/bin/python
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
"""
This module replays the logs produced by the local monitor, which collects
graph status on supercomputers

this module depends on graphviz, which by default is not installed together
with Daliuge. To run this module, please
1. install graphviz
2. pip install pygraphviz

this module also depends on networkx (included in Daliuge), which produces the
edge list that becomes input for gephi vis tool.
"""
import pygraphviz as pgv
import networkx as nx
import json, os, logging, optparse, sys, commands, filecmp
from collections import defaultdict
from datetime import datetime as dt
from xml.etree.ElementTree import ElementTree
import sqlite3 as dbdrv
import numpy as np

logger = logging.getLogger(__name__)

ORIGINAL_COLOR = (87, 87, 87)
YELLOW_COLOR = (255, 255, 0)
# GREEN_COLOR = (102, 255, 178)
GREEN_COLOR = (0, 255, 0)
RED_COLOR = (255, 0, 0)
BLUE_COLOR = (102, 178, 255)

# TODO place java class in the current dir, and include it in Git repo
java_cmd = "java -classpath /tmp/classes:/Users/Chen/proj/gephi-toolkit/gephi-toolkit-0.9.1-all.jar dlg.deploy.utils.export_graph"

sql_create_status = """\
create table ac(ts integer, oid varchar(256), status integer);
create index ac_ts_index on ac(ts);
.separator ","
.import {0} ac
"""

sql_query = """\
SELECT max(ts), oid, status FROM ac
WHERE ts >= {0} and ts < {1}
GROUP BY oid
"""


class GraphPlayer(object):
    def __init__(self, graph_path, status_path):
        for fp in [graph_path, status_path]:
            if fp is None:
                raise Exception("JSON file is none!")
            if not os.path.exists(fp):
                raise Exception("JSON file not found: {0}".format(fp))
        self.oid_gnid_dict = dict()
        self.node_graph_dict = defaultdict(
            list
        )  # k - node-ip, v - a list of tuple of (graph node_id, downstream drop ids)
        self.gnid_ip_dict = dict()
        self.status_path = status_path
        with open(graph_path) as f:
            logger.info("Loading graph from file {0}".format(graph_path))
            self.pg_spec = json.load(f)["g"]
        for i, dropspec in enumerate(self.pg_spec.values()):
            gnid = str(i)
            self.oid_gnid_dict[dropspec["oid"]] = gnid
            ip = dropspec["node"]
            self.node_graph_dict[ip].append(
                (gnid, self.get_downstream_drop_ids(dropspec))
            )
            self.gnid_ip_dict[gnid] = ip
        logger.info("oid to gid mapping done")

    def status_to_colour(self, st):
        st = int(st)
        if st == 0:  # INITIALIZED or NOT_RUN
            r, g, b = ORIGINAL_COLOR
        elif st == 1:  # WRITING or RUNNING
            # logger.debug("running")
            r, g, b = YELLOW_COLOR
        elif st == 2:  # COMPLETED or FINISHED
            # logger.debug("completed")
            r, g, b = GREEN_COLOR
        elif st == 3:  # ERROR
            # logger.debug("error")
            r, g, b = RED_COLOR
        else:
            r, g, b = BLUE_COLOR
        return (r, g, b)

    def convert_to_gexf_status(self, oid, status, colour_dict):
        """
        insert gexf colour line, e.g.
            <viz:color r="13" g="0" b="92"></viz:color>
        into the colour_dict using gnid as the key
        """
        gnid = self.oid_gnid_dict[oid]
        if "execStatus" in status:
            st = status["execStatus"]
        else:
            st = status["status"]
        r, g, b = self.status_to_colour(st)
        colour_dict[gnid] = '<viz:color r="%d" g="%d" b="%d"></viz:color>' % (r, g, b)

    def parse_status(self, gexf_file, out_dir=None, remove_gexf=False):
        """
        Parse the graph status json file and
        produce gexf file for each status line
        """
        if gexf_file is None or (not os.path.exists(gexf_file)):
            raise Exception("GEXF file {0} not found".format(gexf_file))
        if out_dir is None:
            out_dir = os.path.dirname(gexf_file)
        with open(gexf_file) as gf:
            gexf_list = gf.readlines()
        logger.info("Gexf file '{0}' loaded".format(gexf_file))
        with open(self.status_path) as f:
            for i, line in enumerate(f):
                colour_dict = dict()
                colour_dict["{}"] = '<viz:color r="%d" g="%d" b="%d"></viz:color>\n' % (
                    ORIGINAL_COLOR[0],
                    ORIGINAL_COLOR[1],
                    ORIGINAL_COLOR[2],
                )
                output_lines = []
                pgs = json.loads(line)
                ts = pgs["ts"]
                gs = pgs["gs"]
                for oid, status in gs.iteritems():
                    self.convert_to_gexf_status(oid, status, colour_dict)
                node_id = None
                nodes_done = False
                for gexf_line in gexf_list:
                    if (not nodes_done) and gexf_line.find("<edges>") > -1:
                        nodes_done = True
                    if nodes_done:
                        output_lines.append(gexf_line)
                    else:
                        if gexf_line.find("<viz:color") > -1:
                            # logger.debug("Starting with viz color")
                            if node_id is None:
                                raise Exception("wrong order")
                            output_lines.append(colour_dict[node_id])
                        else:
                            if gexf_line.find("<node id=") > -1:
                                # <node id="38345" label="38345">
                                # 38345
                                node_id = gexf_line.split()[1].split("=")[1][1:-1]
                            output_lines.append(gexf_line)
                # each line generate a new file
                new_gexf = "{0}/{1}.gexf".format(out_dir, ts)
                with open(new_gexf, "w") as fo:
                    for new_line in output_lines:
                        # fo.write('{0}{1}'.format(new_line, os.linesep))
                        # fo.write('{0}{1}'.format(new_line, '\n'))
                        fo.write(new_line)
                logger.info("GEXF file '{0}' is generated".format(new_gexf))
                new_png = new_gexf.split(".gexf")[0] + ".png"
                cmd = "{0} {1} {2}".format(java_cmd, new_gexf, new_png)
                ret = commands.getstatusoutput(cmd)
                if ret[0] != 0:
                    logger.error(
                        "Fail to print png from %s to %s: %s"
                        % (new_gexf, new_png, ret[1])
                    )
                del colour_dict
                if remove_gexf:
                    try:
                        os.remove(new_gexf)
                    except:
                        pass

    def get_downstream_drop_ids(self, dropspec):
        if dropspec["type"] == "app":
            ds_kw = "outputs"  # down stream key word
        elif dropspec["type"] == "plain":
            ds_kw = "consumers"
        else:
            ds_kw = "None"
        if ds_kw in dropspec:
            return dropspec[ds_kw]
        else:
            return []

    def build_node_graph(self):
        """
        A graph contains all compute nodes and their relationships
        """
        G = pgv.AGraph(strict=False, directed=True)
        temp_dict = defaultdict(int)  # key - from_to_ip, val - counter

        for i, ip in enumerate(self.node_graph_dict.keys()):
            G.add_node(ip, shape="rect", label="%d" % i)
        logger.info("All nodes added")

        for ip, droplist in self.node_graph_dict.iteritems():
            for gnid, dropids in droplist:
                for did in dropids:
                    tip = self.gnid_ip_dict[self.oid_gnid_dict[did]]
                    k = "{0}_{1}".format(ip, tip)
                    temp_dict[k] += 1

        for k, v in temp_dict.iteritems():
            ks = k.split("_")
            G.add_edge(ks[0], ks[1], weight=v)

        return G

    def get_state_changes(self, gexf_file, grep_log_file, steps=400, out_dir=None):
        """
        grep -R "changed to state" --include=*.log . > statelog.txt
        the simulation time will be evenly divided up by "steps"
        """
        # convert grep_log_file to csv
        if out_dir is None:
            out_dir = os.path.dirname(gexf_file)
        # grep_log_file = '{0}/statelog.txt'.format(out_dir)
        # cmd = ""
        csv_file = "{0}/csv_file.csv".format(out_dir)
        sqlite_file = "{0}/sqlite_file.sqlite".format(out_dir)

        if not os.path.exists(csv_file):
            with open(grep_log_file, "r") as f:
                alllines = f.readlines()
            with open(csv_file, "w") as fo:
                for line in alllines:
                    ts = line.split("[DEBUG]")[0].split("dlgNM.log:")[1].strip()
                    ts = int(dt.strptime(ts, "%Y-%m-%d %H:%M:%S,%f").strftime("%s"))
                    oid = line.split("oid=")[1].split()[0]
                    state = line.split()[-1]
                    fo.write("{0},{1},{2},{3}".format(ts, oid, state, os.linesep))
        else:
            logger.info("csv file already exists: {0}".format(csv_file))

        if not os.path.exists(sqlite_file):
            sql = sql_create_status.format(csv_file)
            sql_file = csv_file.replace(".csv", ".sql")
            with open(sql_file, "w") as fo:
                fo.write(sql)
            cmd = "sqlite3 {0} < {1}".format(sqlite_file, sql_file)
            ret = commands.getstatusoutput(cmd)
            if ret[0] != 0:
                logger.error("fail to create sqlite: {0}".format(ret[1]))
                return
        else:
            logger.info("sqlite file already exists: {0}".format(sqlite_file))

        dbconn = dbdrv.connect(sqlite_file)
        q = "SELECT min(ts) from ac"
        cur = dbconn.cursor()
        cur.execute(q)
        lhs = cur.fetchall()[0][0]
        cur.close()

        q = "SELECT max(ts) from ac"
        cur = dbconn.cursor()
        cur.execute(q)
        rhs = cur.fetchall()[0][0]
        cur.close()

        step_size = (rhs - lhs) / steps
        if step_size == 0:
            return
        lr = list(np.arange(lhs, rhs, step_size))
        if lr[-1] != rhs:
            lr.append(rhs)

        last_gexf = gexf_file
        for i, el in enumerate(lr[0:-1]):
            a = el
            b = lr[i + 1]
            step_name = "{0}-{1}".format(a, b)
            logger.debug("stepname: %s" % step_name)
            new_gexf = "{0}/{1}.gexf".format(out_dir, step_name)
            if os.path.exists(new_gexf):
                logger.info("{0} already exists, ignore".format(new_gexf))
                last_gexf = new_gexf
                continue
            sql = sql_query.format(a, b)
            logger.debug(sql)
            cur = dbconn.cursor()
            cur.execute(sql)
            drops = cur.fetchall()
            cur.close()

            # build a dictionary
            drop_dict = dict()  # key - gnid, value: drop status
            for drop in drops:
                gnid = self.oid_gnid_dict[drop[1]]
                drop_dict[gnid] = drop[2]

            tree = ElementTree()
            tree.parse(last_gexf)
            root = tree.getroot()
            for node in root.iter("{http://www.gexf.net/1.3}node"):
                gnid = node.attrib["id"]
                if not gnid in drop_dict:
                    continue
                new_status = drop_dict[gnid]
                r, g, b = self.status_to_colour(new_status)
                colour = node[2]
                colour.attrib["r"] = "{0}".format(r)
                colour.attrib["g"] = "{0}".format(g)
                colour.attrib["b"] = "{0}".format(b)
            tree.write(new_gexf)
            logger.info("GEXF file '{0}' is generated".format(new_gexf))
            del drop_dict
            if not filecmp.cmp(last_gexf, new_gexf, False):
                new_png = new_gexf.split(".gexf")[0] + ".png"
                cmd = "{0} {1} {2}".format(java_cmd, new_gexf, new_png)
                ret = commands.getstatusoutput(cmd)
            else:
                logger.info("Identical {0} == {1}".format(new_gexf, last_gexf))
            last_gexf = new_gexf
            if ret[0] != 0:
                logger.error(
                    "Fail to print png from %s to %s: %s" % (last_gexf, new_png, ret[1])
                )

    def build_drop_subgraphs(self, node_range="[0:20]"):
        pass

    def build_drop_fullgraphs(self, do_subgraph=False, graph_lib="pygraphviz"):
        """
        this only works for small graphs, no way for # of drops > 1000
        """
        if "pygraphviz" == graph_lib:
            G = pgv.AGraph(strict=True, directed=True)
        else:
            G = nx.Graph()
            do_subgraph = False
        subgraph_dict = defaultdict(list)  # k - node-ip, v - a list of graph nodes
        oid_gnid_dict = dict()

        for i, oid in enumerate(self.pg_spec.keys()):
            oid_gnid_dict[oid] = str(i)
        logger.info("oid to gid mapping done")

        for dropspec in self.pg_spec.itervalues():
            gid = oid_gnid_dict[dropspec["oid"]]
            ip = dropspec["node"]
            subgraph_dict[ip].append(gid)
            if dropspec["type"] == "app":
                G.add_node(
                    gid, shape="rect", label=""
                )  # , fixedsize=True, hight=.05, width=.05)
            elif dropspec["type"] == "plain":  # parallelogram
                G.add_node(
                    gid, shape="circle", label=""
                )  # , fixedsize=True, hight=.05, width=.05)
        logger.info("Graph nodes added")

        for dropspec in self.pg_spec.itervalues():
            gid = oid_gnid_dict[dropspec["oid"]]
            if dropspec["type"] == "app":
                ds_kw = "outputs"  # down stream key word
            elif dropspec["type"] == "plain":
                ds_kw = "consumers"
            else:
                ds_kw = "None"
            if ds_kw in dropspec:
                for doid in dropspec[ds_kw]:
                    G.add_edge(gid, oid_gnid_dict[doid])
        logger.info("Graph edges added")

        if do_subgraph:
            for i, subgraph_nodes in enumerate(subgraph_dict.values()):
                # we don't care about the subgraph label or rank
                subgraph = G.add_subgraph(
                    subgraph_nodes, label="%d" % i, name="cluster_%d" % i, rank="same"
                )
                subgraph.graph_attr["rank"] = "same"
            logger.info("Subgraph added")

        return G


if __name__ == "__main__":
    """
    1. create edge list from 'monitor_g.log'
    2. run gephi manully to (1) finalise the layout, and
                            (2) store the layout in the gexf file
    3. alternatively, one can use existing gexf file from previous runs

    e.g. python monitor_replayer.py -x /Users/Chen/Documents/FromWeb/Galaxy/2016-06-30/galaxy.gexf
    -r /Users/Chen/Documents/FromWeb/Galaxy/2016-07-15/logs/2016-07-15_11-59-21/statelog.txt
    -u /Users/Chen/Documents/FromWeb/Galaxy/2016-07-15/gen -o rrr -d eee
    -l /Users/Chen/Documents/FromWeb/Galaxy/2016-07-15/vis.log
    -g /Users/Chen/Documents/FromWeb/Galaxy/2016-07-15/logs/2016-07-15_11-59-21/0/monitor_g.log
    -t /Users/Chen/Documents/FromWeb/Galaxy/2016-07-15/logs/2016-07-15_11-59-21/0/monitor_g.log
    """
    parser = optparse.OptionParser()
    parser.add_option(
        "-g",
        "--graph_path",
        action="store",
        type="string",
        dest="graph_path",
        help="path to physical graph specification",
        default=None,
    )
    parser.add_option(
        "-t",
        "--status_path",
        action="store",
        type="string",
        dest="status_path",
        help="path to physical graph status",
        default=None,
    )
    parser.add_option(
        "-x",
        "--gexf_file",
        action="store",
        type="string",
        dest="gexf_file",
        help="path to gexf file produced from gephi",
        default=None,
    )
    parser.add_option(
        "-u",
        "--gexf_output_dir",
        action="store",
        type="string",
        dest="gexf_output_dir",
        help="path to gexf output directory",
        default=None,
    )
    parser.add_option(
        "-l",
        "--log_file",
        action="store",
        type="string",
        dest="log_file",
        help="logfile path",
        default=None,
    )
    parser.add_option(
        "-o",
        "--output_file",
        action="store",
        type="string",
        dest="output_file",
        help="output image file",
        default=None,
    )
    parser.add_option(
        "-d",
        "--dot_file",
        action="store",
        type="string",
        dest="dot_file",
        help="output do file",
        default=None,
    )
    parser.add_option(
        "-s",
        "--subgraph",
        action="store_true",
        dest="subgraph",
        help="create subgraph per node",
        default=False,
    )
    parser.add_option(
        "-e",
        "--edgelist",
        action="store_true",
        dest="edgelist",
        help="store edge list instead of dot file",
        default=False,
    )

    parser.add_option(
        "-r",
        "--grep_log_file",
        action="store",
        type="string",
        dest="grep_log_file",
        help="grep log file",
        default=None,
    )

    (options, args) = parser.parse_args()

    if None == options.log_file or None == options.graph_path:
        parser.print_help()
        sys.exit(1)

    FORMAT = "%(asctime)-15s [%(levelname)5.5s] [%(threadName)15.15s] %(name)s#%(funcName)s:%(lineno)s %(message)s"
    logging.basicConfig(filename=options.log_file, level=logging.DEBUG, format=FORMAT)

    if options.edgelist and options.dot_file is not None:
        logger.info("Loading networx graph from file {0}".format(options.graph_path))
        gp = GraphPlayer(options.graph_path, options.status_path)
        g = gp.build_drop_fullgraphs(graph_lib="networkx")
        nx.write_edgelist(g, options.dot_file)
        sys.exit(0)

    if None == options.output_file or None == options.dot_file:
        parser.print_help()
        sys.exit(1)

    gp = GraphPlayer(options.graph_path, options.status_path)
    # gp.parse_status(options.gexf_file, out_dir=options.gexf_output_dir)
    gp.get_state_changes(
        options.gexf_file, options.grep_log_file, out_dir=options.gexf_output_dir
    )
    """
    g = gp.build_node_graph()
    #g = gp.build_drop_fullgraphs(options.graph_path, do_subgraph=options.subgraph)
    g.write(options.dot_file)
    logger.info("Graph viz obj created")
    g.layout(prog='sfdp', args='-Goverlap=prism -Gsize=67!')
    #g.layout(prog='sfdp')
    #g.layout(prog='dot')
    logger.info("Graph viz layout computed")
    g.draw(options.output_file)
    logger.info("Graph viz file drawn to: {0}".format(options.output_file))
    """
