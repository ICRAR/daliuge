
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

"""
Refer to
https://confluence.ska-sdp.org/display/PRODUCTTREE/C.1.2.4.4.4+DFM+Physical+Graph+Manager
"""
import threading, json
import networkx as nx

from dfms.dropmake.pg_generator import GraphException
from dfms.dropmake.scheduler import DAGUtil, SchedulerException

MAX_PGT_FN_CNT = 300

class PGManager(object):
    """
    Physical Graph Manager
    """
    def __init__(self, root_dir):
        self._pgt_dict = dict()
        self._pgt_fn_count = 0
        self._gen_pgt_sem = threading.Semaphore(1)
        self._root_dir = root_dir

    def add_pgt(self, pgt, lg_name):
        """
        Dummy impl. using file system for now (thread safe)
        TODO - use proper graph databases to manage all PGTs

        Return:
            A unique PGT id (handle)
        """
        self._gen_pgt_sem.acquire()
        self._pgt_fn_count += 1
        if (self._pgt_fn_count == MAX_PGT_FN_CNT + 1):
            self._pgt_fn_count = 0
        pgt_id = lg_name.replace(".json", "{0}_pgt.json".format(self._pgt_fn_count))
        pgt_path = "{0}/{1}".format(self._root_dir, pgt_id)
        pgt_content = pgt.json
        try:
            # overwrite file on disks
            with open(pgt_path, "w") as f:
                f.write(pgt_content)
            self._pgt_dict[pgt_id] = pgt
        except Exception, exp:
            raise GraphException("Fail to save PGT {0}:{1}".format(pgt_path, str(exp)))
        finally:
            self._gen_pgt_sem.release()
        return pgt_id

    def get_pgt(self, pgt_id):
        """
        Return:
            The PGT object given its PGT id
        """
        return self._pgt_dict.get(pgt_id, None)

    def get_gantt_chart(self, pgt_id, json_str=True):
        """
        Return:
            the gantt chart matrix (numarray) given a PGT id
        """
        pgt = self.get_pgt(pgt_id)
        if (pgt is None):
            raise GraphException("PGT {0} not found".format(pgt_id))
        try:
            gcm = DAGUtil.ganttchart_matrix(pgt.dag)
        except SchedulerException, se:
            topo_sort = nx.topological_sort(pgt.dag)
            DAGUtil.label_schedule(pgt.dag, topo_sort=topo_sort)
            gcm = DAGUtil.ganttchart_matrix(pgt.dag, topo_sort=topo_sort)

        if (json_str):
            gcm = json.dumps(gcm.tolist())
        return gcm

    def get_schedule_matrices(self, pgt_id, json_str=True):
        """
        Return:
            a list of schedule matrices (numarrays) given a PGT id
        """
        pgt = self.get_pgt(pgt_id)
        if (pgt is None):
            raise GraphException("PGT {0} not found".format(pgt_id))
        jsobj = []
        for part in pgt._partitions:
            sm = part.schedule.schedule_matrix
            if (json_str):
                sm = sm.tolist()
            jsobj.append(sm)
        if (json_str):
            jsobj = json.dumps(jsobj)
        return jsobj
