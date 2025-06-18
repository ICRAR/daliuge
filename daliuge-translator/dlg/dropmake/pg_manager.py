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
import json
import os
import threading

import numpy as np

from dlg.dropmake.lg import GraphException
from dlg.dropmake.scheduler import DAGUtil, SchedulerException

MAX_PGT_FN_CNT = 300


class PGUtil(object):
    """
    Helper functions dealing with Physical Graphs
    """

    @staticmethod
    def vstack_mat(A, B, separator=False):
        """
        Vertically stack two matrices that may have different # of colums

        :param: A matrix A (2d numpy array)
        :param: B matrix B (2d numy array)
        :param: separator whether to add an empty row separator between the two matrices (boolean)
        :return: the vertically stacked matrix (2d numpy array)
        """
        sa = A.shape
        sb = B.shape
        if sa[1] == sb[1]:
            if separator:
                return np.vstack((A, np.zeros((1, sb[1])), B))
            else:
                return np.vstack((A, B))
        s = A if sa[1] < sb[1] else B
        l = A if sa[1] > sb[1] else B
        gap = l.shape[1] - s.shape[1]
        if separator:
            pad = np.zeros((s.shape[0] + 1, l.shape[1]))
            pad[:-1, : (-1 * gap)] = s
        else:
            pad = np.zeros((s.shape[0], l.shape[1]))
            pad[:, : (-1 * gap)] = s
        return np.vstack((pad, l))


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
        # self._gen_pgt_sem.acquire()
        self._pgt_fn_count += 1
        if self._pgt_fn_count == MAX_PGT_FN_CNT + 1:
            self._pgt_fn_count = 0
        pgt_id = lg_name.replace(".graph", "{0}_pgt.graph".format(self._pgt_fn_count))
        pgt_path = "{0}/{1}".format(self._root_dir, pgt_id)

        try:
            # if the pgt name has a group with / then let's create a subdirectory
            # for it
            if "/" in lg_name:
                lg_dir = os.path.dirname(lg_name)
                try:
                    os.makedirs(os.path.join(self._root_dir, lg_dir))
                except OSError:
                    pass
            # overwrite file on disks
            with open(pgt_path, "w") as f:
                # json.dump(pgt.to_gojs_json(string_rep=False, visual=True), f)
                json_data = pgt.gojs_json_obj.copy()
                json_data["reprodata"] = pgt.reprodata
                json.dump(json_data, f)
            self._pgt_dict[pgt_id] = pgt
        except Exception as exp:
            raise GraphException("Fail to save PGT {0}:{1}".format(pgt_path, str(exp))) from exp
        finally:
            pass
            # self._gen_pgt_sem.release()
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
        if pgt is None:
            raise GraphException("PGT {0} not found".format(pgt_id))
        try:
            gcm = DAGUtil.ganttchart_matrix(pgt.dag)
        except SchedulerException:
            DAGUtil.label_schedule(pgt.dag)
            gcm = DAGUtil.ganttchart_matrix(pgt.dag)

        if json_str:
            gcm = json.dumps(gcm.tolist())
        return gcm

    def get_schedule_matrices(self, pgt_id, json_str=True):
        """
        Return:
            a list of schedule matrices (numarrays) given a PGT id
        """
        pgt = self.get_pgt(pgt_id)
        if pgt is None:
            raise GraphException("PGT {0} not found".format(pgt_id))
        jsobj = None
        try:
            parts = pgt.partitions
        except AttributeError as exc:
            raise GraphException(
                "Graph '{0}' has not yet been partitioned, so cannot produce scheduling matrix.".format(
                    pgt_id
                )
            ) from exc
        for part in parts:
            sm = part.schedule.schedule_matrix
            if jsobj is None:
                jsobj = sm
            else:
                jsobj = PGUtil.vstack_mat(jsobj, sm, separator=True)
        if json_str:
            jsobj = json.dumps(jsobj.tolist())
        return jsobj
