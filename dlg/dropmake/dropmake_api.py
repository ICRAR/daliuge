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

"""
Dropmake API examples based on
    test_pg_gen
    test_scheduler
    lg_web
"""
import os
import sys
import time

from optparse import OptionParser

from .pg_generator import LG, MySarkarPGTP


def gen_mysarkar_pgtp(lgfname, pgt_dir, num_islands=2,
                      cores_per_node=2, print_result=False,
                      dump_progress=False):
    """
    Generate Physical Graph Template (Partition) using
    MySarkar - A "somewhat greedy" scheudling algoritm

    No real resource mapping is involved
    """
    par_label = 'p'
    stt = time.time()
    lg = LG(lgfname)
    drop_list = lg.unroll_to_tpl()
    unroll_ett = time.time()
    mpp = num_islands > 0
    pgt = MySarkarPGTP(drop_list, 1, par_label, cores_per_node,
                       merge_parts=mpp)
    if (dump_progress):
        pgt._scheduler._dump_progress = True
    if (mpp):
        pgt.to_gojs_json(string_rep=False, visual=False)
        pgt.merge_partitions(num_islands, form_island=True,
                            island_type=1, visual=False)
    schedule_ett = time.time()
    re_dict = pgt.result()
    re_dict['unroll_time'] = '%.3f' % (unroll_ett - stt)
    re_dict['schedule_time'] = '%.3f' % (schedule_ett - unroll_ett)
    if (print_result):
        part_info = ' - '.join(['{0}:{1}'.format(k, v) for k, v in re_dict.items()])
        print(part_info)
    return re_dict

if __name__ == '__main__':
    """
    example usage:

    $ python -m dlg.dropmake.dropmake_api -l /Users/Chen/proj/daliuge-logical-graphs/SDP\ Pipelines/dist_sagecal.json -p /tmp -r -i 1
    exec_time:146.5 - unroll_time:0.006 - total_data_movement:30.0 - schedule_time:3.569 - algo:Edge Zero - num_parts:2 - min_exec_time:135 - num_islands:1

    $ python -m dlg.dropmake.dropmake_api -l /Users/Chen/proj/daliuge-logical-graphs/SDP\ Pipelines/dist_sagecal.json -p /tmp -r -c 1
    exec_time:220.0 - unroll_time:0.008 - total_data_movement:116.0 - schedule_time:2.073 - algo:Edge Zero - num_parts:3 - min_exec_time:135 - num_islands:2
    """
    parser = OptionParser()
    parser.add_option("-l", "--lgfname", action="store", type="string",
                        dest="lgfname", help="logical graph full name")
    parser.add_option("-p", "--pgtdir", action="store", type="string",
                        dest="pgt_dir", help="Directory to dump pgt")
    parser.add_option("-i", "--islands", action="store", type="int",
                        dest="num_islands", help="number of compute islands",
                        default=2)
    parser.add_option("-c", "--cores", action="store", type="int",
                        dest="cores_per_node", default=2,
                        help="number of cores per compute node")
    parser.add_option("-r", "--print", action="store_true", dest="print_result",
                        default=False, help="Print result on screen")
    parser.add_option("-d", "--dump_progress", action="store_true", dest="dump_progress",
                        default=False, help="Dump progress during scheduling")

    (options, args) = parser.parse_args()
    if (None == options.lgfname or None == options.pgt_dir):
        parser.print_help()
        sys.exit(1)

    if (not os.path.exists(options.lgfname)):
        print("invalid logical graph name %s" % options.lgfname)
        sys.exit(1)

    if (not os.path.exists(options.pgt_dir)):
        print("invalid physical graph directory %s" % options.pgt_dir)
        sys.exit(1)

    gen_mysarkar_pgtp(options.lgfname, options.pgt_dir, options.num_islands,
                      options.cores_per_node, options.print_result,
                      options.dump_progress)
