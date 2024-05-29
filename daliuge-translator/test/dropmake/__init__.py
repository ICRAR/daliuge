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

SARKAR_PARTITION_RESULTS = {
    "testLoop.graph": {
        'algo': 'Edge Zero', 'min_exec_time': 30, 'total_data_movement': 50,
        'exec_time': 80, 'num_parts': 0
    },
    "cont_img_mvp.graph": {
        'algo': 'Edge Zero', 'min_exec_time': 144, 'total_data_movement': 932,
        'exec_time': 444, 'num_parts': 0
    },
    "test_grpby_gather.graph": {
        'algo': 'Edge Zero', 'min_exec_time': 12, 'total_data_movement': 70,
        'exec_time': 47, 'num_parts': 0
    },
    "chiles_simple.graph": {
        'algo': 'Edge Zero', 'min_exec_time': 45, 'total_data_movement': 1080,
        'exec_time': 285, 'num_parts': 0
    }
}

SARKAR_PARTITION_RESULTS_GEN_ISLAND = {
    "testLoop.graph": {
        'algo': 'Edge Zero',
        'min_exec_time': 30,
        'total_data_movement': 0,
        'exec_time': 30, 'num_parts': 1
    },
    "cont_img_mvp.graph": {
        'algo': 'Edge Zero', 'min_exec_time': 144, 'total_data_movement': 135,
        'exec_time': 179, 'num_islands': 2, 'num_parts': 4
    },
    "test_grpby_gather.graph": {
        'algo': 'Edge Zero', 'min_exec_time': 12,
        'total_data_movement': 0, 'exec_time': 12, 'num_parts': 1
    },
    "chiles_simple.graph": {
        'algo': 'Edge Zero', 'min_exec_time': 45, 'total_data_movement': 0,
        'exec_time': 45, 'num_parts': 1
    }
}

MINPARTS_RESULTS = {
    "testLoop.graph": {
        'algo': 'Lookahead', 'min_exec_time': 30, 'total_data_movement': 50,
        'exec_time': 80, 'num_parts': 0
    },
    "cont_img_mvp.graph": {
        'algo': 'Lookahead', 'min_exec_time': 144,
        'total_data_movement': 932, 'exec_time': 444, 'num_parts': 0
    },
    "test_grpby_gather.graph": {
        'algo': 'Lookahead', 'min_exec_time': 12,
        'total_data_movement': 70, 'exec_time': 47, 'num_parts': 0
    },
    "chiles_simple.graph": {
        'algo': 'Lookahead', 'min_exec_time': 45, 'total_data_movement': 1080,
        'exec_time': 285, 'num_parts': 0
    },
}
