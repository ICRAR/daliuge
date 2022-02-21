#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2019
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
import json


class ListTokens(object):
    STRING, COMMA, RANGE_SEP, MULTICASE_START, MULTICASE_END = range(5)


def _list_tokenizer(s):
    buff = []
    for char in s:
        if char == "-":
            yield ListTokens.STRING, "".join(buff)
            buff = []
            yield ListTokens.RANGE_SEP, None
        elif char == ",":
            if buff:
                yield ListTokens.STRING, "".join(buff)
                buff = []
            yield ListTokens.COMMA, None
        elif char == "[":
            if buff:
                yield ListTokens.STRING, "".join(buff)
                buff = []
            yield ListTokens.MULTICASE_START, None
        elif char == "]":
            if buff:
                yield ListTokens.STRING, "".join(buff)
                buff = []
            yield ListTokens.MULTICASE_END, None
        else:
            buff.append(char)
    if buff:
        yield ListTokens.STRING, "".join(buff)
        buff = []


def _parse_list_tokens(token_iter):
    def finish_element(sub_values, range_start):
        if sub_values:
            values.extend(sub_values)
        elif range_start is not None:
            range_end = values.pop()
            str_len = max(len(range_start), len(range_end))
            str_format = "%%0%dd" % str_len
            num_vals = [
                str_format % num for num in range(int(range_start), int(range_end) + 1)
            ]
            values.extend(num_vals)

    values = []
    sub_values = []
    range_start = None
    while True:
        try:
            token, value = next(token_iter)
        except StopIteration:
            finish_element(sub_values, range_start)
            return values
        if token == ListTokens.MULTICASE_END:
            finish_element(sub_values, range_start)
            return values
        if token == ListTokens.MULTICASE_START:
            if values:
                prefix = values.pop()
            sub_values = _parse_list_tokens(token_iter)
            if prefix:
                sub_values = [prefix + s for s in sub_values]
        if token == ListTokens.RANGE_SEP:
            range_start = values.pop()
        elif token == ListTokens.COMMA:
            finish_element(sub_values, range_start)
            sub_values = None
            range_start = None
        elif token == ListTokens.STRING:
            if sub_values:
                sub_values = [s + value for s in sub_values]
            else:
                values.append(value)


def list_as_string(s):
    """'a008,b[072-073,076]' --> ['a008', 'b072', 'b073', 'b076']"""
    return _parse_list_tokens(iter(_list_tokenizer(s)))


def find_numislands(physical_graph_template_file):
    """
    Given the physical graph data extract the graph name and the total number of
    nodes. We are not making a decision whether the island managers are running
    on separate nodes here, thus the number is the sum of all island
    managers and node managers. The values are only populated if not given on the
    init already.
    TODO: We will probably need to do the same with job duration and CPU number
    """

    pgt_data = json.loads(physical_graph_template_file)
    try:
        (pgt_name, pgt) = pgt_data
    except:
        raise ValueError(type(pgt_data))
    nodes = list(map(lambda x: x['node'], pgt))
    islands = list(map(lambda x: x['island'], pgt))
    num_islands = len(dict(zip(islands, nodes)))
    num_nodes = list(map(lambda x, y: x + y, islands, nodes))
    pip_name = pgt_name
    return num_islands, num_nodes, pip_name


def label_job_dur(job_dur):
    """
    e.g. 135 min --> 02:15:00
    """
    seconds = job_dur * 60
    minute, sec = divmod(seconds, 60)
    hour, minute = divmod(minute, 60)
    return "%02d:%02d:%02d" % (hour, minute, sec)


def num_daliuge_nodes(num_nodes: int, run_proxy: bool):
    """
    Returns the number of daliuge nodes available to run workflow
    """
    if run_proxy:
        ret = num_nodes - 1  # exclude the proxy node
    else:
        ret = num_nodes - 0  # exclude the data island node?
    if ret <= 0:
        raise Exception(
            "Not enough nodes {0} to run DALiuGE.".format(num_nodes)
        )
    return ret
