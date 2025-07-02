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
import logging
import re
import subprocess
import time

logger = logging.getLogger(f"dlg.{__name__}")


class ListTokens(object):
    STRING, COMMA, RANGE_SEP, MULTICASE_START, MULTICASE_END = range(5)


def _list_tokenizer(s):
    buff = []
    in_square_brackets = False
    for char in s:
        if char == "-" and in_square_brackets:
            yield ListTokens.STRING, "".join(buff)
            buff = []
            yield ListTokens.RANGE_SEP, None
        elif char == ",":
            if buff:
                yield ListTokens.STRING, "".join(buff)
                buff = []
            yield ListTokens.COMMA, None
        elif char == "[":
            in_square_brackets = True
            if buff:
                yield ListTokens.STRING, "".join(buff)
                buff = []
            yield ListTokens.MULTICASE_START, None
        elif char == "]":
            in_square_brackets = False
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
            prefix = ""
            if values:
                prefix = values.pop()
            sub_values = _parse_list_tokens(token_iter)
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
    if not physical_graph_template_file:
        return None, None, physical_graph_template_file

    with open(physical_graph_template_file, "r") as f:
        pgt_data = json.load(f, strict=False)
    try:
        (pgt_name, pgt) = pgt_data
    except TypeError as e:
        raise ValueError(type(pgt_data)) from e
    try:
        nodes = list(map(lambda x: x["node"], pgt))
        islands = list(map(lambda x: x["island"], pgt))
    except KeyError:
        return None, None, pgt_name
    num_islands = len(dict(zip(islands, range(len(islands)))))
    num_nodes = len(dict(zip(nodes, range(len(nodes)))))
    return num_islands, num_nodes, pgt_name


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
        raise Exception("Not enough nodes {0} to run DALiuGE.".format(num_nodes))
    return ret


def find_node_ips():
    query = subprocess.check_output(
        [
            r"kubectl get nodes --selector=kubernetes.io/role!=master -o jsonpath={.items[*].status.addresses[?\(@.type==\"InternalIP\"\)].address}"
        ],
        shell=True,
    )
    node_ips = query.decode(encoding="utf-8").split(" ")
    return node_ips


def find_service_ips(num_expected, retries=3, timeout=10):
    pattern = r"^daliuge-daemon-service-.*\s*ClusterIP\s*\d+\.\d+\.\d+\.\d+"
    ip_pattern = r"\d+\.\d+\.\d+\.\d+"
    ips = []
    attempts = 0
    while len(ips) < num_expected and attempts < retries:
        ips = []
        query = subprocess.check_output(
            [r"kubectl get svc -o wide"], shell=True
        ).decode(encoding="utf-8")
        outcome = re.findall(pattern, query, re.M)
        for service in outcome:
            ip = re.search(ip_pattern, service)
            if ip:
                ips.append(ip.group(0))
        logger.info("K8s service ips: %s", ips)
        time.sleep(timeout)
    return ips


def find_pod_ips(num_expected, retries=3, timeout=10):
    ips = []
    attempts = 0
    while len(ips) < num_expected and attempts < retries:
        ips = []
        query = str(
            subprocess.check_output([r"kubectl get pods -o wide"], shell=True).decode(
                encoding="utf-8"
            )
        )
        pattern = r"^daliuge-daemon.*"
        ip_pattern = r"\d+\.\d+\.\d+\.\d+"
        outcome = re.findall(pattern, query, re.M)
        for pod in outcome:
            ip = re.search(ip_pattern, pod)
            if ip:
                ips.append(ip.group(0))
        logger.info("K8s pod ips: %s", ips)
        time.sleep(timeout)
    return ips


def _status_all_running(statuses):
    if statuses == []:
        return False
    for status in statuses:
        if status != "Running":
            return False
    return True


def wait_for_pods(num_expected, retries=18, timeout=10):
    all_running = False
    attempts = 0
    while not all_running and attempts < retries:
        query = str(
            subprocess.check_output([r"kubectl get pods -o wide"], shell=True).decode(
                encoding="utf-8"
            )
        )
        logger.info(query)
        pattern = r"^daliuge-daemon.*"
        outcome = re.findall(pattern, query, re.M)
        if len(outcome) < num_expected:
            all_running = False
            continue
        all_running = True
        for pod in outcome:
            if "Running" not in pod:
                all_running = False
        attempts += 1
        time.sleep(timeout)
    return all_running
