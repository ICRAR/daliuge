# This file was ported and adapted from:
# https://github.com/mrocklin/heft/blob/master/heft/core.py

# The original copyright statement is as below:
# Copyright (c) 2013 Matthew Rocklin
# All rights reserved.
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#   a. Redistributions of source code must retain the above copyright notice,
#      this list of conditions and the following disclaimer.
#   b. Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#   c. Neither the name of HEFT nor the names of its contributors
#      may be used to endorse or promote products derived from this software
#      without specific prior written permission.

"""
Heterogeneous Earliest Finish Time -- A static scheduling heuristic

      Performance-effective and low-complexity task scheduling
                    for heterogeneous computing
                                by
             Topcuoglu, Haluk; Hariri, Salim Wu, M
     IEEE Transactions on Parallel and Distributed Systems 2002

Cast of Characters:

job - the job to be allocated
orders - dict {agent: [jobs-run-on-agent-in-order]}
jobson - dict {job: agent-on-which-job-is-run}
prec - dict {job: (jobs which directly precede job)}
succ - dict {job: (jobs which directly succeed job)}
compcost - function :: job, agent -> time to compute job on agent
commcost - function :: job, job, agent, agent -> time to transfer results
                       of one job needed by another between two agents

[1]. http://en.wikipedia.org/wiki/Heterogeneous_Earliest_Finish_Time

Significant changes for DALiuGE involve the support for tasks that require
multiple cores. The original HEFT algorithm assumes each task
consumes exactly one processor at a time
"""

from functools import partial
from collections import namedtuple
from util import reverse_dict
from itertools import chain

Event = namedtuple('Event', 'job start end')

def wbar(ni, agents, compcost):
    """ Average computation cost """
    return sum(compcost(ni, agent) for agent in agents) / len(agents)

def cbar(ni, nj, agents, commcost):
    """ Average communication cost """
    n = len(agents)
    if n == 1:
        return 0
    npairs = n * (n-1)
    return 1. * sum(commcost(ni, nj, a1, a2) for a1 in agents for a2 in agents
                                        if a1 != a2) / npairs

def ranku(ni, agents, succ,  compcost, commcost):
    """ Rank of job

    This code is designed to mirror the wikipedia entry.
    Please see that for details

    [1]. http://en.wikipedia.org/wiki/Heterogeneous_Earliest_Finish_Time
    """
    rank = partial(ranku, compcost=compcost, commcost=commcost,
                           succ=succ, agents=agents)
    w = partial(wbar, compcost=compcost, agents=agents)
    c = partial(cbar, agents=agents, commcost=commcost)

    if ni in succ and succ[ni]:
        return w(ni) + max(c(ni, nj) + rank(nj) for nj in succ[ni])
    else:
        return w(ni)

def endtime(job, events):
    """ Endtime of job in list of events """
    for e in events:
        if e.job == job:
            return e.end

def find_first_gap(agent_orders, desired_start_time, duration):
    """Find the first gap in an agent's list of jobs

    The gap must be after `desired_start_time` and of length at least
    `duration`.
    """

    # No jobs: can fit it in whenever the job is ready to run
    if (agent_orders is None) or (len(agent_orders)) == 0:
        return desired_start_time;

    # Try to fit it in between each pair of Events, but first prepend a
    # dummy Event which ends at time 0 to check for gaps before any real
    # Event starts.
    a = chain([Event(None,None,0)], agent_orders[:-1])
    for e1, e2 in zip(a, agent_orders):
        earliest_start = max(desired_start_time, e1.end)
        if e2.start - earliest_start > duration:
            return earliest_start

    # No gaps found: put it at the end, or whenever the task is ready
    return max(agent_orders[-1].end, desired_start_time)

def start_time(job, orders, jobson, prec, commcost, compcost, agent):
    """ Earliest time that job can be executed on agent """

    duration = compcost(job, agent)

    if job in prec:
        comm_ready = max([endtime(p, orders[jobson[p]])
                       + commcost(p, job, jobson[p], agent) for p in prec[job]])
    else:
        comm_ready = 0

    return find_first_gap(orders[agent], comm_ready, duration)

def allocate(job, orders, jobson, prec, compcost, commcost):
    """ Allocate job to the machine with earliest finish time

    Operates in place
    """
    st = partial(start_time, job, orders, jobson, prec, commcost, compcost)
    ft = lambda machine: st(machine) + compcost(job, machine)

    # 'min()' represents 'earliest' finished time (ft)
    # this is exactly why the allocation policy is considered greedy!
    agent = min(orders.keys(), key=ft)
    start = st(agent)
    end = ft(agent)
    #assert(end == start + compcost(job, agent))

    orders[agent].append(Event(job, start, end))
    #orders[agent] = sorted(orders[agent], key=lambda e: e.start)
    orders[agent].sort(key=lambda e: e.start)
    # Might be better to use a different data structure to keep each
    # agent's orders sorted at a lower cost.

    jobson[job] = agent

def makespan(orders):
    """ Finish time of last job """
    return max(v[-1].end for v in orders.values() if v)

def schedule(succ, agents, compcost, commcost):
    """ Schedule computation dag onto worker agents

    inputs:

    succ - DAG of tasks {a: (b, c)} where b, and c follow a
    agents - set of agents that can perform work
    compcost - function :: job, agent -> runtime
    commcost - function :: j1, j2, a1, a2 -> communication time
    """
    rank = partial(ranku, agents=agents, succ=succ,
                          compcost=compcost, commcost=commcost)
    prec = reverse_dict(succ)

    jobs = set(succ.keys()) | set(x for xx in succ.values() for x in xx)
    jobs = sorted(jobs, key=rank)

    orders = {agent: [] for agent in agents}
    jobson = dict()
    for job in reversed(jobs):
        allocate(job, orders, jobson, prec, compcost, commcost)

    return orders, jobson
