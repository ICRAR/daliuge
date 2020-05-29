from dlg_heft import schedule, \
    makespan, RES_TYPES

"""
This is a simple script to use the HEFT function provided based on the example given in the original HEFT paper.
You have to define the DAG, compcost function and commcost funtion, and setup each machine's capacity and each task's workload

Each task is numbered 1 to 10
Each machine/agent is named 'a', 'b' and 'c'

Output expected:

a [Event(task=6, start=23, end=36), Event(task=8, start=53, end=58)]
b [Event(task=4, start=18, end=26), Event(task=9, start=43, end=55), Event(task=10, start=69, end=76)]
c [Event(task=1, start=0, end=9), Event(task=3, start=9, end=28), Event(task=2, start=9, end=27), Event(task=5, start=9, end=19), Event(task=7, start=28, end=39)]
{1: 'c', 4: 'b', 3: 'c', 2: 'c', 5: 'c', 6: 'a', 9: 'b', 7: 'c', 8: 'a', 10: 'b'}
"""

dag = {1: (2, 3, 4, 5, 6),
       2: (8, 9),
       3: (7,),
       4: (8, 9),
       5: (9,),
       6: (8,),
       7: (10,),
       8: (10,),
       9: (10,),
       10: ()}


def setup_capacity(agents):
    ac = dict()
    for i, a in enumerate(agents):
        supply = {}
        for j, res in enumerate(RES_TYPES):
            supply[res] = (8 + j) * (i + 1)
        # print(supply)
        ac[a] = supply
    # print('all capacity', ac)
    return ac


def setup_workload(tasks):
    wl = dict()
    for i, t in enumerate(tasks):
        demand = {}
        for j, res in enumerate(RES_TYPES):
            demand[res] = (1 + j) * (i + 1)
        # print(demand)
        wl[t] = demand
    return wl


def compcost(task, agent):
    if (task == 1):
        if (agent == 'a'):
            return 14
        elif (agent == 'b'):
            return 16
        else:
            return 9

    if (task == 2):
        if (agent == 'a'):
            return 13
        elif (agent == 'b'):
            return 19
        else:
            return 18
    if (task == 3):
        if (agent == 'a'):
            return 11
        elif (agent == 'b'):
            return 13
        else:
            return 19
    if (task == 4):
        if (agent == 'a'):
            return 13
        elif (agent == 'b'):
            return 8
        else:
            return 17
    if (task == 5):
        if (agent == 'a'):
            return 12
        elif (agent == 'b'):
            return 13
        else:
            return 10
    if (task == 6):
        if (agent == 'a'):
            return 13
        elif (agent == 'b'):
            return 16
        else:
            return 9
    if (task == 7):
        if (agent == 'a'):
            return 7
        elif (agent == 'b'):
            return 15
        else:
            return 11
    if (task == 8):
        if (agent == 'a'):
            return 5
        elif (agent == 'b'):
            return 11
        else:
            return 14
    if (task == 9):
        if (agent == 'a'):
            return 18
        elif (agent == 'b'):
            return 12
        else:
            return 20
    if (task == 10):
        if (agent == 'a'):
            return 21
        elif (agent == 'b'):
            return 7
        else:
            return 16


def commcost(ni, nj, A, B):
    if (A == B):
        return 0
    else:
        if (ni == 1 and nj == 2):
            return 18
        if (ni == 1 and nj == 3):
            return 12
        if (ni == 1 and nj == 4):
            return 9
        if (ni == 1 and nj == 5):
            return 11
        if (ni == 1 and nj == 6):
            return 14
        if (ni == 2 and nj == 8):
            return 19
        if (ni == 2 and nj == 9):
            return 16
        if (ni == 3 and nj == 7):
            return 23
        if (ni == 4 and nj == 8):
            return 27
        if (ni == 4 and nj == 9):
            return 23
        if (ni == 5 and nj == 9):
            return 13
        if (ni == 6 and nj == 8):
            return 15
        if (ni == 7 and nj == 10):
            return 17
        if (ni == 8 and nj == 10):
            return 11
        if (ni == 9 and nj == 10):
            return 13
        else:
            return 0


agents = 'abc'
cpct = setup_capacity(agents)
wkld = setup_workload(dag.keys())
orders, taskson = schedule(dag, agents, compcost, commcost, cpct, wkld)
for eachP in sorted(orders):
    print(eachP, orders[eachP])
print(taskson)
print(makespan(orders))
