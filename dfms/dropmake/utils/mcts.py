# Ported by chen.wu@icrar.org from
# https://github.com/jbradberry/mcts
# with significant changes made for DAG scheduling (rather than the Go game!)
# These changes are subject to the following copyright:
# ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2016
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
# The original Copyright statement is as follows:
# Copyright (c) 2015 Jeff Bradberry
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

"""
Monte Carlo Tree Search
https://en.wikipedia.org/wiki/Monte_Carlo_tree_search

Here we treat each edge zeroing step as a "move" in the Go game
"""

import time
from random import choice
from math import log, sqrt
from collections import defaultdict

class DAGTree(object):
    """
    Each node represents a "state" (aka "position" in game plays)
    Each edge represents a "move"
    Each state is a list of integer: e.g. '123123111222'
    """

    def __init__(self, dag, scheduler):
        """
        Initialise states information
        dag:    a "light" unpartitioned DAG
        """
        self._dag = dag
        self._scheduler = scheduler
        self._leng = len(dag.edges())

    def next_state(self, state, move):
        """
        Given the current state and move, return the next (child) state
        """
        if (state is None):
            return move
        else:
            return '{0}{1}'.format(state, move)

    def legal_moves(self, state_history):
        """
        Return a list of allowed moves based on the current state

        Dummy implementation for now.
        But to support constraints in the near future, returns moves that
        comply with the constraints
        """
        return ['1', '2', '3']

    def payout(self, state_history):
        """
        Play until the end of the game
        Then calculate payout based on the objective function:
            the length of the critical path
        """
        G = self._dag.copy()
        leng = self._leng
        # convert '98760' to [9, 8, 7, 6, 0]
        x = [int(ii) for ii in list(state_history[-1][:])]
        # print x
        if (len(x) < leng): #padding
            x += [3] * (leng - len(x))

        stuff = self._scheduler._partition_G(G, x)
        lgl = stuff[0]
        num_parts = stuff[1]
        # TODO add num_parts as the panelty score
        return lgl * -1

    def parent_state(self, state):
        """
        Given the child state, returns the (only) parent state
        Return None if the child state is the Root state
        """
        if (len(state) == 1):
            return None
        else:
            return state[0:-1]

    def append_state(self, state_history, state):
        if (type(state) == list):
            state = ''.join([str(x) for x in state])
        state_history.append(state)

    def latest_state(self, state_history):
        if (len(state_history) > 0):
            return state_history[-1]
        else:
            return None

class MCTS(object):
    def __init__(self, dag_tree, calculation_time=30, max_moves=1000, factor=1.4):
        self._dag_tree = dag_tree
        self._calc_time = calculation_time
        self._max_moves = max_moves
        self._states = []
        #self._dag_tree.append_state(self._states, init_state)
        self.scores = defaultdict(int)#{} # key: state, value: score
        self.plays = defaultdict(int)#{} # key: state, value: count
        self.max_depth = 0
        # Exploration constant, increase for more exploratory moves,
        # decrease to prefer moves with known higher win rates.
        self.C = factor

    def update(self, state):
        self._dag_tree.append_state(self._states, state)

    def run(self):
        m, state = self.next_move()
        leng = self._dag_tree._leng
        while (len(state) < leng):
            m, state = self.next_move()
            #print "max_depth = {0}, state: {1}".format(self.max_depth, state)
            #print "max_depth = {0}".format(self.max_depth)
        return [int(ii) for ii in list(state)]

    def next_move(self):
        """
        Returns the next move that gives the best average/overall payout
        """
        games = 0
        scores = self.scores
        plays = self.plays
        self.max_depth = 0
        stt = time.time()
        while time.time() - stt < self._calc_time:
            self.simulate_moves()
            games += 1
        state = self._dag_tree.latest_state(self._states)
        legal = self._dag_tree.legal_moves(self._states[:])
        moves_states = [(p, self._dag_tree.next_state(state, p)) for p in legal]
        # Pick the move with highest overall score
        overall_payout = float("-infinity")
        for p, S in moves_states:
            nm = plays.get(S, 1)
            op = scores.get(S, 0) / nm
            if (op > overall_payout):
                overall_payout = op
                num_moves = nm
                move = p
                new_state = S
        #print overall_payout, num_moves, move, new_state
        self.update(new_state)
        return (move, new_state)

    def simulate_moves(self):
        """
        Simulate a "random" play if necessary from the current state
        then updates the statistics using backpropogation
        """

        # A bit of an optimization here, so we have a local
        # variable lookup instead of an attribute access each loop.
        plays, scores = self.plays, self.scores
        states_copy = self._states[:]
        state = self._dag_tree.latest_state(states_copy)
        CC = self.C

        for t in range(1, self._max_moves + 1):
            # 1. Selection
            legal = self._dag_tree.legal_moves(states_copy)
            moves_states = [(p, self._dag_tree.next_state(state, p)) for p in legal]
            if all(plays.get(S) for p, S in moves_states):
                # If we have stats on all of the legal moves here, use UCB1.
                log_total = log(sum(plays[S] for p, S in moves_states))
                value = float("-infinity")
                for p, S in moves_states:
                    v = (scores[S] / plays[S]) + CC * sqrt(log_total / plays[S])
                    if (v > value):
                        value = v
                        move = p
                        state = S
                states_copy.append(state)
            else:
                # Otherwise, just make an arbitrary decision. and expand it
                move, state = choice(moves_states)
                states_copy.append(state)
                # 2. Expansion
                # Only one node is added per simulated game.
                # if state not in plays:
                #     plays[state] = 0
                #     scores[state] = 0
                if t > self.max_depth:
                    self.max_depth = t
                break

        # 3. Simulation
        payout = self._dag_tree.payout(states_copy)

        # 4. Back propogation
        plays[state] += 1
        scores[state] += payout
        ps = self._dag_tree.parent_state(state)
        while (ps is not None):
            #print plays
            plays[ps] += 1
            scores[ps] += payout
            ps = self._dag_tree.parent_state(ps)
