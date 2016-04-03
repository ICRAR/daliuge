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
"""

class DAGTree(object):

    def __init__(self, **kwargs):
        """
        Initialise states information
        """
        self._true_dag = None

    def next_state(self, state, move):
        """
        Given the current state and move, return the new (child) state
        """
        pass

    def legal_moves(self, state_history):
        """
        Return a list of allowed moves based on the current state
        """
        pass

    def payout(self, state_history):
        """
        Play until the end of the game
        Then calculate payout based on the objective function
        """
        pass

    def parent_state(self, state):
        """
        Given the child state, returns the (only) parent state
        """
        pass


class MCTS(object):
    def __init__(self, dag_tree, calculation_time=30, max_moves=1):
        self._dag_tree = dag_tree
        self._calc_time = calculation_time
        self._max_moves = max_moves
        self._states = []
        self.scores = {} # key: state, value: count
        self.plays = {} # key: state, value: count

    def update(self, state):
        self._states.append(state)

    def next_move(self):
        """
        Returns the next move that gives the best average/overall payout
        """
        games = 0
        stt = time.time()
        while time.time() - stt < self._calc_time:
            self.simulate_moves()
            games += 1
        state = self._states[-1]
        legal = self._dag_tree.legal_moves(self._states[:])
        moves_states = [(p, self._dag_tree.next_state(state, p)) for p in legal]
        # Pick the move with highest overall score
        overall_payout, num_moves, move, new_state = max(
            (self.scores.get(S, 0) / self.plays.get(S, 1),
             self.plays.get(S, 0), p, S)
            for p, S in moves_states
        )
        self.update(new_state)

    def simulate_moves(self):
        # Plays out a "random" game from the current position/state,
        # then updates the statistics tables with the result.

        # A bit of an optimization here, so we have a local
        # variable lookup instead of an attribute access each loop.
        plays, scores = self.plays, self.scores

        states_copy = self.states[:]
        state = states_copy[-1]

        legal = self._dag_tree.legal_plays(states_copy)
        moves_states = [(p, self._dag_tree.next_state(state, p)) for p in legal]

        # 1. Selection
        if all(plays.get(S) for p, S in moves_states):
            # If we have stats on all of the legal moves here, use UCB1.
            log_total = log(
                sum(plays[S] for p, S in moves_states))
            value, move, state = max(
                ((scores[S] / plays[S)]) +
                 self.C * sqrt(log_total / plays[S)]), p, S)
                for p, S in moves_states
            )
        else:
            # Otherwise, just make an arbitrary decision.
            move, state = choice(moves_states)

        states_copy.append(state)

        # 2. Expansion
        # Only one node is added per simulated game.
        if state not in self.plays:
            self.plays[state] = 0
            self.scores[state] = 0

        # 3. Simulation
        payout = self._dag_tree.payout(states_copy)

        # 4. Back propogation (this is not recursive yet!!)
        self.plays[state] += 1
        self.scores[state] += payout
