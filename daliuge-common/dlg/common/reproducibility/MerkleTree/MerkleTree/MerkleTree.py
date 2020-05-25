# Copyright (c) 2020 N.J. Pritchard
from Node import Node


class MerkleTree:
    """
    Basic implementation of a MerkleTree
    """

    def __init__(self):
        self.left = None
        self.right = None
        self.parent = None
        self.data = Node()

    def print(self):
        """
        Prints a representation of the MerkleTree
        :return:
        """
        self.data.print()
        if type(self.left) == MerkleTree:
            self.left.print()
        if type(self.right) == MerkleTree:
            self.right.print()

    def _add_data(self, data: list):
        """
        Internal recursive data adding routine
        Splits supplied data into halves at each stages building the MerkleTree with recursive hashing
        :param data: List of JSON-friendly elements
        :return hash: The resulting hash of the current list
        """
        print(data)
        if type(data) != list:
            raise TypeError("Data must be supplied in list form")
        self.data.add_data(data)
        if len(data) > 1:
            bound = len(data) // 2
            if len(data) % 2 != 0:
                bound += 1
            if self.left is None:
                self.left = MerkleTree()
                self.left.parent = self
            if self.right is None:
                self.right = MerkleTree()
                self.right.parent = self
            self.data.add_data(self.left._add_data(data[:bound]), "left")
            self.data.add_data(self.right._add_data(data[bound:]), "right")
        self.data.generate_hash()
        return self.data.hash

    def add_data(self, data):  # Assumes data is list
        """
        Attempts to add the supplied data to the tree by splitting on a per-item basis.
        Expects data to be serialized by JSON and in a list
        :param data: List of JSON-friendly elements
        :return hash: A string representation of the resulting root hash
        """
        print(data)
        return self._add_data(data)


def compare_mktree(x: MerkleTree, y: MerkleTree):
    """
    Compares two MerkleTrees using the minimal number of comparisons
    :param x: The first MerkleTree
    :param y: The second MerkleTree
    :return: True if the trees are identical, false otherwise
    """
    if x is None and y is None:
        return True
    if type(x) != MerkleTree or type(y) != MerkleTree:
        raise TypeError("Must compare MerkleTrees")
    if x is not None and y is not None:
        return ((x.data.hash == y.data.hash) and
                compare_mktree(x.left, y.left) and
                compare_mktree(x.right, y.right))
    return False
