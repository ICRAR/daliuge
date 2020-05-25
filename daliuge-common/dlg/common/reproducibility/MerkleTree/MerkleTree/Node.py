# Copyright (c) 2020 N.J. Pritchard
import hashlib
import json


class Node(object):
    """
    Our own implemenetation of a data-node as a precursor to a block structure.
    Abstracts away hash generation
    """

    def __init__(self):
        self.data = {}
        self.data_serial = None
        self.hash = None
        self.changed = False

    @property
    def is_empty(self):
        return self.data == {}

    def add_data(self, value, key="data"):
        """
        Adds data values blindly
        :param value: The data
        :param key: The internal dictionary key which can be specified if non-default behaviour is needed
        """
        self.data[key] = value
        self.changed = True

    def get_data(self):
        return self.data

    def generate_hash(self):
        """
        Hashes the current data
        """
        if self.changed:
            self.data_serial = json.dumps(self.data, sort_keys=True)
            self.hash = hashlib.sha3_256(self.data_serial.encode(encoding="utf-8")).hexdigest()
            self.changed = False

    def print(self):
        for element in self.data:
            print(str(element) + " " + str(self.data.get(element)))
        print(self.hash)


def node_compare(x: Node, y: Node):
    """
    Compares two nodes by hash
    :param x: The first node
    :param y: The second node
    :return: True if matching, false otherwise
    """
    if type(x) != Node or type(y) != Node:
        raise TypeError("Need to compare Nodes")
    if x.hash != y.hash:
        return False
    else:
        return True
