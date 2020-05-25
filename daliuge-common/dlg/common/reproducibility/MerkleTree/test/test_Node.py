# Copyright (c) 2020 N.J. Pritchard
from Node import Node, node_compare


class TestNode:
    x = Node()

    def test_add_data(self):
        test_node = Node()
        test_node.add_data(0)
        assert test_node.get_data()['data'] == 0
        test_node.add_data(1)
        assert test_node.get_data()['data'] == 1
        test_node.add_data([0, 1, 2])
        assert test_node.get_data()['data'] == [0, 1, 2]
        test_node.add_data(0, "test")
        assert test_node.get_data()["test"] == 0
        test_node.add_data("ABCDEFG", "txList")
        assert test_node.get_data()["txList"] == "ABCDEFG"
        test_node.add_data(True, "boolTest")
        assert test_node.get_data()["boolTest"]
        test_node.add_data(None, "nullTest")
        assert test_node.get_data()["nullTest"] is None

    def test_generate_hash(self):
        assert True


class TestComparison:
    x = Node()
    y = Node()
    z = Node()
    x.add_data("help")
    y.add_data("help")
    z.add_data("helP")
    x.generate_hash()
    y.generate_hash()
    z.generate_hash()

    def test_node_compare(self):
        assert node_compare(self.x, self.y)
        assert node_compare(self.y, self.x)
        assert not node_compare(self.x, self.z)
        assert not node_compare(self.z, self.x)
