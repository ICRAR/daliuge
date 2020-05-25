# Copyright (c) 2020 N.J. Pritchard
from MerkleTree import MerkleTree, compare_mktree


class TestMerkleTree:
    x = MerkleTree()
    y = MerkleTree()
    x.add_data(['A', 'B', 'C', 'D', 'E', 'F', 'G'])
    y.add_data(['A', 'B', 'C', 'C', 'E', 'F', 'G'])

    def test_add_data(self):
        assert not compare_mktree(self.x, self.y)
        self.y.add_data(['A', 'B', 'C', 'D', 'E', 'F', 'G'])
        assert compare_mktree(self.x, self.y)


class TestCompareMKTree:
    x = MerkleTree()
    y = MerkleTree()
    z = MerkleTree()
    w = MerkleTree()
    a = MerkleTree()
    b = MerkleTree()
    x.add_data([1, 2, 3, 4])
    y.add_data([1, 2, 3, 4])
    z.add_data([2, 3, 4, 5])
    w.add_data([1, 2, 3, 6])
    a.add_data(['1', '2', '3', '4'])
    b.add_data(['1', '2', '3', '4', '5'])

    def test_compare_mktree(self):
        assert compare_mktree(self.x, self.y)
        assert not compare_mktree(self.x, self.z)
        assert not compare_mktree(self.x, self.w)
        assert not compare_mktree(self.x, self.a)
        assert not compare_mktree(self.a, self.b)
        assert not compare_mktree(self.b, self.a)
