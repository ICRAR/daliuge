
import os
import unittest

from pathlib import Path

from dlg.data.drops.file import FileDROP


class TestDROPFilepath(unittest.TestCase):
    """
    Test Filepath methods work as expected. This includes:
        - Sanitizing paths
        - Identifying directory or filepath
        - Expanding environment variables.
    """

    def test_basic_filepath(self):
        dir = Path("/tmp/daliuge_tfiles") # see PathBaseDrop.get_dir()
        fdrop = FileDROP(uid="A", oid="A", filepath="test.txt")
        self.assertEqual(dir/"test.txt", Path(fdrop.path))

    def test_basic_dir(self):
        dir = Path("/tmp/daliuge_tfiles")  # see PathBaseDrop.get_dir()
        fdrop = FileDROP(uid="A", oid="A", filepath="mydir/")
        self.assertEqual(dir / "mydir", Path(fdrop.dirname))

    def test_root_dir(self):
        # dir = Path("/tmp/daliuge_tfiles")  # see PathBaseDrop.get_dir()
        fdrop = FileDROP(uid="A", oid="A", filepath="/mydir/")
        self.assertEqual(Path("/mydir"), Path(fdrop.dirname))

    def test_expand_envvar(self):
        os.environ["MYDIR"] = "/mydir/"
        fdrop = FileDROP(uid="A", oid="A", filepath="$MYDIR")
        self.assertEqual(Path("/mydir"), Path(fdrop.dirname))

        fdrop = FileDROP(uid="A", oid="A", filepath="$MYDIR/test.txt")
        self.assertEqual(Path("/mydir") / "test.txt", Path(fdrop.path))

        dir = Path("/tmp/daliuge_tfiles")  # see PathBaseDrop.get_dir()
        os.environ["MYDIR"] = "mydir/"
        fdrop = FileDROP(uid="A", oid="A", filepath="$MYDIR")
        self.assertEqual(dir/"mydir/", Path(fdrop.dirname))

        fdrop = FileDROP(uid="A", oid="A", filepath="$MYDIR/test.txt")
        self.assertEqual(dir/"mydir/test.txt", Path(fdrop.path))

