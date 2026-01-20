
import os
import unittest

from pathlib import Path

from dlg.data.drops.file import FileDROP
from dlg.exceptions import ErrorManagerCaughtException


class TestDROPFilepath(unittest.TestCase):
    """
    Test Filepath methods work as expected. This includes:
        - Sanitizing paths
        - Identifying directory or filepath
        - Expanding environment variables.
    """

    def test_basic_filepath(self):
        fdir = Path("/tmp/daliuge_tfiles") # see PathBaseDrop.get_dir()
        fdrop = FileDROP(uid="A", oid="A", filepath="test.txt")
        self.assertEqual(fdir/"test.txt", Path(fdrop.path))

    def test_basic_dir(self):
        fdir = Path("/tmp/daliuge_tfiles")  # see PathBaseDrop.get_dir()
        fdrop = FileDROP(uid="A", oid="A", filepath="mydir/")
        self.assertEqual(fdir / "mydir", Path(fdrop.dirname))

    def test_root_dir(self):
        fdrop = FileDROP(uid="A", oid="A", filepath="/tmp/")
        self.assertEqual(Path("/tmp"), Path(fdrop.dirname))

    def test_expand_envvar(self):
        os.environ["MYDIR"] = "/tmp/"
        fdrop = FileDROP(uid="A", oid="A", filepath="$MYDIR")
        self.assertEqual(Path("/tmp"), Path(fdrop.dirname))

        fdrop = FileDROP(uid="A", oid="A", filepath="$MYDIR/test.txt")
        self.assertEqual(Path("/tmp") / "test.txt", Path(fdrop.path))

        fdir = Path("/tmp/daliuge_tfiles")  # see PathBaseDrop.get_dir()
        os.environ["MYDIR"] = "mydir/"
        fdrop = FileDROP(uid="A", oid="A", filepath="$MYDIR")
        self.assertEqual(fdir/"mydir/", Path(fdrop.dirname))

        fdrop = FileDROP(uid="A", oid="A", filepath="$MYDIR/test.txt")
        self.assertEqual(fdir/"mydir/test.txt", Path(fdrop.path))

    def test_expand_missingenvar(self):
        self.assertRaises(ErrorManagerCaughtException, FileDROP, uid="A", oid="A",
                                                        filepath="$MISSING/test.txt")