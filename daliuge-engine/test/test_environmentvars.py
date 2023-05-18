#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2014
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#
import os
import tempfile
import unittest

from dlg.drop import AbstractDROP
from dlg.data.drops.file import FileDROP
from dlg.data.drops.environmentvar_drop import EnvironmentVarDROP
from dlg.utils import getDlgDir


def create_std_env_vars(name="env_vars"):
    return EnvironmentVarDROP(
        oid="a",
        uid="a",
        name=name,
        dir_var="/HOME/",
        int_var=3,
        bool_var=False,
        float_var=0.5,
        dict_var={"first": 1, "second": "sec"},
        list_var=[1, 2.0, "3"],
    )


def create_empty_env_vars(name="env_vars"):
    return EnvironmentVarDROP(oid="b", uid="b", name=name)


class TestEnvironmentVarDROP(unittest.TestCase):
    def test_get(self):
        """
        Tests that environment variables are read in and fetched correctly.
        """
        env_drop = create_std_env_vars()
        self.assertEqual("/HOME/", env_drop.get("dir_var"))
        self.assertEqual(3, env_drop.get("int_var"))
        self.assertEqual(False, env_drop.get("bool_var"))
        self.assertEqual(0.5, env_drop.get("float_var"))
        self.assertEqual(
            {"first": 1, "second": "sec"}, env_drop.get("dict_var")
        )
        self.assertEqual([1, 2.0, "3"], env_drop.get("list_var"))
        self.assertIsNone(env_drop.get("non_var"))
        self.assertIsNone(env_drop.get("uid"))

    def test_get_empty(self):
        """
        Tests that an empty environment drop contains no environment variables.
        """
        env_drop = create_empty_env_vars()
        self.assertEqual(dict(), env_drop._variables)

    def test_get_multiple(self):
        """
        Tests the get_multiple routine for environment variables is correct
        """
        env_drop = create_std_env_vars()
        expected_vars = [
            None,
            "/HOME/",
            3,
            False,
            0.5,
            {"first": 1, "second": "sec"},
            [1, 2.0, "3"],
            None,
        ]
        query_keys = [
            "uid",
            "dir_var",
            "int_var",
            "bool_var",
            "float_var",
            "dict_var",
            "list_var",
            "non_var",
        ]
        self.assertEqual(expected_vars, env_drop.get_multiple(query_keys))

    def test_set(self):
        """
        Should currently raise un-implemented, but here for completeness
        """
        env_drop = create_std_env_vars()
        self.assertRaises(NotImplementedError, env_drop.set, "var", "val")

    def test_drop_get_single(self):
        """
        Tests the AbstractDROP fetch routine functions correctly with a single environment drop
        """
        env_drop = create_std_env_vars()
        test_drop = AbstractDROP(uid="b", oid="b")
        test_drop.addProducer(env_drop)
        self.assertEqual(
            "/HOME/", test_drop.get_environment_variable("$env_vars.dir_var")
        )
        self.assertEqual(
            3, test_drop.get_environment_variable("$env_vars.int_var")
        )
        self.assertEqual(
            False, test_drop.get_environment_variable("$env_vars.bool_var")
        )
        self.assertEqual(
            0.5, test_drop.get_environment_variable("$env_vars.float_var")
        )
        self.assertEqual(
            {"first": 1, "second": "sec"},
            test_drop.get_environment_variable("$env_vars.dict_var"),
        )
        self.assertEqual(
            [1, 2.0, "3"],
            test_drop.get_environment_variable("$env_vars.list_var"),
        )
        self.assertEqual(
            "$env_vars.non_var",
            test_drop.get_environment_variable("$env_vars.non_var"),
        )
        self.assertEqual(
            "$env_vars.uid",
            test_drop.get_environment_variable("$env_vars.uid"),
        )

    def test_drop_get_multiple(self):
        """
        Tests the AbstractDROP multiple fetch routine functions correctly with a single environment
        drop
        """
        env_name = "env_vars"
        env_drop = create_std_env_vars(name=env_name)
        test_drop = AbstractDROP(uid="b", oid="b")
        test_drop.addProducer(env_drop)
        expected_vars = [
            f"${env_name}.uid",
            "/HOME/",
            3,
            False,
            0.5,
            {"first": 1, "second": "sec"},
            [1, 2.0, "3"],
            f"${env_name}.non_var",
        ]
        query_keys = [
            "uid",
            "dir_var",
            "int_var",
            "bool_var",
            "float_var",
            "dict_var",
            "list_var",
            "non_var",
        ]
        query_keys = [
            f"${env_name}.{x}" for x in query_keys
        ]  # Build queries of the correct form
        # Add some purposefully malformed vars
        extra_keys = ["dir_var", "$non_store.non_var"]
        query_keys.extend(extra_keys)
        expected_vars.extend(extra_keys)
        self.assertEqual(
            expected_vars, test_drop.get_environment_variables(query_keys)
        )

    def test_drop_get_empty(self):
        """
        Tests the case where the environment drop has no name
        """
        env_name = ""
        env_drop = create_empty_env_vars(name=env_name)
        test_drop = AbstractDROP(uid="c", oid="c")
        test_drop.addProducer(env_drop)
        self.assertEqual("", test_drop.get_environment_variable(""))
        self.assertEqual("$", test_drop.get_environment_variable("$"))

    def test_drop_get_multiEnv(self):
        """
        Tests the AbstractDROP fetch routine with multiple environment drops
        """
        env1_name = "env_vars"
        env2_name = "more_vars"
        env1_drop = create_std_env_vars(name=env1_name)
        env2_drop = EnvironmentVarDROP(
            oid="d", uid="d", name=env2_name, dir_var="/DIFFERENT/", int_var=4
        )
        test_drop = AbstractDROP(uid="c", oid="c")
        test_drop.addProducer(env1_drop)
        test_drop.addProducer(env2_drop)
        self.assertEqual(
            "/HOME/",
            test_drop.get_environment_variable(f"${env1_name}.dir_var"),
        )
        self.assertEqual(
            "/DIFFERENT/",
            test_drop.get_environment_variable(f"${env2_name}.dir_var"),
        )
        self.assertEqual(
            3, test_drop.get_environment_variable(f"${env1_name}.int_var")
        )
        self.assertEqual(
            4, test_drop.get_environment_variable(f"${env2_name}.int_var")
        )
        self.assertEqual(
            f"{env1_name}.int_var",
            test_drop.get_environment_variable(f"{env1_name}.int_var"),
        )
        self.assertEqual(
            f".int_var", test_drop.get_environment_variable(f".int_var")
        )
        self.assertEqual(
            f"$third_env.int_var",
            test_drop.get_environment_variable(f"$third_env.int_var"),
        )
        self.assertEqual(
            [
                "/HOME/",
                "/DIFFERENT/",
                3,
                4,
                f"${env1_name}.non_var",
                "$fake.var",
            ],
            test_drop.get_environment_variables(
                [
                    f"${env1_name}.dir_var",
                    f"${env2_name}.dir_var",
                    f"${env1_name}.int_var",
                    f"${env2_name}.int_var",
                    f"${env1_name}.non_var",
                    "$fake.var",
                ]
            ),
        )

    def test_autofill_environment_vars(self):
        """
        Tests the autofilling functionality of AbstractDROP
        """
        env_drop = create_std_env_vars(name="env_vars")
        test_drop = AbstractDROP(
            oid="a",
            uid="a",
            dir_var="$env_vars.dir_var",
            int_var="$env_vars.int_var",
            non_var=set(),
        )
        test_drop.addProducer(env_drop)
        test_drop.autofill_environment_variables()
        self.assertEqual("/HOME/", test_drop.parameters["dir_var"])
        self.assertEqual(3, test_drop.parameters["int_var"])

    def test_get_dlg_vars(self):
        test_drop = AbstractDROP(
            oid="a",
            uid="a",
            dlg_root="$DLG_ROOT",
            non_dlg_var="$DLG_NONEXISTS",
            non_var=set(),
        )
        test_drop.autofill_environment_variables()
        self.assertEqual(getDlgDir(), test_drop.parameters["dlg_root"])
        self.assertEqual(
            getDlgDir(), test_drop.get_environment_variable("$DLG_ROOT")
        )
        self.assertEqual("$DLG_NONEXISTS", test_drop.parameters["non_dlg_var"])
        self.assertEqual(
            "$DLG_NONEXISTS",
            test_drop.get_environment_variable("$DLG_NONEXISTS"),
        )

    def test_filename_integration(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            os.environ["DLG_ROOT"] = tmp_dir
            os.environ["DLG_FILE"] = "test_file"
            test_drop = FileDROP(
                oid="a", uid="a", filepath="$DLG_ROOT/$DLG_FILE"
            )
            test_drop.write(b"1234")
            self.assertEqual(tmp_dir, test_drop.dirname)
            self.assertEqual("test_file", test_drop.filename)
