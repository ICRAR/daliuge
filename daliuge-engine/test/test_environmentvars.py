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

import unittest
from dlg.environmentvar_drop import EnvironmentVarDROP
from dlg.drop import AppDROP


def create_std_env_vars(name='env_vars'):
    return EnvironmentVarDROP(oid='a', uid='a', nm=name, dlg_root='/DLG_HOME/', int_var=3,
                              bool_var=False,
                              float_var=0.5, dict_var={'first': 1, 'second': 'sec'},
                              list_var=[1, 2.0, '3'])


def create_empty_env_vars(name='env_vars'):
    return EnvironmentVarDROP(oid='b', uid='b', nm=name)


class TestEnvironmentVarDROP(unittest.TestCase):

    def test_get(self):
        """
        Tests that environment variables are read in and fetched correctly.
        """
        env_drop = create_std_env_vars()
        self.assertEqual('/DLG_HOME/', env_drop.get('dlg_root'))
        self.assertEqual(3, env_drop.get('int_var'))
        self.assertEqual(False, env_drop.get('bool_var'))
        self.assertEqual(0.5, env_drop.get('float_var'))
        self.assertEqual({'first': 1, 'second': 'sec'}, env_drop.get('dict_var'))
        self.assertEqual([1, 2.0, '3'], env_drop.get('list_var'))
        self.assertIsNone(env_drop.get('non_var'))
        self.assertIsNone(env_drop.get('uid'))

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
        expected_vars = [None, '/DLG_HOME/', 3, False, 0.5, {'first': 1, 'second': 'sec'},
                         [1, 2.0, '3'], None]
        query_keys = ['uid', 'dlg_root', 'int_var', 'bool_var', 'float_var', 'dict_var', 'list_var',
                      'non_var']
        self.assertEqual(expected_vars, env_drop.get_multiple(query_keys))

    def test_set(self):
        """
        Should currently raise un-implemented, but here for completeness
        """
        env_drop = create_std_env_vars()
        self.assertRaises(NotImplementedError, env_drop.set, 'var', 'val')

    def test_drop_get_single(self):
        """
        Tests the AbstractDROP fetch routine functions correctly with a single environment drop
        """
        env_drop = create_std_env_vars()
        app_drop = AppDROP(uid='b', oid='b', environment_stores={env_drop.name: env_drop})
        print(app_drop._environment_variable_stores)
        self.assertEqual('/DLG_HOME/', app_drop.get_environment_variable('$env_vars.dlg_root'))
        self.assertEqual(3, app_drop.get_environment_variable('$env_vars.int_var'))
        self.assertEqual(False, app_drop.get_environment_variable('$env_vars.bool_var'))
        self.assertEqual(0.5, app_drop.get_environment_variable('$env_vars.float_var'))
        self.assertEqual({'first': 1, 'second': 'sec'},
                         app_drop.get_environment_variable('$env_vars.dict_var'))
        self.assertEqual([1, 2.0, '3'], app_drop.get_environment_variable('$env_vars.list_var'))
        self.assertIsNone(app_drop.get_environment_variable('$env_vars.non_var'))
        self.assertIsNone(app_drop.get_environment_variable('$env_vars.uid'))

    def test_drop_get_multiple(self):
        """
        Tests the AbstractDROP multiple fetch routine functions correctly with a single environment
        drop
        """
        env_name = 'env_vars'
        env_drop = create_std_env_vars(name=env_name)
        app_drop = AppDROP(uid='b', oid='b', environment_stores={env_drop.name: env_drop})
        expected_vars = [None, '/DLG_HOME/', 3, False, 0.5, {'first': 1, 'second': 'sec'},
                         [1, 2.0, '3'], None]
        query_keys = ['uid', 'dlg_root', 'int_var', 'bool_var', 'float_var', 'dict_var', 'list_var',
                      'non_var']
        query_keys = [f'${env_name}.{x}' for x in query_keys]  # Build queries of the correct form
        # Add some purposefully malformed vars
        query_keys.extend(['dlg_root', '$non_store.non_var'])
        expected_vars.extend([None, None])
        self.assertEqual(expected_vars, app_drop.get_environment_variables(query_keys))

    def test_drop_get_empty(self):
        """
        Tests the case where the environment drop has no name
        """
        env_name = ''
        env_drop = create_empty_env_vars(name=env_name)
        app_drop = AppDROP(uid='c', oid='c', environment_stores={env_drop.name: env_drop})
        self.assertEqual(None, app_drop.get_environment_variable(''))
        self.assertEqual(None, app_drop.get_environment_variable('$'))

    def test_drop_get_multiEnv(self):
        """
        Tests the AbstractDROP fetch routine with multiple environment drops
        """
        env1_name = 'env_vars'
        env2_name = 'more_vars'
        env1_drop = create_std_env_vars(name=env1_name)
        env2_drop = EnvironmentVarDROP(oid='d', uid='d', nm=env2_name, dlg_root='/DIFFERENT/',
                                       int_var=4)
        app_drop = AppDROP(uid='c', oid='c',
                           environment_stores={env1_name: env1_drop, env2_name: env2_drop})
        self.assertEqual('/DLG_HOME/', app_drop.get_environment_variable(f"${env1_name}.dlg_root"))
        self.assertEqual('/DIFFERENT/', app_drop.get_environment_variable(f"${env2_name}.dlg_root"))
        self.assertEqual(3, app_drop.get_environment_variable(f"${env1_name}.int_var"))
        self.assertEqual(4, app_drop.get_environment_variable(f"${env2_name}.int_var"))
        self.assertIsNone(app_drop.get_environment_variable(f'{env1_name}.int_var'))
        self.assertIsNone(app_drop.get_environment_variable(f'.int_var'))
        self.assertIsNone(app_drop.get_environment_variable(f'$third_env.int_var'))
        self.assertEqual(['/DLG_HOME/', '/DIFFERENT/', 3, 4, None, None],
                         app_drop.get_environment_variables(
                             [f'${env1_name}.dlg_root', f'${env2_name}.dlg_root',
                              f'${env1_name}.int_var', f'${env2_name}.int_var',
                              f'${env1_name}.non_var', '$fake.var']
                         ))
