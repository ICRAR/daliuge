#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2016
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

import shutil
import tempfile
import unittest

import pkg_resources

from dfms import tool, utils
from dfms.restutils import RestClient, RestClientException
import os


lg_dir = pkg_resources.resource_filename(__name__, '.')  # @UndefinedVariable
lgweb_port = 8000

class TestLGWeb(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)
        self.temp_dir = tempfile.mkdtemp()
        args = ['-d', lg_dir, '-t', self.temp_dir, '-p', str(lgweb_port), '-H', 'localhost']
        self.devnull = open(os.devnull, 'wb')
        self.web_proc = tool.start_process('lgweb', args, stdout=self.devnull, stderr=self.devnull)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)
        self.devnull.close()
        utils.terminate_or_kill(self.web_proc, 10)
        unittest.TestCase.tearDown(self)

    def test_getjson(self):

        c = RestClient('localhost', lgweb_port, 10)

        # a specific one
        lg = c._get_json('/jsonbody?lg_name=logical_graphs/chiles_simple.json')
        self.assertIsNotNone(lg)

        # by default the first one found by the lg_web should be returned
        lg = c._get_json('/jsonbody')
        self.assertIsNotNone(lg)

        # doesn't exist
        self.assertRaises(RestClientException, c._get_json, '/jsonbody?lg_name=doesnt_exist.json')

    def test_postjson(self):

        c = RestClient('localhost', lgweb_port, 10)

        # new graphs cannot currently be added
        form_data = {'lg_name': 'new.json', 'lg_content': '{"id": 1, "name": "example"}'}
        self.assertRaises(RestClientException, c._post_form, '/jsonbody', form_data)

        # Replace the contents of an existing one
        # (but replace it back with original after the test)
        original_fname = os.path.join(lg_dir, 'logical_graphs', 'chiles_simple.json')
        copy_fname = tempfile.mktemp()
        shutil.copy(original_fname, copy_fname)

        try:
            form_data['lg_name'] = 'logical_graphs/chiles_simple.json'
            c._post_form('/jsonbody', form_data)
            new = c._get_json('/jsonbody?lg_name=logical_graphs/chiles_simple.json')
            self.assertIsNotNone(new)
            self.assertIn('id', new)
            self.assertIn('name', new)
            self.assertEqual(1, new['id'])
            self.assertEqual('example', new['name'])
        finally:
            shutil.move(copy_fname, original_fname)