#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia
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
"""

"""
import os
import unittest

from dlg.json_drop import JsonDROP

DIR = '/tmp/daliuge_tfiles'
FILE_TEXT = '''
{
  "Observed from": "20-Jan-2014/04:20:12.0",
  "Observed to": "20-Jan-2014/06:01:40.0",
  "MeasurementSet Name": "/mnt/hidata/chiles/final_products/20140120_941_2_FINAL_PRODUCTS/13B-266.sb25390589.eb28661773.56677.175470648144_calibrated_deepfield.ms",
  "Observer": "Jacqueline H. van Gorkom",
  "Fields": {
    "Fields": [
        ["0",
         "NONE",
         "deepfield",
         "10:01:24.000000",
         "+02.21.00.00000",
         "J2000",
         "0",
         "2535000"
        ]
      ],
    "Field count": "1"
  },
  "Project": "uid://evla/pdb/25320050",
  "Data records": "2535000",
  "Total integration time": "6088 seconds",
  "Spectral Windows": {
    "Spectral Windows details": "(15 unique spectral windows and 1 unique polarization setups)",
    "Spectral Windows": [
      ["0",  "EVLA_L#A0C0#0",  "2048", "TOPO", "941.000",  "15.625", "32000.0", "12", "RR", "LL"],
      ["1",  "EVLA_L#A0C0#1",  "2048", "TOPO", "973.000",  "15.625", "32000.0", "12", "RR", "LL"],
      ["2",  "EVLA_L#A0C0#2",  "2048", "TOPO", "1005.000", "15.625", "32000.0", "12", "RR", "LL"],
      ["3",  "EVLA_L#A0C0#3",  "2048", "TOPO", "1037.000", "15.625", "32000.0", "12", "RR", "LL"],
      ["4",  "EVLA_L#A0C0#4",  "2048", "TOPO", "1069.000", "15.625", "32000.0", "12", "RR", "LL"],
      ["5",  "EVLA_L#A0C0#5",  "2048", "TOPO", "1101.000", "15.625", "32000.0", "12", "RR", "LL"],
      ["6",  "EVLA_L#A0C0#6",  "2048", "TOPO", "1133.000", "15.625", "32000.0", "12", "RR", "LL"],
      ["7",  "EVLA_L#A0C0#7",  "2048", "TOPO", "1165.000", "15.625", "32000.0", "12", "RR", "LL"],
      ["8",  "EVLA_L#A0C0#8",  "2048", "TOPO", "1197.000", "15.625", "32000.0", "12", "RR", "LL"],
      ["9",  "EVLA_L#A0C0#9",  "2048", "TOPO", "1229.000", "15.625", "32000.0", "12", "RR", "LL"],
      ["10", "EVLA_L#A0C0#10", "2048", "TOPO", "1261.000", "15.625", "32000.0", "12", "RR", "LL"],
      ["11", "EVLA_L#A0C0#11", "2048", "TOPO", "1293.000", "15.625", "32000.0", "12", "RR", "LL"],
      ["12", "EVLA_L#A0C0#12", "2048", "TOPO", "1325.000", "15.625", "32000.0", "12", "RR", "LL"],
      ["13", "EVLA_L#A0C0#13", "2048", "TOPO", "1357.000", "15.625", "32000.0", "12", "RR", "LL"],
      ["14", "EVLA_L#A0C0#14", "2048", "TOPO", "1389.000", "15.625", "32000.0", "12", "RR", "LL"]
    ]
  },
  "Bottom edge": "941.000",
  "Observation": "EVLA(26 antennas)"
}
'''


class TestJsonDROP(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not os.path.exists(DIR):
            os.makedirs(DIR)

        #with open(os.path.join(DIR, 'oid___uid'), mode='w') as write_file:
        with open(os.path.join(DIR, 'uid'), mode='w') as write_file:
            write_file.write(FILE_TEXT)

    def test_Json1(self):
        drop = JsonDROP('oid', 'uid')

        self.assertEqual(drop['Bottom edge'], '941.000')
        self.assertEqual(drop['Observation'], 'EVLA(26 antennas)')

        spectral_windows = drop['Spectral Windows']
        self.assertEqual(len(spectral_windows['Spectral Windows']), 15)
