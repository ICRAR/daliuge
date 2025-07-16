#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2017
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

import re

inp_regex = re.compile(r"%i\[(-[0-9]+)\]")
out_regex = re.compile(r"%o\[(-[0-9]+)\]")


class BashCommand(object):
    """
    An efficient implementation of the bash command with parameters
    """

    def __init__(self, cmds):
        """
        create the logical form of the bash command line

        cmds: a list such that ' '.join(cmds) looks something like:
                 'python /home/dfms/myclean.py -d %i[-21] -f %i[-3] %o[-2] -v'
        """
        self._input_map = (
            dict()
        )  # key: logical drop id, value: a list of physical oids
        self._output_map = dict()
        if len(cmds) > 0 and isinstance(cmds[0], dict):
            cmds = [list(c.keys())[0] for c in cmds]
        cmd = " ".join(cmds)
        self._cmds = cmd.replace(
            ";", " ; "
        ).split()  # re-split just in case hidden spaces
        # self._cmds = re.split(';| *', cmd) # resplit for * as well as spaces

        for m in inp_regex.finditer(cmd):
            self._input_map[
                int(m.group(1))
            ] = set()  # TODO - check if sequence needs to be reserved!
        for m in out_regex.finditer(cmd):
            self._output_map[int(m.group(1))] = set()

    def add_input_param(self, lgn_id, oid):
        lgn_id = int(lgn_id)
        if lgn_id in self._input_map:
            self._input_map[lgn_id].add(oid)

    def add_output_param(self, lgn_id, oid):
        lgn_id = int(lgn_id)
        if lgn_id in self._output_map:
            self._output_map[lgn_id].add(oid)

    def to_real_command(self):
        def _get_delimit(matchobj):
            return " " if matchobj.start() == 0 else ","

        for k, _ in enumerate(self._cmds):
            d = self._cmds[k]
            imatch = inp_regex.search(d)
            omatch = out_regex.search(d)
            if imatch is not None:
                lgn_id = int(imatch.group(1))
                self._cmds[k] = d.replace(
                    imatch.group(0),
                    _get_delimit(imatch).join(
                        ["%i[{0}]".format(x) for x in self._input_map[lgn_id]]
                    ),
                )
            elif omatch is not None:
                lgn_id = int(omatch.group(1))
                self._cmds[k] = d.replace(
                    omatch.group(0),
                    _get_delimit(omatch).join(
                        ["%o[{0}]".format(x) for x in self._output_map[lgn_id]]
                    ),
                )

        return " ".join(self._cmds)
