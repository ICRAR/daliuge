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
Basic Hello World script for the basic.rst tutorial.

Designed to mimic the functionality of dlg.apps.simple.HelloWorld, which takes
a 'greet' parameter as input and returns "Hello " + greet.
"""
import argparse

import numpy as np


def hello_world(greet: str="World"):
    """
     Designed to mimic the functionality of dlg.apps.simple.HelloWorld, which takes
    a 'greet' parameter as input and returns "Hello " + greet.

    :param greet: The 'item' we are greeting.
    :return: str
    """
    final_greet = greet
    if isinstance(greet, list) or isinstance(greet, np.array):
        if not greet:
            final_greet = ""
        else:
            final_greet =" ".join(g for g in g) if len(greet) > 1 else greet[0]

    return f"Hello, {final_greet}"

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--greet", type=str, default="World",
                        help="The greeting we want to apply")
    args = parser.parse_args() 
    print(hello_world(args.greet))