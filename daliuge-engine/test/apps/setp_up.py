#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2020
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

# Try to compile the library, if possible. If it's there already we're cool
def build_shared_library(libname, libpath):
    import os
    import distutils.ccompiler
    from dlg.runtime import get_include_dir

    prev_path = os.getcwd()
    os.chdir(os.path.dirname(__file__))

    try:

        # No need to rebuild
        srcname = libname + ".c"
        if (
            os.path.isfile(libpath)
            and os.stat(srcname).st_ctime <= os.stat(libpath).st_ctime
        ):
            return True

        comp = distutils.ccompiler.new_compiler()
        distutils.sysconfig.customize_compiler(comp)
        comp.add_include_dir(distutils.sysconfig.get_python_inc())

        comp.add_include_dir(get_include_dir())
        objs = comp.compile([srcname])
        comp.link_shared_lib(objs, output_libname=libname)

        return True
    except:
        return False
    finally:
        os.chdir(prev_path)
