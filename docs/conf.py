# -*- coding: utf-8 -*-
#
# daliuge documentation build configuration file, created by
# sphinx-quickstart on Mon Feb  8 16:19:47 2016.

import os
import subprocess
import sys
from datetime import datetime

read_the_docs_build = os.environ.get("READTHEDOCS", None) == "True"


def prepare_for_docs(path):
    # Run "python setup.py build" to generate the version.py files, and make
    # packages available for documenting their APIs
    path = os.path.abspath(path)
    sys.path.insert(0, path)
    if read_the_docs_build:
        subprocess.Popen([sys.executable, "setup.py", "build"], cwd=path).wait()


prepare_for_docs("../daliuge-common")
prepare_for_docs("../daliuge-translator")
prepare_for_docs("../daliuge-engine")

# Mock the rest of the external modules we need so the API autodoc
# gets correctly generated
try:
    from unittest.mock import MagicMock
except:
    from mock import Mock as MagicMock


class Mock(MagicMock):
    @classmethod
    def __getattr__(cls, _):
        return MagicMock()


MOCK_MODULES = (
    "boto3",
    "botocore",
    "bottle",
    "configobj",
    "crc32c",
    "dill",
    "docker",
    "docker.models",
    "docker.models.containers",
    "gevent",
    "lockfile",
    "metis",
    "netifaces",
    "networkx",
    "numpy",
    "overrides",
    "paramiko",
    "paramiko.client",
    "paramiko.rsakey",
    "psutil",
    "pyswarm",
    "python-daemon",
    "pyzmq",
    "scp",
    "zeroconf",
    "zerorpc",
    "pyext",
)
sys.modules.update((mod_name, Mock()) for mod_name in MOCK_MODULES)

# -- General configuration ------------------------------------------------

needs_sphinx = "1.3"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.todo",
    # "sphinx.ext.coverage",
    "sphinx.ext.imgmath",
    "sphinx_rtd_theme",
]
templates_path = ["_templates"]
source_suffix = [".rst", ".md"]
master_doc = "index"

# General information about the project.
project = "daliuge"
copyright = "2016-{0}, ICRAR".format(datetime.now().year)
author = "ICRAR"

try:
    from dlg.common.version import version, full_version as release
except ImportError:
    version = "0.2.0"
    release = version

language = "en"
app_development = "development/app_development/"
exclude_patterns = ["_build", "development/wip_docs/*", "development/dev/*"]

pygments_style = "sphinx"
# If true, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = True

numfig = True

# Common definitions used across the board
rst_prolog = """
.. |daliuge| replace:: DALiuGE
"""

# -- Options for HTML output ----------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
html_css_files = ["css/daliuge.css"]
html_style = "css/daliuge.css"
htmlhelp_basename = "daliugedoc"

html_logo = "images/DLGLogo_White.png"
html_theme_options = {"logo_only": True}
# -- Options for LaTeX output ---------------------------------------------

latex_elements = {}

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title,
#  author, documentclass [howto, manual, or own class]).
latex_documents = [
    (master_doc, "daliuge.tex", "daliuge Documentation", "ICRAR", "manual")
]

# -- Options for manual page output ---------------------------------------

man_pages = [(master_doc, "daliuge", "daliuge Documentation", [author], 1)]

# -- Options for Texinfo output -------------------------------------------

texinfo_documents = [
    (
        master_doc,
        "daliuge",
        "daliuge Documentation",
        author,
        "daliuge",
        "One line description of project.",
        "Miscellaneous",
    )
]
