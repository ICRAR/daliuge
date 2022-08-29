import os

import pkg_resources

from dlg import common

global lg_dir
global pgt_dir


def lg_path(lg_name):
    return "{0}/{1}".format(lg_dir, lg_name)


def lg_exists(lg_name):
    return os.path.exists(lg_path(lg_name))


def pgt_path(pgt_name):
    return "{0}/{1}".format(pgt_dir, pgt_name)


def pgt_exists(pgt_name):
    return os.path.exists(pgt_path(pgt_name))


def lg_repo_contents():
    return _repo_contents(lg_dir)


def pgt_repo_contents():
    return _repo_contents(pgt_dir)


def _repo_contents(root_dir):
    # We currently allow only one depth level
    b = os.path.basename
    contents = {}
    for dirpath, dirnames, fnames in os.walk(root_dir):
        if ".git" in dirnames:
            dirnames.remove(".git")
        if dirpath == root_dir:
            continue

        # Not great yet -- we should do a full second step pruning branches
        # of the tree that are empty
        files = [f for f in fnames if f.endswith(".graph")]
        if files:
            contents[b(dirpath)] = files

    return contents


def file_as_string(fname, package=__name__, enc="utf8"):
    b = pkg_resources.resource_string(package, fname)  # @UndefinedVariable
    return common.b2s(b, enc)
