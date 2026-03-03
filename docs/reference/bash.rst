.. _bashapps:

BashShellApps
#############

Supported features
==================
- Input from data drops can be passed to any shell application
- Bash redirection is supported to output data using f-string style {}

Current limitations
===================

- Output is not managed by DALiuGE, so it is necessary to use path-based Drops for output, along with the :ref:`side effect` behaviour.
- Event support is limited
