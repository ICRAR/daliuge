.. _reproducibility_adding_drops:

Creating New Drop Types
=======================

Drops must supply provenance data on demand as part of our scientific reproducibility efforts.
When implementing entirely new drop types,
ensuring the availability of appropriate information is essential to continue the feature's power.

Drops supply provenance information for various 'R-modes' through ``generate_x_data`` methods.
In the case of application drops specifically,
the ``generate_recompute_data`` method may need overriding if there is any specific information
for the exact replication of this component.
For example, Python drops may supply their code or an execution trace.

In the case of data drops, the ``genreate_reproduce_data`` may need overriding
and should return a summary of the contained data. For example, the hash of a file,
a list of database queries or whatever information deemed characteristic of a data-artefact
(perhaps statistical information for science products).

Additionally, if adding an entirely new drop type,
you will need to create a new drop category in ``dlg.common.__init__.py`` and related entries in
``dlg.common.reproducibility.reproducibility_fields.py``.

