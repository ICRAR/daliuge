Users Guide Introduction
========================

The |daliuge| system can and has been used on a huge range of different size systems. It can easily be deployed on a person's laptop, across multiple personal machines as well as small and large compute clusters. That flexibility comes with some difficulty in describing how the system is intended to be used, since that is obviously dependent on the way it is deployed. This guide mainly describes the basic usage of the system as it would appear when deployed on a single machine or a small cluster. Other deployment scenarios and their respective differences are described in the :ref:`deployment` chapter. The purpose of |daliuge| is to allow users to develop and execute complex parallel workflows and as such it's real strength only shines when it comes to massive deployments. However, the basic usage does not really change at all and many real-life, mostly weak scaling workflows can be scaled up and down by changing just one or a few parameters.

Hopefully you will be able to identify yourself with one (or more) of the four user groups:

* Scientists who want to reduce their data using an existing workflow.
* Scientists who want to design a new workflow using existing components.
* Developers who want to develop new components.
* Developers who want to develop a new algorithm.

This guide will not cover the last two user groups, since those are covered by the :ref:`development` chapter.

This guide will not cover the usage of the EAGLE editor in any more detail than required, since that is covered in the `EAGLE documentation <https://eagle-dlg.readthedocs.io>`_.