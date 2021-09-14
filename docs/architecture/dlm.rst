Data Lifecycle Manager
----------------------

As mentioned in :ref:`intro` and :ref:`dataflow.datadriven` |daliuge| also integrates
a data lifecycle management within the data processing framework. Its purpose is
to make sure the data is dealt with correctly in terms of storage, taking into
account how and when it is used. This includes, for instance, placing medium-
and long-term persistent data into the optimal storage media, and to remove
data that is not used anymore.

The current |daliuge| implementation contains a Data Lifecycle Manager (DLM).  
Because of the high coupling that is needed with all the Drops the
DLM is contained within the :ref:`node_drop_manager` processes, and thus shares
the same memory space with the Drops it manages. By subscribing to events sent
by individual Drops it can track their state and react accordingly.

The DLM functionalities currently implemented in |daliuge| are:

* Automatically expire Drops; i.e., moves them from the **COMPLETED** state
  into the **EXPIRED** state, after which they are not readable anymore.

* Automatically delete data from Drops in the **EXPIRED** state, and move the
  Drops into the **DELETED** state.

* Persist Drops' states in a registry (currently implemented with an
  in-memory registry and a RDBMS-based registry).

How and when a Drop is expired can be configured via two per-Drop, mutually
exclusive methods:

* A ``lifetime`` can be set in a Drop indicating how long should it live, and
  after which it should be moved to the **EXPIRED** state, regardless of whether
  it is still being used or not.
* A ``expire_after_use`` flag can be set in a Drop indicating that it should be
  expired right after all its consumers have finished executing.
