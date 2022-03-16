.. _plasma_components:

Plasma Components
=================


Plasma Drops are a special shared memory drop where memory is managed by an external data store process
that is selected using a filesystem socket. The Plasma data store counts Plasma object references which are automatically incremented
decremented by DALiuGE.

Plasma allows direct memory access between DALiuGE, native apps and docker apps that link to the apache arrow Plasma library.

Plasma Drop
-----------

:class:`PlasmaDROP <dlg.drop.PlasmaDROP>` is the basic Plasma component implementation that serializes data to a fixed size Plasma buffer
allocated in a Plasma store. A Plasma socket filesystem location must be specified. Daliuge will automatically populate the drop
PlasmaID parameter. 

It is worth noting that PlasmaDrop can only share memory to processes and virtual machines running on the same physical machine. In a compute
cluster stream proxies will be created to transfer data across nodes.

PlasmaFlight Drop (Experimental)
--------------------------------

:class:`PlasmaFlightDROP <dlg.drop.PlasmaFlightDROP>` is an extended Plasma Drop implementation using Apache Arrow Flight for network
communication (details at https://github.com/ICRAR/plasmaflight).

Arrow Flight queries can be made by using a Plasma ObjectID as the flight ticket.

PlasmaFlight drops are effective for sharing 

PlasmaFlight Service
--------------------

A service application that can host both the Plasma DataStore and PlasmaFlight server for Plasma components.
