import heapq
import logging
from .data_base import DataDROP
from dlg.exceptions import InvalidRelationshipException
from sqlite3 import OperationalError
from dlg.data.io import (
    ErrorIO,
)
from dlg.ddap_protocol import (
    DROPLinkType,
    DROPRel,
)

logger = logging.getLogger(f"dlg.{__name__}")


class ContainerDROP(DataDROP):
    """
    A DROP that doesn't directly point to some piece of data, but instead
    holds references to other DROPs (its children), and from them its own
    internal state is deduced.

    Because of its nature, ContainerDROPs cannot be written to directly,
    and likewise they cannot be read from directly. One instead has to pay
    attention to its "children" DROPs if I/O must be performed.
    """

    def initialize(self, **kwargs):
        super(DataDROP, self).initialize(**kwargs)
        self._children = []

    # ===========================================================================
    # No data-related operations should actually be called in Container DROPs
    # ===========================================================================
    def getIO(self):
        return ErrorIO()

    @property
    def dataURL(self):
        raise OperationalError()

    def addChild(self, child):

        # Avoid circular dependencies between Containers
        if child == self.parent:
            raise InvalidRelationshipException(
                DROPRel(child, DROPLinkType.CHILD, self),
                "Circular dependency found",
            )

        logger.debug("Adding new child for %r: %r", self, child)

        self._children.append(child)
        child.parent = self

    def delete(self):
        # TODO: this needs more thinking. Probably a separate method to perform
        #       this recursive deletion will be needed, while this delete method
        #       will go hand-to-hand with the rest of the I/O methods above,
        #       which are currently raise a NotImplementedError
        if self._children:
            for c in [c for c in self._children if c.exists()]:
                c.delete()

    @property
    def expirationDate(self):
        if self._children:
            return heapq.nlargest(1, [c.expirationDate for c in self._children])[0]
        return self._expirationDate

    @property
    def children(self):
        return self._children[:]

    def exists(self):
        if self._children:
            # TODO: Or should it be all()? Depends on what the exact contract of
            #       "exists" is
            return any([c.exists() for c in self._children])
        return True
