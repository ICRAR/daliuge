from copy import deepcopy
from dataclasses import dataclass, field
from typing import TypeVar

L = TypeVar("L", bound="LogicalArtefact")
P = TypeVar("P", bound="PhysicalArtefact")


@dataclass(frozen=True)
class LogicalArtefact:
    """
    Shared shape for the dict-form logical artefacts.

    `source`: EAGLE JSON document, reprodata key included.
    """

    source: dict

    @classmethod
    def from_wire(cls: type[L], payload: dict) -> L:
        """
        Wrap an already-parsed EAGLE JSON document.
        """
        if not isinstance(payload, dict):
            raise TypeError(
                f"{cls.__name__} wire form must be a dict, "
                f"got {type(payload).__name__}"
            )

        reprodata = payload.get("reprodata")
        if reprodata is not None and not isinstance(reprodata, dict):
            raise TypeError(
                f"{cls.__name__} 'reprodata' must be a dict, "
                f"got {type(reprodata).__name__}"
            )

        return cls(source=deepcopy(payload))

    def to_wire(self) -> dict:
        """
        Return the graph as a fresh document the caller owns.
        """
        return deepcopy(self.source)

    @property
    def reprodata(self) -> dict:
        return self.source.get("reprodata", {})


@dataclass(frozen=True)
class PhysicalArtefact:
    """
    Shared shape for the list-form physical artefacts.
    """

    drops: list[dict]
    reprodata: dict = field(default_factory=dict)

    @classmethod
    def from_wire(cls: type[P], payload: list[dict]) -> P:
        """
        Split a wire list into drops and reprodata.
        """
        if not isinstance(payload, list):
            raise TypeError(
                f"{cls.__name__} wire form must be a list, "
                f"got {type(payload).__name__}"
            )

        if not all(isinstance(e, dict) for e in payload):
            bad = next(i for i, e in enumerate(payload) if not isinstance(e, dict))
            raise TypeError(
                f"{cls.__name__} element {bad} must be a dict, "
                f"got {type(payload[bad]).__name__}"
            )

        if payload and isinstance(payload[-1], dict) and not payload[-1].get("oid"):
            return cls(
                drops=deepcopy(payload[:-1]),
                reprodata=deepcopy(payload[-1])
            )
        else:
            return cls(drops=deepcopy(payload))

    def to_wire(self) -> list:
        """
        Return drops with reprodata appended
        """
        return deepcopy([*self.drops, self.reprodata])


@dataclass(frozen=True)
class LogicalGraphTemplate(LogicalArtefact):
    """Bare EAGLE's graph output"""


@dataclass(frozen=True)
class LogicalGraph(LogicalArtefact):
    """LGT with its parameters resolved and graph configuration applied."""


@dataclass(frozen=True)
class PhysicalGraphTemplate(PhysicalArtefact):
    """Expanded logical graph, a flat list of drops where the workflow will run"""


@dataclass(frozen=True)
class PhysicalGraphTemplatePartitioned(PhysicalArtefact):
    """Partitioned PGT with placeholders node and island (`#N` / `#M`)"""


@dataclass(frozen=True)
class PhysicalGraph(PhysicalArtefact):
    """Complete physical graph with real hostnames, engine ready"""
