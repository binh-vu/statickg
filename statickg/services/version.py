from __future__ import annotations

from typing import NotRequired, TypedDict

from rdflib import DCTERMS, RDF, XSD, Graph, Literal, URIRef

from statickg.models.prelude import ETLOutput, RelPath, Repository
from statickg.services.interface import BaseFileService


class VersionServiceConstructArgs(TypedDict): ...


class VersionServiceInvokeArgs(TypedDict):
    entity: str  # uri of the entity
    entity_type: NotRequired[str]  # type of the entity
    output: RelPath | str


class VersionService(BaseFileService[VersionServiceInvokeArgs]):
    """A service that can generate version of knowledge graph"""

    def forward(
        self, repo: Repository, args: VersionServiceInvokeArgs, tracker: ETLOutput
    ):
        """Generate version of knowledge graph"""
        g = Graph()

        ent = URIRef(args["entity"])
        if "entity_type" in args:
            g.add(
                (
                    ent,
                    RDF.type,
                    URIRef(args["entity_type"]),
                )
            )

        g.add((ent, DCTERMS.hasVersion, Literal(repo.get_version_id())))
        g.add(
            (
                ent,
                DCTERMS.created,
                Literal(
                    repo.get_version_creation_time().isoformat(), datatype=XSD.dateTime
                ),
            )
        )

        outfile = (
            args["output"].get_path()
            if isinstance(args["output"], RelPath)
            else args["output"]
        )
        self.logger.info(
            "Writing version to {}",
            (
                args["output"].get_ident()
                if isinstance(args["output"], RelPath)
                else args["output"]
            ),
        )
        g.serialize(outfile, format="turtle")
