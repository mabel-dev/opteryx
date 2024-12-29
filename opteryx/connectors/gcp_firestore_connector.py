# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Dict
from typing import Generator

from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.types import OrsoTypes

from opteryx import config
from opteryx.connectors.base.base_connector import INITIAL_CHUNK_SIZE
from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.capabilities import PredicatePushable
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import MissingDependencyError
from opteryx.exceptions import UnmetRequirementError

GCP_PROJECT_ID = config.GCP_PROJECT_ID


def _get_project_id():  # pragma: no cover
    """Fetch the ID from GCP"""
    try:
        import requests
    except ImportError as exception:  # pragma: no cover
        raise UnmetRequirementError(
            "Firestore requires 'GCP_PROJECT_ID` to be set in config, or "
            "`requests` to be installed."
        ) from exception

    # if it's set in the config/environ, use that
    if GCP_PROJECT_ID:
        return GCP_PROJECT_ID

    # otherwise try to get it from GCP
    response = requests.get(
        "http://metadata.google.internal/computeMetadata/v1/project/project-id",
        headers={"Metadata-Flavor": "Google"},
        timeout=10,
    )
    response.raise_for_status()
    return response.text


def _initialize():  # pragma: no cover
    """Create the connection to Firebase"""
    try:
        from google.cloud import firestore
    except ImportError as err:  # pragma: no cover
        raise MissingDependencyError(err.name) from err

    project_id = GCP_PROJECT_ID
    if project_id is None:
        project_id = _get_project_id()
    return firestore.Client(project=project_id)


class GcpFireStoreConnector(BaseConnector, PredicatePushable):
    __mode__ = "Collection"
    __type__ = "FIRESTORE"

    PUSHABLE_OPS: Dict[str, bool] = {"Eq": True, "NotEq": True}

    OPS_XLAT: Dict[str, str] = {"Eq": "==", "NotEq": "!="}

    PUSHABLE_TYPES = {OrsoTypes.BOOLEAN, OrsoTypes.DOUBLE, OrsoTypes.INTEGER, OrsoTypes.VARCHAR}

    def __init__(self, **kwargs):
        BaseConnector.__init__(self, **kwargs)
        PredicatePushable.__init__(self, **kwargs)

    def read_dataset(
        self,
        columns: list = None,
        chunk_size: int = INITIAL_CHUNK_SIZE,
        predicates: list = None,
        **kwargs,
    ) -> Generator:
        """
        Return a morsel of documents
        """
        from google.cloud.firestore_v1.base_query import FieldFilter

        from opteryx.utils.file_decoders import filter_records

        database = _initialize()
        documents = database.collection(self.dataset)

        collected_predicates = []
        have_pushed_a_negative = False

        if predicates:
            for predicate in predicates:
                if have_pushed_a_negative and predicate.value == "NotEq":
                    collected_predicates.append(predicate)
                    continue
                if predicate.value == "NotEq":
                    have_pushed_a_negative = True
                documents = documents.where(
                    filter=FieldFilter(
                        predicate.left.source_column,
                        self.OPS_XLAT[predicate.value],
                        predicate.right.value,
                    )
                )

        documents = documents.stream()

        for morsel in self.chunk_dictset(
            ({**doc.to_dict(), "_id": doc.id} for doc in documents),
            columns=columns,
            initial_chunk_size=chunk_size,
        ):
            if collected_predicates:
                morsel = filter_records(collected_predicates, morsel)
            yield morsel

    def get_dataset_schema(self) -> RelationSchema:
        if self.schema:
            return self.schema

        # only read one record
        record = next(self.read_dataset(chunk_size=10), None)

        if record is None:
            raise DatasetNotFoundError(dataset=self.dataset)

        arrow_schema = record.schema

        self.schema = RelationSchema(
            name=self.dataset,
            columns=[FlatColumn.from_arrow(field) for field in arrow_schema],
        )

        return self.schema
