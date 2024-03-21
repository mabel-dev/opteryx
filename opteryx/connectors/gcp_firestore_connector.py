# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Generator

from orso.schema import FlatColumn
from orso.schema import RelationSchema

from opteryx import config
from opteryx.connectors.base.base_connector import INITIAL_CHUNK_SIZE
from opteryx.connectors.base.base_connector import BaseConnector
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
        import firebase_admin
        from firebase_admin import credentials
    except ImportError as err:  # pragma: no cover
        raise MissingDependencyError(err.name) from err
    if not firebase_admin._apps:
        # if we've not been given the ID, fetch it
        project_id = GCP_PROJECT_ID
        if project_id is None:
            project_id = _get_project_id()
        creds = credentials.ApplicationDefault()
        firebase_admin.initialize_app(creds, {"projectId": project_id, "httpTimeout": 10})


class GcpFireStoreConnector(BaseConnector):
    __mode__ = "Collection"

    def read_dataset(
        self, columns: list = None, chunk_size: int = INITIAL_CHUNK_SIZE, **kwargs
    ) -> Generator:
        """
        Return a morsel of documents
        """
        from firebase_admin import firestore

        _initialize()
        database = firestore.client()
        documents = database.collection(self.dataset)

        #        for predicate in self._predicates:
        #            documents = documents.where(*predicate)

        documents = documents.stream()

        for morsel in self.chunk_dictset(
            ({**doc.to_dict(), "_id": doc.id} for doc in documents),
            columns=columns,
            initial_chunk_size=chunk_size,
        ):
            yield morsel

    def get_dataset_schema(self) -> RelationSchema:
        if self.schema:
            return self.schema

        # Try to read the schema from the metastore
        self.schema = self.read_schema_from_metastore()
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
