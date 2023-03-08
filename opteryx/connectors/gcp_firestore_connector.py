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
import typing

from opteryx import config
from opteryx.connectors import BaseDocumentStorageAdapter
from opteryx.connectors.capabilities import PredicatePushable
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
        firebase_admin.initialize_app(creds, {"projectId": project_id})


class GcpFireStoreConnector(BaseDocumentStorageAdapter, PredicatePushable):
    def __init__(self, *args, prefix: str = "", remove_prefix: bool = False, **kwargs):
        BaseDocumentStorageAdapter.__init__(
            self, *args, prefix=prefix, remove_prefix=remove_prefix, **kwargs  # type: ignore
        )
        PredicatePushable.__init__(
            self, *args, prefix=prefix, remove_prefix=remove_prefix, **kwargs
        )
        self.supported_ops = ["=="]

        self._remove_prefix = remove_prefix
        self._prefix = prefix

    def get_document_count(self, collection) -> int:  # pragma: no cover
        """
        Return an interable of blobs/files

        FireStore currently doesn't support this
        """
        return -1

    def read_documents(self, collection, morsel_size: typing.Union[int, None] = None):
        """
        Return a morsel of documents
        """
        from firebase_admin import firestore

        if morsel_size is None:
            morsel_size = config.MORSEL_SIZE

        queried_collection = collection
        if self._remove_prefix:
            if collection.startswith(f"{self._prefix}."):
                queried_collection = collection[len(self._prefix) + 1 :]

        _initialize()
        database = firestore.client()
        documents = database.collection(queried_collection)

        for predicate in self._predicates:
            documents = documents.where(*predicate)

        documents = documents.stream()

        for morsel in self.chunk_dictset(
            ({**doc.to_dict(), "_id": doc.id} for doc in documents), morsel_size
        ):
            yield morsel
