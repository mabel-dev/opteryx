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
import os

from opteryx import config

from opteryx.connectors import BaseDocumentStorageAdapter
from opteryx.exceptions import MissingDependencyError
from opteryx.exceptions import UnmetRequirementError

try:
    import firebase_admin
    from firebase_admin import credentials
    from firebase_admin import firestore

    HAS_FIREBASE = True
except ImportError:  # pragma: no cover
    HAS_FIREBASE = False

GCP_PROJECT_ID = config.GCP_PROJECT_ID
BATCH_SIZE = 5000


def _get_project_id():  # pragma: no cover
    """Fetch the ID from GCP"""
    try:
        import requests
    except ImportError as exception:  # pragma: no cover
        raise UnmetRequirementError(
            "Firestore requires 'GCP_PROJECT_ID` to be set in config, or "
            "`requests` to be installed."
        ) from exception

    # if it's set in the environ, use that
    project_id = os.environ.get("GCP_PROJECT_ID")
    if project_id:
        return project_id

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
    if not HAS_FIREBASE:
        raise MissingDependencyError(
            "`firebase-admin` missing, please install or add to requirements.txt"
        )
    if not firebase_admin._apps:
        # if we've not been given the ID, fetch it
        project_id = GCP_PROJECT_ID
        if project_id is None:
            project_id = _get_project_id()
        creds = credentials.ApplicationDefault()
        firebase_admin.initialize_app(creds, {"projectId": project_id})


class GcpFireStoreConnector(BaseDocumentStorageAdapter):
    def get_document_count(self, collection) -> int:  # pragma: no cover
        """
        Return an interable of blobs/files

        FireStore currently doesn't support this
        """
        return -1

    def read_documents(self, collection, page_size: int = BATCH_SIZE):
        """
        Return a page of documents
        """
        _initialize()
        database = firestore.client()
        documents = database.collection(collection).stream()

        for page in self.page_dictset(
            ({**doc.to_dict(), "_id": doc.id} for doc in documents), page_size
        ):
            yield page
