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

from opteryx import config

from opteryx.exceptions import MissingDependencyError
from opteryx.exceptions import UnmetRequirementError
from opteryx.storage.adapters.document import BaseDocumentStorageAdapter

try:
    import firebase_admin
    from firebase_admin import credentials
    from firebase_admin import firestore

    HAS_FIREBASE = True
except ImportError:
    HAS_FIREBASE = False

GCP_PROJECT_ID = config.GCP_PROJECT_ID
BATCH_SIZE = config.INTERNAL_BATCH_SIZE

def _get_project_id():
    """ Fetch the ID from GCP """
    import requests
    
    response = requests.get(
            'http://metadata.google.internal/computeMetadata/v1/project/project-id',
            headers={
                'Metadata-Flavor': 'Google'
            })
    response.raise_for_status()
    return response.text

def _initialize():  # pragma: no cover
    """ Create the connection to Firebase """
    if not HAS_FIREBASE:
        raise MissingDependencyError(
            "`firebase-admin` missing, please install or add to requirements.txt"
        )
    if not firebase_admin._apps:
        # if we've not been given the ID, fetch it
        if GCP_PROJECT_ID is None:
            GCP_PROJECT_ID = _get_project_id()
        creds = credentials.ApplicationDefault()
        firebase_admin.initialize_app(creds, {"projectId": GCP_PROJECT_ID})


class FireStoreStorage(BaseDocumentStorageAdapter):  # pragma: no cover - no emulator
    def get_document_count(self, collection) -> int:
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

        for page in self.page_dictset((doc.to_dict() for doc in documents), page_size):
            yield page
