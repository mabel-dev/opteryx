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

from typing import Iterable
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


class FireStoreStorage(BaseDocumentStorageAdapter):
    def _initialize():
        if not HAS_FIREBASE:
            raise MissingDependencyError(
                "`firebase-admin` missing, please install or add to requirements.txt"
            )
        if not firebase_admin._apps:
            if GCP_PROJECT_ID is None:
                raise UnmetRequirementError(
                    "Firestore requires `GCP_PROJECT_ID` set in config"
                )
            creds = credentials.ApplicationDefault()
            firebase_admin.initialize_app(creds, {"projectId": GCP_PROJECT_ID})

    def get_document_count(self, collection) -> Iterable:
        """
        Return an interable of blobs/files
        """
        raise NotImplementedError("get_document_list not implemented")

    def read_documents(self, collection, batch_size: int = 1000) -> bytes:
        """
        Return a filelike object
        """
        raise NotImplementedError("read_document not implemented")
