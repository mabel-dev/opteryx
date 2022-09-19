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

"""
A MongoDB Reader
This is a light-weight MongoDB reader to fulfil a specific purpose,
it needs some work to make it fully reusable.

Based on the now deprecated Mabel MongoDB reader
https://github.com/mabel-dev/mabel/blob/6bcd978b90870187d5eff939be3f5845a3cdf900/mabel/adapters/mongo/mongodb_reader.py
"""
import os

from opteryx import config
from opteryx.connectors import BaseDocumentStorageAdapter
from opteryx.exceptions import UnmetRequirementError

try:
    import pymongo  # type:ignore
except ImportError:  # pragma: no cover
    pass

BATCH_SIZE = 500


class MongoDbConnector(BaseDocumentStorageAdapter):
    def __init__(self):
        """establish the connection to mongodb"""
        mongo_connection = os.environ.get("MONGO_CONNECTION")
        mongo_database = os.environ.get("MONGO_DATABASE")

        if mongo_connection is None or mongo_database is None:  # pragma: no cover
            raise UnmetRequirementError(
                "MongoDB adapter requires MONGO_CONNECTION and MONGO_DATABASE set in environment variables."
            )

        client = pymongo.MongoClient(mongo_connection)
        self._database = client[mongo_database]

    def get_document_count(self, collection) -> int:
        """
        Return the count, or an estimate of, the number of documents
        """
        return self._database[collection].estimated_document_count()

    def read_documents(self, collection, page_size: int = BATCH_SIZE):
        """
        Return a page of documents
        """
        documents = self._database[collection].find()
        for page in self.page_dictset(documents, page_size):
            yield page
