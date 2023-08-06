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


class HadroConnector(BaseDocumentStorageAdapter):
    def __init__(self, *args, prefix: str = "", remove_prefix: bool = False, **kwargs):
        BaseDocumentStorageAdapter.__init__(
            self, *args, prefix=prefix, remove_prefix=remove_prefix, **kwargs  # type: ignore
        )
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
        from hadrodb import HadroDB

        if morsel_size is None:
            morsel_size = config.MORSEL_SIZE

        queried_collection = collection
        if self._remove_prefix:
            if collection.startswith(f"{self._prefix}."):
                queried_collection = collection[len(self._prefix) + 1 :]
        queried_collection = queried_collection.replace(".", "/")

        hadro = HadroDB(collection=queried_collection)

        for morsel in self.chunk_dictset((hadro[doc] for doc in hadro.keys()), morsel_size):
            yield morsel
