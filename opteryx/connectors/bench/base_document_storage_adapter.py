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
Base Inner Reader for nosql document stores
"""
from typing import Iterable

import pyarrow


class BaseDocumentStorageAdapter:
    __mode__ = "Collection"

    def __init__(self, prefix: str, remove_prefix: bool = False) -> None:
        self._prefix = prefix
        self._remove_prefix = remove_prefix
        self.chunk_size = 500

    def chunk_dictset(self, dictset: Iterable[dict], morsel_size: int):
        chunk_size = self.chunk_size
        chunk = [{} for _ in range(chunk_size)]
        first_chunk = True

        for index, record in enumerate(dictset):
            _id = record.pop("_id", None)  # Inlining the transformation
            record["id"] = None if _id is None else str(_id)

            chunk[index % chunk_size] = record

            if index > 0 and index % chunk_size == 0:
                morsel = pyarrow.Table.from_pylist(chunk)

                if first_chunk and chunk_size == 500 and morsel.nbytes > 0:
                    chunk_size = int(morsel_size // (morsel.nbytes / 500))
                    self.chunk_size = chunk_size
                    first_chunk = False
                    chunk = [{} for _ in range(chunk_size)]  # Reallocate chunk if size changes

                yield morsel
                chunk = [{} for _ in range(chunk_size)]  # Reallocate chunk
                chunk[0] = record

        yield pyarrow.Table.from_pylist(chunk[: (index + 1) % chunk_size])

    def get_document_count(self, collection) -> int:  # pragma: no cover
        """
        Return the count, or an estimate of, the number of documents
        """
        raise NotImplementedError("get_document_list not implemented")

    def read_documents(self, collection, morsel_size: int = 500):  # pragma: no cover
        """
        Return a morsel of documents
        """
        raise NotImplementedError("read_document not implemented")

    @property
    def can_push_selection(self):
        return False
