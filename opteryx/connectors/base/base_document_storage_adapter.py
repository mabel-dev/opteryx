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
import abc
from typing import Iterable

import pyarrow


class BaseDocumentStorageAdapter(abc.ABC):
    __mode__ = "Collection"

    def __init__(self, prefix: str, remove_prefix: bool = False) -> None:
        self._prefix = prefix
        self._remove_prefix = remove_prefix
        self.chunk_size = 500

    def chunk_dictset(self, dictset: Iterable[dict], morsel_size: int):
        """
        Enables paging through a dictset by returning a chunk of records at a time.
        Parameters:
            dictset: iterable of dictionaries:
                The dictset to process
            chunk_size: integer:
                The number of bytes per chunk
        """
        chunk_size = self.chunk_size
        index = -1
        chunk: list = [{}] * chunk_size
        for index, record in enumerate(dictset):
            _id = record.pop("_id", None)
            record["id"] = None if _id is None else str(_id)
            if index > 0 and index % chunk_size == 0:
                morsel = pyarrow.Table.from_pylist(chunk)
                # from 500 records, estimate the number of records to fill the morsel size
                # in the unlikely event that the right size for chunks is 500, we calculate
                # this every cycle
                if chunk_size == 500 and morsel.nbytes > 0:
                    chunk_size = int(morsel_size // (morsel.nbytes / 500))
                    self.chunk_size = chunk_size
                # yield after the calculation, as you probably expect this to be set after
                # the first read, not the second read
                yield morsel
                chunk = [{}] * chunk_size
                chunk[0] = record
            else:
                chunk[index % chunk_size] = record
        morsel = pyarrow.Table.from_pylist(chunk[: (index + 1) % chunk_size])
        yield morsel

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
