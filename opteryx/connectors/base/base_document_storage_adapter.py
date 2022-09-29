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


class BaseDocumentStorageAdapter(abc.ABC):

    __mode__ = "Collection"

    def page_dictset(self, dictset: Iterable[dict], page_size: int):
        """
        Enables paging through a dictset by returning a page of records at a time.
        Parameters:
            dictset: iterable of dictionaries:
                The dictset to process
            page_size: integer:
                The number of records per page
        """
        index = -1
        chunk: list = [{}] * page_size
        for index, record in enumerate(dictset):
            _id = record.pop("_id", None)
            record["id"] = None if _id is None else str(_id)
            if index > 0 and index % page_size == 0:
                yield chunk
                chunk = [{}] * page_size
                chunk[0] = record
            else:
                chunk[index % page_size] = record
        yield chunk[: (index + 1) % page_size]

    def get_document_count(self, collection) -> int:  # pragma: no cover
        """
        Return the count, or an estimate of, the number of documents
        """
        raise NotImplementedError("get_document_list not implemented")

    def read_documents(self, collection, page_size: int = 500):  # pragma: no cover
        """
        Return a page of documents
        """
        raise NotImplementedError("read_document not implemented")
