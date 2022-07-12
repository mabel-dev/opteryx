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
    def get_document_list(self, collection) -> Iterable:
        """
        Return an interable of blobs/files
        """
        raise NotImplementedError("get_document_list not implemented")

    def read_document(self, collection, document) -> bytes:
        """
        Return a filelike object
        """
        raise NotImplementedError("read_document not implemented")
