"""
Implement a Relation, this analogous to a database table. It is called a Relation in
line with terminology for Relational Algreba and technically makes Mabel a Relational
Database although probably not charactistically.

Internally the data for the Relation is stored as a list of Tuples and a dictionary
records the schema and other key information about the Relation. Tuples are faster
than Dicts for accessing data.

Some naive benchmarking with 135k records (with 269 columns):
- dataset is 30% smaller
- Dedupe 90% faster
- Trivial selection 75% faster

We don't use numpy arrays for Relations, which documentation would have you believe
is orders of magnititude faster again because we are storing heterogenous data which
at the moment I can't think of a good way implement without hugh overheads to deal
with this limitation.


- self.data is a list of tuples
- self.header is a dictionary
    {
        "attribute": {
            "type": type (domain)
            "min": smallest value in this attribute
            "max": largest value in this attribute
            "count": count of non-null values in attribute
            "unique": number of unique values in this attribute
        }
    }

    Only "type" can be assumed to be present, especially after a select operation
"""
import sys
import os

from mabel.data.internals import data_containers

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from typing import Iterable, Tuple
from enum import Enum
from mabel.data.internals.attribute_domains import coerce
from mabel.data.internals.data_containers.base_container import BaseDataContainer
from mabel.data.internals.storage_classes import STORAGE_CLASS


class Relation(BaseDataContainer):

    __slots__ = ("header", "data", "name")

    def __init__(
        self,
        data: Iterable[Tuple] = [],
        *,
        header: dict = {},
        name: str = None,
        storage_class=STORAGE_CLASS.NO_PERSISTANCE,
    ):
        """
        Create a Relation.

        Parameters:
            data: Iterable
                An iterable which is the data in the Relation
            header: Dictionary (optional)
                Schema and profile information for this Relation
            name: String (optional)
                A handle for this Relation, cosmetic only
            persistance: STORAGE_CLASS (optional)
                How to store this dataset while we're processing it. The default is
                NO_PERSISTANCE which applies no specific persistance. MEMORY loads
                into a Python `list`, DISK saves to disk - disk persistance is slower
                but can handle much larger data sets. 'COMPRESSED_MEMORY' uses
                compression to fit more in memory for a performance cost.
        """
        self.data = data
        self.header = header
        self.name = name

    def apply_selection(self, predicate):
        """
        Apply a Selection operation to a Relation, this filters the data in the
        Relation to just the entries which match the predicate.

        Parameters:
            predicate (callable):
                A function which can be applied to a tuple to determine if it
                should be returned in the target Relation.

        Returns:

        """
        # selection invalidates what we thought we knew about counts etc
        new_header = {k: {"type": v.get("type")} for k, v in self.header.items()}
        return Relation(filter(predicate, self.data), header=new_header)

    def apply_projection(self, attributes):
        if not isinstance(attributes, (list, tuple)):
            attributes = [attributes]
        attribute_indices = []
        new_header = {k: v for k, v in self.header.items() if k in attributes}
        for index, attribute in enumerate(self.header.keys()):
            if attribute in attributes:
                attribute_indices.append(index - 1)

        def _inner_projection():
            for tup in self.data:
                yield tuple([tup[indice] for indice in attribute_indices])

        return Relation(_inner_projection(), header=new_header)

    def materialize(self):
        if not isinstance(self.data, list):
            self.data = list(self.data)
        return self.data

    def count(self):
        self.materialize()
        return len(self.data)

    def distinct(self):
        """
        Return a new Relation with only unique values
        """
        hash_list = {}

        def do_dedupe(data):
            for item in data:
                hashed_item = hash(item)
                if hashed_item not in hash_list:
                    yield item
                    hash_list[hashed_item] = True

        return Relation(do_dedupe(self.data), header=self.header)

    def from_dictset(self, dictset):
        """
        Load a Relation from a DictSet.

        In this case, the entire Relation is loaded into memory.
        """
        from mabel.data.internals.data_containers.base_container import (
            BaseDataContainer,
        )

        if not isinstance(dictset, BaseDataContainer):
            raise TypeError(f"Cannot convert from type `{type(dictset).name}`.")

        self.header = {k: {"type": v} for k, v in dictset.types().items()}
        self.data = [
            tuple([coerce(row.get(k)) for k in self.header.keys()])
            for row in dictset.icollect_list()
        ]

    def to_dictset(self):
        """
        Convert a Relation to a DictSet, this will fill every column so missing entries
        will be populated with Nones.
        """
        from mabel import DictSet

        def _inner_to_dictset():
            yield from [
                {k: row[i] for i, (k, v) in enumerate(self.header.items())}
                for row in self.data
            ]

        return DictSet(_inner_to_dictset())

    def attributes(self):
        return [k for k in self.header.keys()]

    def __str__(self):
        return f"{self.name or 'Relation'} ({', '.join([k + ':' + v.get('type') for k,v in self.header.items()])})"

    def __len__(self):
        """
        Alias for .count
        """
        return self.count()

    def serialize(self):
        from pyarrow.orc import orc
        import pyarrow.json
        import io

        buffer = b'{"row":1}\n{"row":2}\n{"row":3}'
        return pyarrow.json.read_json(io.BytesIO(buffer))

    def deserialize(self):
        from pyarrow.orc import orc
        import io

        buffer = b'ORC\x1d\x00\x00\n\x0c\n\x04\x00\x00\x00\x00\x12\x04\x08\x03P\x00,\x00\x00\xe3\x12\xe7bg\x80\x00!\x1e\x0ef!6\x0e&\x016\t\x9e\x00\x06\x00\x05\x00\x00\xff\xe0\x05\x00\x00\xff\xe0\t\x00\x00F\x02$`T\x00\x00\xe3b\xe3`\x13`\x90\x10\xe4\x02\xd1\x8c\x12\x92@\x9a\x01\xc8g\x05\xd3\x8c`\x9a\x11H\xb3\x0b\xb1\x80\xc4\x81$\x93\x00\x83\x14\xb3\xbbo\x08\x00-\x00\x00\n\x14\n\x04\x08\x03P\x00\n\x0c\x08\x03\x12\x06\x08\x02\x10\x06\x18\x0cP\x00\x8a\x00\x00\xe3`\x16\xc8\x90\xe2\xe2`\x16\xd0\x92\x10T\xd0\xd5`V\x12\xe0\xe0\x11bd\x94b.\xca/W`\xd0`0`P\xe2\xe0`\x81\xb0\x0c\x98\xadX8\x98\x03\x18\xacx8\x98\x85\xd88\x98\x04\xd8$x\x02\x18\x1c&\xf8y0\x02\x00\x08H\x10\x01\x18\x80\x80\x04"\x02\x00\x0c(\x190\x06\x82\xf4\x03\x03ORC\x17'

        data0 = orc.ORCFile(io.BytesIO(buffer))
        df0 = data0.read(columns=["row"])


if __name__ == "__main__":

    print(type(None))

    from mabel.data import STORAGE_CLASS, Reader
    from mabel.adapters.disk import DiskReader

    ds = Reader(
        inner_reader=DiskReader, dataset="tests/data/half", partitioning=()
    )  # , persistence=STORAGE_CLASS.MEMORY)
    ds = DictSet(ds.sample(0.1), storage_class=STORAGE_CLASS.MEMORY)

    import sys
    from types import ModuleType, FunctionType
    from gc import get_referents

    # Custom objects know their class.
    # Function objects seem to know way too much, including modules.
    # Exclude modules as well.
    BLACKLIST = type, ModuleType, FunctionType

    def getsize(obj):
        """sum size of object & members."""
        if isinstance(obj, BLACKLIST):
            raise TypeError(
                "getsize() does not take argument of type: " + str(type(obj))
            )
        seen_ids = set()
        size = 0
        objects = [obj]
        while objects:
            need_referents = []
            for obj in objects:
                if not isinstance(obj, BLACKLIST) and id(obj) not in seen_ids:
                    seen_ids.add(id(obj))
                    size += sys.getsizeof(obj)
                    need_referents.append(obj)
            objects = get_referents(*need_referents)
        return size

    rel = Relation()
    rel.from_dictset(ds)

    # print(getsize(rel))
    # print(getsize(ds))

    from mabel.utils.timer import Timer
    from mabel.data.internals.zone_map_writer import ZoneMapWriter

    with Timer("rel"):
        for i in range(10):
            r = rel.apply_selection(lambda x: x[0] > 100).count()
    #        r = rel.distinct().count()
    print(r)

    with Timer("ds"):
        for i in range(10):
            d = ds.select(lambda x: x["followers"] > 100).count()
    #        r = ds.distinct().count()
    print(d)

    # print(rel.apply_projection("description").to_dictset())

    print(rel.attributes(), len(rel.attributes()))

    with Timer("without mapper"):
        for i in [a for a in ds._iterator]:
            pass

    with Timer("with mapper"):
        zm = ZoneMapWriter()
        for i in [a for a in ds._iterator]:
            zm.add(i)

    from pprint import pprint

    pprint(list(zm.profile()))
