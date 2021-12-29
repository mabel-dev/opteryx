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
            "nullable": are there nulls
        }
    }

    Only "type" can be assumed to be present, especially after a select operation
"""
import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from typing import Iterable, Tuple
from enum import Enum
from opteryx.internals.attribute_domains import opteryx_TYPES, PARQUET_TYPES, coerce
from opteryx.exceptions import MissingDependencyError


class Relation():

    __slots__ = ("header", "data", "name")

    def __init__(
        self,
        data: Iterable[Tuple] = [],
        *,
        header: dict = {},
        name: str = None
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

        # we may get passed a generator, we're going to make it a list
        if not isinstance(self.data, list):
            self.data = list(self.data)

    def apply_selection(self, predicate):
        """
        Apply a Selection operation to a Relation, this filters the data in the
        Relation to just the entries which match the predicate.

        Parameters:
            predicate (list or tuple):
                A DNF structured filter predicate.

        Returns:
            Relation
        """
        # selection invalidates what we thought we knew about counts etc
        new_header = {k: {"type": v.get("type")} for k, v in self.header.items()}
        return Relation(filter(predicate, self.data), header=new_header)

    def apply_projection(self, attributes):
        if not isinstance(attributes, (list, tuple)):
            attributes = [attributes]
        attribute_indices = []
        new_header = {k: self.header.get(k) for k in attributes}
        for index, attribute in enumerate(self.header.keys()):
            if attribute in attributes:
                attribute_indices.append(index)

        def _inner_projection():
            for tup in self.data:
                yield tuple([tup[indice] for indice in attribute_indices])

        return Relation(_inner_projection(), header=new_header)

    def count(self):
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

        return DictSet(self.fetchall())

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
        """
        Serialize to Parquet, this is used for communicating and saving the Relation.
        """
        try:
            import pyarrow.parquet as pq
            import pyarrow.json
        except ImportError:  # pragma: no cover
            raise MissingDependencyError(
                "`pyarrow` is missing, please install or include in requirements.txt"
            )

        import io
        
        # load into pyarrow as a JSON dataset
        in_pyarrow_buffer = pyarrow.json.read_json(io.BytesIO(self.to_json()))

        # then we convert the pyarrow table to parquet
        pq_temp_buffer = io.BytesIO()
        pq.write_table(in_pyarrow_buffer, pq_temp_buffer, compression="ZSTD")

        # the read out the parquet formatted data
        pq_temp_buffer.seek(0, 0)
        buffer = pq_temp_buffer.read()
        pq_temp_buffer.close()

        return buffer

    def to_json(self):
        """
        Convert the relation to a JSONL byte string
        """
        import orjson
        return b'\n'.join(map(orjson.dumps, self.fetchall()))

    def fetchone(self, offset:int=0):
        try:
            return dict(zip(self.header.keys(), self.data[offset]))
        except IndexError:
            return None

    def fetchmany(self, size:int=100, offset:int=0):
        keys = self.header.keys()
        
        def _inner_fetch():
            for index in range(offset, min(len(self.data), offset+size)):
                yield dict(zip(keys, self.data[index]))

        return list(_inner_fetch())

    def fetchall(self, offset:int=0):
        keys = self.header.keys()
        
        def _inner_fetch():
            for index in range(offset, len(self.data)):
                yield dict(zip(keys, self.data[index]))

        return list(_inner_fetch())

    @staticmethod
    def deserialize(stream):
        """
        Deserialize from a stream of bytes which is a Parquet file.

        Return a Relation with a basic schema.
        """
        try:
            import pyarrow.parquet as pq  # type:ignore
        except ImportError:  # pragma: no cover
            raise MissingDependencyError(
                "`pyarrow` is missing, please install or include in requirements.txt"
            )
        if not hasattr(stream, "read") or isinstance(stream, bytes):
            import io
            stream = io.BytesIO(stream)
        table = pq.read_table(stream)

        def _inner(table):
            for batch in table.to_batches():
                dict_batch = batch.to_pydict()
                for index in range(len(batch)):
                    yield tuple([v[index] for k, v in dict_batch.items()])

        def pq_schema_to_rel_schema(pq_schema):
            schema = {}
            for attribute in pq_schema:
                schema[attribute.name] = { "type": PARQUET_TYPES.get(str(attribute.type), opteryx_TYPES.OTHER).value }
            return schema

        return Relation(_inner(table), header=pq_schema_to_rel_schema(table.schema))

    @staticmethod
    def load(file):
        with open(file, 'rb') as pq_file:
            return Relation.deserialize(pq_file)



if __name__ == "__main__":

    r = Relation.load('tests/data/parquet/tweets.parquet')

    print(r.count())
    print(r.attributes())
    r.distinct()
    print(r.attributes())
    print(r.apply_projection(["user_verified"]).distinct().count())
    print(r.fetchone(1000))
    print(r.apply_projection(["user_verified"]).distinct().fetchmany())
    print(len(r.fetchall()))

    print(len(r.to_json()))

    print(len(r.serialize()))

    s = Relation.deserialize(r.serialize())
    print(s.count())