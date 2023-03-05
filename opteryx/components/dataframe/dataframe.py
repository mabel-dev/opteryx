import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))


class Dataframe:
    __slots__ = ("_schema", "_tuples")

    def __init__(self, schema, data):
        self._schema = schema
        self._tuples = data

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
        new_header = {k: {"type": v.get("type")} for k, v in self._schema.items()}
        return Dataframe(filter(predicate, self._tuples), new_header)

    def apply_projection(self, attributes):
        if not isinstance(attributes, (list, tuple)):
            attributes = [attributes]
        attribute_indices = []
        new_header = {k: v for k, v in self._schema.items() if k in attributes}
        for index, attribute in enumerate(self._schema.keys()):
            if attribute in attributes:
                attribute_indices.append(index)

        def _inner_projection():
            for tup in self._tuples:
                yield tuple([tup[indice] for indice in attribute_indices])

        return Dataframe(_inner_projection(), new_header)

    def materialize(self):
        if not isinstance(self._tuples, list):
            self._tuples = list(self._tuples)

    def __len__(self):
        self.materialize()
        return len(self._tuples)

    def distinct(self):
        hash_list = {}

        def do_dedupe(data):
            for item in data:
                hashed_item = hash(item)
                if hashed_item not in hash_list:
                    yield item
                    hash_list[hashed_item] = True

        return Dataframe(do_dedupe(self._tuples), self._schema)

    def collect(self, columns):
        single = False
        if not isinstance(columns, (list, set, tuple)):
            single = True
            columns = [columns]

        # Initialize empty lists for each column
        result = tuple([] for _ in columns)

        # Extract the specified columns into the result lists
        for row in self._tuples:
            for j, column in enumerate(columns):
                result[j].append(row[column])

        if single:
            return result[0]
        return result

    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)

    def row(self, i):
        self.materialize()
        return self._tuples[i]

    @property
    def keys(self):
        return tuple(self._schema.keys())

    @classmethod
    def from_arrow(cls, table) -> tuple:
        schema = table.schema

        fields = {
            str(field.name): {"type": field.type.to_pandas_dtype(), "nullable": field.nullable}
            for field in schema
        }
        columns = [table.column(i) for i in schema.names]

        # Create a list of tuples from the columns
        tuples = (tuple(col[i].as_py() for col in columns) for i in range(table.num_rows))

        return cls(fields, tuples)

    def __str__(self):
        from display import ascii_table

        return ascii_table(self)

    def slice(self, offset: int = 0, length: int = None):
        self.materialize()
        if offset < 0:
            offset = len(self._tuples) + offset
        if length is None:
            return Dataframe(self._schema, self._tuples[offset:])
        return Dataframe(self._schema, self._tuples[offset : offset + length])


if __name__ == "__main__":  # pragma: no cover
    import opteryx

    planets = opteryx.query("SELECT * FROM $planets").arrow()
    df = Dataframe.from_arrow(planets)
    df.materialize()
    print(df)
