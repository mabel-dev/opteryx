from .connection import Connection

def connect(*args, **kwargs):
    return Connection(*args, **kwargs)

from .internals.relation import Relation
