"""
Projection Node

This is a SQL Query Execution Plan Node.

This Node eliminates columns that are not needed in a Relation. This is also the Node
that performs column renames.
"""

class ProjectionNode():

    def __init__(self, **kwargs):
        pass

    def execute(self, relation=None):

        # first we rename the columns

        # then we perform any renames
        pass
