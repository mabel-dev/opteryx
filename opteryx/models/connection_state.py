# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import utils


class ConnectionState:
    """
    This object holds the properties relating to the connection / session.

    This is required for Mesos/MySQL connections.
    """

    def __init__(self):
        # We can't track connections across severless units so try to create one that'll
        # be unique for it's life
        self.connecton_id = utils.random_int()
        self.user = None
        self.database = None

        # session config values
        self.properties = {}
