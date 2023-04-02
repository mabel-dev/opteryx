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
