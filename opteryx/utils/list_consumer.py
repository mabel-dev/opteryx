class ListConsumer:

    __slots__ = ("position", "list_data")

    def __init__(self, list_data):
        self.position = 0
        self.list_data = list_data

    def get(self):
        return self.list_data[self.position]

    def peek(self):
        if not self.has_more():
            return None
        return self.list_data[self.position + 1]

    def next(self):
        self.position += 1

    def has_more(self):
        return (len(self.list_data) - self.position) > 0
