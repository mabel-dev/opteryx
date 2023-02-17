import struct


class BTreeNode:
    def __init__(self, leaf=False):
        self.keys = []
        self.children = []
        self.leaf = leaf

    def add_key(self, key):
        self.keys.append(key)
        self.keys.sort()

    def add_child(self, node):
        self.children.append(node)


class BTree:
    def __init__(self, t):
        self.t = t
        self.root = BTreeNode(leaf=True)

    def search(self, node, key):
        i = 0
        while i < len(node.keys) and key > node.keys[i]:
            i += 1

        if i < len(node.keys) and key == node.keys[i]:
            return True

        if node.leaf:
            return False

        return self.search(node.children[i], key)

    def insert(self, key):
        node = self.root

        if len(node.keys) == (2 * self.t) - 1:
            new_node = BTreeNode()
            self.root = new_node
            new_node.children.append(node)
            self.split_child(new_node, 0)
            self.insert_non_full(new_node, key)
        else:
            self.insert_non_full(node, key)

    def insert_non_full(self, node, key):
        i = len(node.keys) - 1

        if node.leaf:
            node.add_key(key)
        else:
            while i >= 0 and key < node.keys[i]:
                i -= 1

            if len(node.children[i + 1].keys) == (2 * self.t) - 1:
                self.split_child(node, i + 1)
                if key > node.keys[i + 1]:
                    i += 1

            self.insert_non_full(node.children[i + 1], key)

    def split_child(self, node, i):
        t = self.t
        child = node.children[i]
        new_node = BTreeNode(leaf=child.leaf)
        node.add_child(new_node)
        node.add_key(child.keys[t - 1])
        new_node.keys = child.keys[t : (2 * t - 1)]
        child.keys = child.keys[0 : (t - 1)]

        if not child.leaf:
            new_node.children = child.children[t : (2 * t)]
            child.children = child.children[0 : (t - 1)]

    def traverse(self, node):
        i = 0
        keys = node.keys
        result = []
        for child in node.children:
            result.extend(self.traverse(child))
            result.append(keys[i])
            i += 1

        # result.extend(self.traverse(node.children))
        return result

    def save_to_storage(self, filename):
        with open(filename, "wb") as file:
            for key, value in self.items():
                key_bytes = key.encode("utf-8")
                value_bytes = struct.pack("f", value)
                file.write(struct.pack("I", len(key_bytes)))
                file.write(key_bytes)
                file.write(value_bytes)

    def load_from_storage(self, filename):
        with open(filename, "rb") as file:
            data = file.read()
            i = 0
            while i < len(data):
                key_len = struct.unpack("I", data[i : i + 4])[0]
                i += 4
                key = data[i : i + key_len].decode("utf-8")
                i += key_len
                value = struct.unpack("f", data[i : i + 4])[0]
                i += 4
                print(key, value)
                self.insert(value)

    def print_tree(self):
        result = self.traverse(self.root)
        print(result)
