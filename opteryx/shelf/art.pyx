import cython
import re
from string import punctuation


cdef:
    str GLOB = '*'
    object SENTINEL = object()

    inline int _get_position(int i, int n):
        if i == -1:
            return n
        return i

    inline int _findinstr(str source, str word, int start):
        return source.find(word, start)

    inline void _append_no_conflict_handlers_if_any(RadixTreeNode root, str method, list nc_handlers):
        if root.no_conflict_methods.__contains__(method):
            nc_handlers.extend(root.no_conflict_methods[method])


@cython.freelist(1024)
cdef class RadixTreeNode:
    cdef:
        public str path
        public dict methods
        public dict no_conflict_methods
        public list children
        public str indices
        public int indices_len
        public int path_len
        public dict optimized_index
        readonly bint index_zero_is_variable
        readonly bint index_zero_is_glob

    def __cinit__(self, str path=None, object handler=None, list methods=None, bint no_conflict=False):
        if path is None:
            self.path_len = 0
        else:
            self.path_len = len(path)
        self.path = path
        self.methods = dict()
        self.no_conflict_methods = dict()
        self.children = list()
        self.indices = ""
        self.indices_len = 0
        self.optimized_index = dict()
        self.index_zero_is_variable = 0
        self.index_zero_is_glob = 0

        if handler is not None and methods is not None:
            self.add_methods(methods, handler, no_conflict)

    def __repr__(self):
        return (
            "<RadixTreeNode path: {}, methods: {}, indices: \"{}\", children: "
            "{}>".format(
                "\"{}\"".format(self.path) if self.path is not None else "None",
                self.methods,
                self.indices,
                self.children
            )
        )

    def add_methods(self, methods, handler, no_conflict=False):
        if not methods:
            return

        if no_conflict:
            for method in methods:
                if method in self.no_conflict_methods and handler in self.no_conflict_methods[method]:
                    continue

                if method in self.no_conflict_methods:
                    self.no_conflict_methods[method].append(handler)
                else:
                    self.no_conflict_methods[method] = [handler]
        else:
            for method in methods:
                if method in self.methods and self.methods[method] != handler:
                    raise KeyError(
                        "{} conflicts with existing handler "
                        "{}".format(handler, self.methods[method])
                    )

                self.methods[method] = handler

    cpdef RadixTreeNode insert_child(self, str index, RadixTreeNode child):
        return self._c_insert_child(index, child)

    cdef RadixTreeNode _c_insert_child(self, str index, RadixTreeNode child):
        cdef int pos = self.get_index_position(index)
        self.indices = self.indices[:pos] + index + self.indices[pos:]
        self.indices_len = len(self.indices)
        self.children.insert(pos, child)
        return child

    cdef int get_index_position(self, str index):
        return _get_position(self.indices.find(index), 0)

    cdef RadixTreeNode get_child(self, str index):
        cdef int i = self.get_index_position(index)
        if i < self.indices_len and self.indices[i] == index:
            return self.children[i]

    cdef optimize(self, RadixTree tree):
        cdef:
            str index
            RadixTreeNode child

        self.optimized_index.clear()  # if not already clean

        for index in self.indices:
            self.optimized_index[index] = self.get_child(index)

        if self.indices_len > 0:
            self.index_zero_is_variable = self.indices[0] == tree.VARIABLE
            self.index_zero_is_glob = self.indices[0] == GLOB
        else:
            self.index_zero_is_variable = 0
            self.index_zero_is_glob = 0

        for child in self.children:
            child.optimize(tree)

    cdef inline RadixTreeNode get_child_optimized(self, str index):
        return self.optimized_index.get(index)


cdef class RadixTree:
    cdef:
        public RadixTreeNode root
        str _VARIABLE
        str _SEPARATOR
        object _VAR_PATTERN

    def __cinit__(self, str variable=None, str separator=None):
        self.root = RadixTreeNode()
        self.VARIABLE = variable or ':'
        self.SEPARATOR = separator or '/'
        self._VAR_PATTERN = re.compile(r"(?:[{}\*])([a-zA-Z0-9\_]+)".format(re.escape(self._VARIABLE)))

    def __repr__(self):
        return repr(self.root)

    @property
    def config(self):
        return {
            'variable': self.VARIABLE,
            'separator': self.SEPARATOR
        }

    @property
    def SEPARATOR(self):
        return self._SEPARATOR

    @SEPARATOR.setter
    def SEPARATOR(self, str value):
        if self.root.children:
            raise ValueError("You can't change the separator character after routes have been inserted")
        if value is not None and value in punctuation and len(value) == 1:
            if value == self._VARIABLE:
                raise ValueError("The separator character must not equal the variable character")
            self._SEPARATOR = value
        else:
            raise ValueError("The separator character must be of length one and a valid punctuation")

    @property
    def VARIABLE(self):
        return self._VARIABLE

    @VARIABLE.setter
    def VARIABLE(self, str value):
        if self.root.children:
            raise ValueError("You can't change the variable character after routes have been inserted")
        if value is not None and value in punctuation and len(value) == 1:
            if value == self._SEPARATOR:
                raise ValueError("The variable character must not equal the separator character")
            self._VARIABLE = value
        else:
            raise ValueError("The variable character must be of length one and a valid punctuation")

    @property
    def sentinel(self):
        return SENTINEL

    def insert(self, str path, object handler, list methods, bint no_conflict=False):
        if path is None or len(path.strip()) == 0 or path.strip()[0] != self._SEPARATOR:
            raise ValueError("path cannot be None, empty or invalid")

        parts = path.strip(self.SEPARATOR).split(self.SEPARATOR)
        matches = [self._VAR_PATTERN.match(p) for p in parts]
        all_matches = [m.group(1) for m in matches if m is not None]

        if len(all_matches) != len(set(all_matches)):
            raise ValueError('"{}" has at least one duplicate variable name'.format(path))

        i, node = self._c_insert(path, handler, methods, no_conflict)

        if node is not None:
            conflict = [path[:i] + p for p in self.traverse(node)]
            raise ValueError('"{}" conflicts with {}'.format(path, conflict))

        self.root.optimize(self)

    cdef list traverse(self, RadixTreeNode root):
        cdef:
            list r = []
            RadixTreeNode child

        if len(root.indices) != 0:
            for i, c in enumerate(root.indices):
                child = root.children[i]
                path = "{}{}".format(c if c in [self._VARIABLE, GLOB] else "", child.path)

                if child.methods and child.indices:
                    r.append(path)

                r.extend([path + p for p in self.traverse(child) or [""]])

        return r

    cdef tuple _c_insert(self, str path, object handler, list methods, bint no_conflict):
        cdef:
            int i = 0
            int n = len(path)
            int code = 0
            int j, p, m
            RadixTreeNode root, child

        root = self.root

        while i < n:
            if not no_conflict:
                if len(root.indices) != 0 and (root.indices[0] == GLOB or
                    path[i] == GLOB and len(root.indices) != 0 or
                    path[i] != self._VARIABLE and root.indices[0] == self._VARIABLE or
                    path[i] == self._VARIABLE and root.indices[0] != self._VARIABLE or
                    path[i] == self._VARIABLE and root.indices[0] == self._VARIABLE and
                    path[i + 1: _get_position(_findinstr(path, self._SEPARATOR, i), n)] != root.children[0].path):

                    return i, root

            child = root.get_child(path[i])

            if child is None:
                p = _get_position(path.find(self._VARIABLE, i), n)
                if p == n:
                    p = _get_position(path.find(GLOB, i), n)
                    if p == n:
                        root.insert_child(path[i], RadixTreeNode(path[i:], handler, methods, no_conflict))
                        return code, None

                    root = root.insert_child(path[i], RadixTreeNode(path[i:p]))
                    root.insert_child(GLOB, RadixTreeNode(path[p + 1:], handler, methods, no_conflict))
                    return code, None

                root = root.insert_child(path[i], RadixTreeNode(path[i:p]))
                i = _get_position(path.find(self._SEPARATOR, p), n)
                root = root.insert_child(self._VARIABLE, RadixTreeNode(path[p + 1: i]))

                if i == n:
                    root.add_methods(methods, handler, no_conflict)
            else:
                root = child
                if path[i] == self._VARIABLE:
                    i += len(root.path) + 1
                    if i == n:
                        root.add_methods(methods, handler, no_conflict)
                else:
                    j = 0
                    m = len(root.path)

                    while i < n and j < m and path[i] == root.path[j]:
                        i += 1
                        j += 1

                    if j < m:
                        child = RadixTreeNode(root.path[j:])
                        child.methods = root.methods
                        child.no_conflict_methods = root.no_conflict_methods
                        child.children = root.children
                        child.indices = root.indices
                        child.indices_len = len(child.indices)

                        root.path = root.path[:j]
                        root.path_len = len(root.path)
                        root.methods = {}
                        root.no_conflict_methods = {}
                        root.children = [child]
                        root.indices = child.path[0]
                        root.indices_len = len(root.indices)

                    if i == n:
                        root.add_methods(methods, handler, no_conflict)

        return code, None

    cpdef tuple get(self, str path, str method):
        cdef dict params = {}

        handler, nc_handlers = self._c_get(path, method, params)
        if handler is None:
            return None, [], {}
        elif handler == SENTINEL:
            return SENTINEL, [], {}
        return handler, nc_handlers, params

    cdef tuple _c_get(self, str path, str method, dict params):
        cdef:
            int i = 0
            int n = len(path)
            int pos
            object handler = None
            list nc_handlers = []
            RadixTreeNode root

        root = self.root

        while i < n:
            if root.indices_len == 0:
                return None, None

            _append_no_conflict_handlers_if_any(root, method, nc_handlers)

            if root.index_zero_is_variable:
                root = root.children[0]
                pos = _get_position(_findinstr(path, self._SEPARATOR, i), n)
                params[root.path] = path[i:pos]
                i = pos

            elif root.index_zero_is_glob:
                root = root.children[0]
                params[root.path] = path[i:]
                break
            else:
                root = root.get_child_optimized(path[i])

                if root is None:
                    return None, None

                pos = i + root.path_len

                if path[i:pos] != root.path:
                    return None, None

                i = pos

        handler = root.methods.get(method)

        if handler is None:
            if root.methods:
                return SENTINEL, None
            return None, None

        _append_no_conflict_handlers_if_any(root, method, nc_handlers)

        return root.methods.get(method), nc_handlers

    cpdef set methods_for(self, str path):
        methods = self._c_methods_for(path)
        if methods is None:
            return set()
        return methods

    cdef set _c_methods_for(self, str path):
        cdef:
            int i = 0
            int n = len(path)
            int pos
            RadixTreeNode root

        root = self.root

        while i < n:
            if root.indices_len == 0:
                return None

            if root.index_zero_is_variable:
                root = root.children[0]
                pos = _get_position(_findinstr(path, self._SEPARATOR, i), n)
                i = pos

            elif root.index_zero_is_glob:
                root = root.children[0]
                break
            else:
                root = root.get_child_optimized(path[i])

                if root is None:
                    return None

                pos = i + root.path_len

                if path[i:pos] != root.path:
                    return None

                i = pos

        return set(root.methods.keys())

