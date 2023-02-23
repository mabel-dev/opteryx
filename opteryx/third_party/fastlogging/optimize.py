# mypy: disable-error-code=attr-defined
import ast
import importlib.util
import inspect
import marshal

# type:ignore
import os
import struct
import time

# noinspection PyUnresolvedReferences
from types import ModuleType

from .fastlogging import DEBUG
from .fastlogging import ERROR
from .fastlogging import FATAL
from .fastlogging import INFO
from .fastlogging import WARNING

optimized = set()


class OptimizeAst(ast.NodeTransformer):  # pragma: no cover
    def __init__(
        self,
        id_,
        optimize,
        deoptimize=0,
        remove=0,
        const2value=False,
        value2const=False,
    ):
        self.id = id_
        self.__const2value = const2value
        self.__value2const = value2const
        self.__level2func = {
            "DEBUG": "debug",
            "INFO": "info",
            "WARNING": "warning",
            "ERROR": "error",
            "FATAL": "fatal",
        }
        self.__level2value = {
            "DEBUG": DEBUG,
            "INFO": INFO,
            "WARNING": WARNING,
            "ERROR": ERROR,
            "FATAL": FATAL,
        }
        self.__value2level = {
            DEBUG: "DEBUG",
            INFO: "INFO",
            WARNING: "WARNING",
            ERROR: "ERROR",
            FATAL: "FATAL",
        }
        self.__level2optimize = self.__level2dict(optimize)
        self.__level2unoptimize = self.__level2dict(deoptimize)
        self.__remove = set()
        self.__removeLevel = set()
        if remove >= DEBUG:
            self.__remove.add("debug")
            self.__removeLevel.add("DEBUG")
        if remove >= INFO:
            self.__remove.add("info")
            self.__removeLevel.add("INFO")
        if remove >= WARNING:
            self.__remove.add("warning")
            self.__removeLevel.add("WARNING")
        if remove >= ERROR:
            self.__remove.add("error")
            self.__removeLevel.add("ERROR")
        if remove >= FATAL:
            self.__remove.add("fatal")
            self.__remove.add("critical")
            self.__removeLevel.add("FATAL")

    # noinspection PyMethodMayBeStatic
    def __level2dict(self, level):
        level2dict = {}
        if level >= FATAL:
            level2dict["fatal"] = "FATAL"
            level2dict["critical"] = "FATAL"
        if level >= ERROR:
            level2dict["error"] = "ERROR"
        if level >= WARNING:
            level2dict["warning"] = "WARNING"
        if level >= INFO:
            level2dict["info"] = "INFO"
        if level >= DEBUG:
            level2dict["debug"] = "DEBUG"
        return level2dict

    def __compare_args(self, levelName):
        if self.__const2value:
            levelNum = self.__level2value[levelName]
            return ast.Num(n=levelNum), ast.Num(n=levelNum)
        return ast.Name(id=levelName, ctx=ast.Load()), ast.Name(id=levelName, ctx=ast.Load())

    def __levelName_compare_args(self, args):
        # noinspection PyBroadException
        try:
            levelName = args.id  # e.g. INFO
            compare, args = self.__compare_args(levelName)
        except:
            levelNum = args.n
            levelName = self.__value2level[levelNum]
            if self.__value2const:
                compare = ast.Name(id=levelName, ctx=ast.Load())
                args = ast.Name(id=levelName, ctx=ast.Load())
            else:
                compare = ast.Num(n=levelNum)
                args = ast.Num(n=levelNum)
        return levelName, compare, args

    def __compare(self, compare):
        return ast.Compare(
            left=ast.Attribute(
                value=ast.Name(id=self.id, ctx=ast.Load()), attr="level", ctx=ast.Load()
            ),
            ops=[ast.LtE()],
            comparators=[compare],
        )

    def __expr(self, attr, args, body):
        return ast.Expr(
            value=ast.Call(
                func=ast.Attribute(
                    value=ast.Name(id=self.id, ctx=ast.Load()),
                    attr=attr,
                    ctx=ast.Load(),
                ),
                args=args,
                keywords=body.keywords,
                starargs=body.starargs if hasattr(body, "starargs") else None,
                kwargs=body.kwargs if hasattr(body, "kwargs") else None,
            )
        )

    def visit_If(self, node):
        def visit_children(node_If):
            children = [self.visit(child) for child in ast.iter_child_nodes(node_If)]
            node_If.body = [child for child in children[1:] if child is not None]
            return node_If

        # noinspection PyBroadException
        try:
            if (node.test.left.value.id != self.id) or (node.test.left.attr != "level"):
                return visit_children(node)
            if node.body[0].value.func.value.id != self.id:
                return visit_children(node)
        except:
            return visit_children(node)
        body = node.body[0].value
        attr = body.func.attr
        if attr in self.__remove:
            return None
        levelName, compare, args = self.__levelName_compare_args(node.test.comparators[0])
        if levelName in self.__removeLevel:
            return None
        if attr == "log":
            # if logger.level <= LOG_foo:
            #     logger.log(LOG_foo, ...)
            body_args = body.args[1:]
            level = self.__level2func[levelName]  # e.g. ERROR -> error
            if level not in self.__level2optimize:
                if level in self.__level2unoptimize:
                    # Deoptimize -> remove 'if' and 'log' -> 'foo'
                    return visit_children(
                        ast.copy_location(self.__expr(level, body_args, body), node)
                    )
            # Optimize
            args = [args] + body_args
        else:
            # if logger.level <= LOG_foo:
            #     logger.foo(...)
            if attr not in self.__level2optimize:
                if attr in self.__level2unoptimize:
                    # Deoptimize
                    return visit_children(
                        ast.copy_location(self.__expr(attr, body.args, body), node)
                    )
                args = body.args
            else:
                attr = "log"
                args = [args] + body.args
        # Optimize
        return visit_children(
            ast.copy_location(
                ast.If(
                    test=self.__compare(compare),
                    body=[self.__expr(attr, args, body)],
                    orelse=[],
                ),
                node,
            )
        )

    def visit_Expr(self, node):
        func = node.value.func
        # noinspection PyBroadException
        try:
            if func.value.id != self.id:
                return node
        except:
            return node
        attr = func.attr
        if attr in self.__remove:
            return None
        value = node.value
        if attr == "log":
            # logger.log(LOG_foo, ...)
            levelName, compare, args = self.__levelName_compare_args(value.args[0])
            if levelName in self.__removeLevel:
                return None
            value_args = value.args[1:]
            level = self.__level2func[levelName]  # e.g. ERROR -> error
            if level not in self.__level2optimize:
                if level in self.__level2unoptimize:
                    # Deoptimize
                    return ast.copy_location(
                        self.__expr(self.__level2func[levelName], value_args, value),
                        node,
                    )
                return ast.copy_location(self.__expr(attr, [args] + value_args, value), node)
            # Optimize
            args = [args] + value_args
        else:
            # logger.foo(...)
            levelName = self.__level2optimize.get(attr)
            if levelName is None:
                # Deoptimize or unchanged
                return node
            # Optimize
            compare, args = self.__compare_args(levelName)
            attr = "log"
            args = [args] + value.args
        # Optimize
        return ast.copy_location(
            ast.If(
                test=self.__compare(compare),
                body=[self.__expr(attr, args, value)],
                orelse=[],
            ),
            node,
        )


def OptimizeObj(
    glob,
    obj,
    id_,
    optimize=0,
    deoptimize=0,
    remove=0,
    const2value=False,
    value2const=False,
):  # pragma: no cover
    tree = ast.parse(inspect.getsource(obj))
    tree = OptimizeAst(id_, optimize, deoptimize, remove, const2value, value2const).visit(tree)
    if inspect.ismodule(obj):
        return compile(ast.fix_missing_locations(tree), filename=obj.__file__, mode="exec")
    module = ModuleType("tempModule")
    module.__dict__.update(glob)
    exec(
        compile(ast.fix_missing_locations(tree), filename=obj.__name__, mode="exec"),
        module.__dict__,
    )
    return getattr(module, obj.__name__)


def OptimizeModule(
    obj, id_, optimize=0, deoptimize=0, remove=0, const2value=False, value2const=False
):  # pragma: no cover
    tree = ast.parse(inspect.getsource(obj))
    tree = OptimizeAst(id_, optimize, deoptimize, remove, const2value, value2const).visit(tree)
    if inspect.ismodule(obj):
        fileName = obj.__file__
    else:
        fileName = obj.__name__
    return compile(ast.fix_missing_locations(tree), filename=fileName, mode="exec")


def OptimizeFile(
    glob,
    fileName,
    id_,
    optimize=0,
    deoptimize=0,
    remove=0,
    const2value=False,
    value2const=False,
):  # pragma: no cover
    tree = ast.parse(open(fileName).read())
    tree = OptimizeAst(id_, optimize, deoptimize, remove, const2value, value2const).visit(tree)
    module = ModuleType("tempModule")
    module.__dict__.update(glob)
    exec(
        compile(ast.fix_missing_locations(tree), filename=fileName, mode="exec"),
        module.__dict__,
    )
    return module


def Optimize(
    glob, id_, optimize=0, deoptimize=0, remove=0, const2value=False, value2const=False
):  # pragma: no cover
    def OptimizeDecorator(obj):
        if obj.__name__ in optimized:
            return obj
        optimized.add(obj.__name__)
        return OptimizeObj(glob, obj, id_, optimize, deoptimize, remove, const2value, value2const)

    return OptimizeDecorator


def WritePycFile(fileName, code):  # pragma: no cover
    with open(os.path.splitext(fileName)[0] + ".pyc", "wb") as F:
        F.write(b"\0\0\0\0")
        F.write(struct.pack("L", int(time.time())))
        marshal.dump(code, F)
        F.flush()
        F.seek(0, 0)
        F.write(importlib.util.MAGIC_NUMBER)
