import functools
import textwrap
from collections import defaultdict
from typing import NamedTuple


class Query:
    keywords = [
        "WITH",
        "SELECT",
        "FROM",
        "WHERE",
        "GROUP BY",
        "HAVING",
        "ORDER BY",
        "LIMIT",
    ]

    separators = dict(WHERE="AND", HAVING="AND")
    default_separator = ","

    formats: tuple = (
        defaultdict(lambda: "{value}"),
        defaultdict(lambda: "{value} AS {alias}", WITH="{alias} AS {value}"),
    )

    subquery_keywords = {"WITH"}
    fake_keywords = dict(JOIN="FROM")
    flag_keywords = dict(SELECT={"DISTINCT", "ALL"})

    def __init__(self, data=None, separators=None):
        self.data = {}
        if data is None:
            data = dict.fromkeys(self.keywords, ())
        for keyword, args in data.items():
            self.data[keyword] = _FlagList()
            self.add(keyword, *args)

        if separators is not None:
            self.separators = separators

    def add(self, keyword, *args):
        keyword, fake_keyword = self._resolve_fakes(keyword)
        keyword, flag = self._resolve_flags(keyword)
        target = self.data[keyword]

        if flag:
            if target.flag:  # pragma: no cover
                raise ValueError(f"{keyword} already has flag: {flag!r}")
            target.flag = flag

        kwargs: dict = {}
        if fake_keyword:
            kwargs.update(keyword=fake_keyword)
        if keyword in self.subquery_keywords:
            kwargs.update(is_subquery=True)

        for arg in args:
            target.append(_Thing.from_arg(arg, **kwargs))

        return self

    def _resolve_fakes(self, keyword):
        for part, real in self.fake_keywords.items():
            if part in keyword:
                return real, keyword
        return keyword, ""

    def _resolve_flags(self, keyword):
        prefix, _, flag = keyword.partition(" ")
        if prefix in self.flag_keywords:
            if flag and flag not in self.flag_keywords[prefix]:  # pragma: no cover
                raise ValueError(f"invalid flag for {prefix}: {flag!r}")
            return prefix, flag
        return keyword, ""

    def __getattr__(self, name):
        if not name.isupper():
            return getattr(super(), name)
        return functools.partial(self.add, name.replace("_", " "))

    def __str__(self):
        return "".join(self._lines())

    def _lines(self):
        for keyword, things in self.data.items():
            if not things:
                continue

            if things.flag:
                yield f"{keyword} {things.flag}\n"
            else:
                yield f"{keyword}\n"

            grouped: tuple = [], []
            for thing in things:
                grouped[bool(thing.keyword)].append(thing)
            for group in grouped:
                yield from self._lines_keyword(keyword, group)

    def _lines_keyword(self, keyword, things):
        for i, thing in enumerate(things, 1):
            last = i == len(things)

            if thing.keyword:
                yield thing.keyword + "\n"

            format_ = self.formats[bool(thing.alias)][keyword]
            value = thing.value
            if thing.is_subquery:
                value = f"(\n{self._indent(value)}\n)"
            yield self._indent(format_.format(value=value, alias=thing.alias))

            if not last and not thing.keyword:
                try:
                    yield " " + self.separators[keyword]
                except KeyError:
                    yield self.default_separator

            yield "\n"

    _indent = functools.partial(textwrap.indent, prefix="    ")


class _Thing(NamedTuple):
    value: str
    alias: str = ""
    keyword: str = ""
    is_subquery: bool = False

    @classmethod
    def from_arg(cls, arg, **kwargs):
        if isinstance(arg, str):
            alias, value = "", arg
        elif len(arg) == 2:
            alias, value = arg
        else:  # pragma: no cover
            raise ValueError(f"invalid arg: {arg!r}")
        return cls(_clean_up(value), _clean_up(alias), **kwargs)


class _FlagList(list):
    flag: str = ""


def _clean_up(thing: str) -> str:
    return textwrap.dedent(thing.rstrip()).strip()
