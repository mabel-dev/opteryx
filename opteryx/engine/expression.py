# no-maintain-checks
# cython: language_level=3
"""
This module is compiled, any changes to it need the following to be run before they
will be effective:

python setup.py build_ext --inplace


This implements a SQL-like query syntax to filter dictionaries based on combinations
of predicates.

The implementation is sort of an expression tree, it doesn't need to be a complete
expression tree as it's only doing boolean logic.

Derived from: https://gist.github.com/leehsueh/1290686
"""
from ..readers.internals.inline_evaluator import *
from ...utils.dates import parse_iso
from ...utils.token_labeler import Tokenizer, TOKENS, OPERATORS


class InvalidExpression(BaseException):
    pass


class TreeNode:
    __slots__ = ("token_type", "value", "left", "right", "parameters")

    def __init__(self, token_type, value):
        self.token_type = token_type
        self.value = value
        self.left = None
        self.right = None
        self.parameters = []


class Expression(object):
    tokenizer = None
    root = None

    def __init__(self, exp):
        self.tokenizer = Tokenizer(exp)
        self.parse()

    def parse(self):
        self.root = self.parse_expression()

    def parse_expression(self):
        andTerm1 = self.parse_and_term()
        while (
            self.tokenizer.has_next() and self.tokenizer.next_token_type() == TOKENS.OR
        ):
            self.tokenizer.next()
            andTermX = self.parse_and_term()
            andTerm = TreeNode(TOKENS.OR, None)
            andTerm.left = andTerm1
            andTerm.right = andTermX
            andTerm1 = andTerm
        return andTerm1

    def parse_and_term(self):
        condition1 = self.parse_condition()
        while (
            self.tokenizer.has_next() and self.tokenizer.next_token_type() == TOKENS.AND
        ):
            self.tokenizer.next()
            conditionX = self.parse_condition()
            condition = TreeNode(TOKENS.AND, None)
            condition.left = condition1
            condition.right = conditionX
            condition1 = condition
        return condition1

    def parse_condition(self):

        terminal1 = None

        if self.tokenizer.has_next() and self.tokenizer.next_token_type() == TOKENS.NOT:
            not_condition = TreeNode(self.tokenizer.next_token_type(), None)
            self.tokenizer.next()
            child_condition = self.parse_condition()
            not_condition.left = child_condition
            not_condition.right = None  # NOT is a unary operator
            return not_condition

        if self.tokenizer.has_next() and self.tokenizer.next_token_type() in (
            TOKENS.FUNCTION,
            TOKENS.AGGREGATOR,
        ):
            # we don't evaluate functions as part of this module, the function call
            # should be part of the record, e.g. record["YEAR(date_of_birth)"]
            # we we extract out the function and treat is as a VARIABLE

            collector = [self.tokenizer.next_token_value(), "("]

            self.tokenizer.next()
            if (
                not self.tokenizer.has_next
                or self.tokenizer.next_token_type() != TOKENS.LEFTPARENTHESES
            ):
                raise InvalidExpression("Functions should be followed by paranthesis")

            self.tokenizer.next()
            open_parentheses = 1

            while open_parentheses > 0:
                if not self.tokenizer.has_next():
                    break
                if self.tokenizer.next_token_type() == TOKENS.RIGHTPARENTHESES:
                    open_parentheses -= 1
                elif self.tokenizer.next_token_type() == TOKENS.LEFTPARENTHESES:
                    open_parentheses += 1
                collector.append(self.tokenizer.next())

            if open_parentheses != 0:
                raise InvalidExpression("Unbalanced parantheses")

            terminal1 = TreeNode(TOKENS.VARIABLE, "".join(collector))

        if (
            self.tokenizer.has_next()
            and self.tokenizer.next_token_type() == TOKENS.LEFTPARENTHESES
        ):
            # If we have a ( then go looking for the matching )

            self.tokenizer.next()
            expression = self.parse_expression()
            if (
                self.tokenizer.has_next()
                and self.tokenizer.next_token_type() == TOKENS.RIGHTPARENTHESES
            ):
                self.tokenizer.next()
                return expression
            raise InvalidExpression(f"`)` expected, but got `{self.tokenizer.next()}`")

        if not terminal1:
            terminal1 = self.parse_terminal()
        if self.tokenizer.has_next():
            if self.tokenizer.next_token_type() == TOKENS.OPERATOR:
                condition = TreeNode(
                    self.tokenizer.next_token_type(),
                    self.tokenizer.next_token_value().upper(),
                )
                self.tokenizer.next()
                terminal2 = self.parse_terminal()
                condition.left = terminal1
                condition.right = terminal2
                return condition
            raise InvalidExpression(
                f"Operator expected, but got `{self.tokenizer.next()}`"
            )
        raise InvalidExpression("Operator expected, but got nothing")

    def parse_terminal(self):
        if self.tokenizer.has_next():
            token_type = self.tokenizer.next_token_type()
            if token_type == TOKENS.INTEGER:
                n = TreeNode(token_type, int(self.tokenizer.next()))
                return n
            if token_type == TOKENS.FLOAT:
                n = TreeNode(token_type, float(self.tokenizer.next()))
                return n
            if token_type in (TOKENS.VARIABLE, TOKENS.NOT):
                n = TreeNode(token_type, self.tokenizer.next())
                return n
            if token_type == TOKENS.LITERAL:
                n = TreeNode(token_type, str(self.tokenizer.next()[1:-1]))
                return n
            if token_type == TOKENS.BOOLEAN:
                n = TreeNode(token_type, self.tokenizer.next().lower() == "true")
                return n
            if token_type == TOKENS.NULL:
                n = TreeNode(token_type, None)
                return n
            if token_type == TOKENS.DATE:
                n = TreeNode(token_type, parse_iso(self.tokenizer.next()[1:-1]))
                return n

            if token_type == TOKENS.LEFTPARENTHESES:
                collector = []
                if self.tokenizer.has_next():
                    self.tokenizer.next()
                while (
                    self.tokenizer.has_next()
                    and self.tokenizer.next_token_type() != TOKENS.RIGHTPARENTHESES
                ):
                    if self.tokenizer.next_token_type() != TOKENS.COMMA:
                        collector.append(self.interpret_value(self.tokenizer.next()))
                if self.tokenizer.next_token_type() == TOKENS.RIGHTPARENTHESES:
                    self.tokenizer.next()
                n = TreeNode(TOKENS.LITERAL, collector)
                return n

        raise InvalidExpression(f"Unexpected token, got `{self.tokenizer.next()}`")

    def interpret_value(self, value):
        if not isinstance(value, str):
            return value
        if value.upper() in ("TRUE", "FALSE"):
            return value.upper() == "TRUE"

        try:
            num = int(value)
            return num
        except ValueError:
            pass

        try:
            num = float(value)
            return num
        except ValueError:
            pass

        return parse_iso(value) or value

    def evaluate(self, variable_dict):
        return self.evaluate_recursive(self.root, variable_dict)

    def __call__(self, variable_dict):
        return self.evaluate_recursive(self.root, variable_dict)

    def evaluate_recursive(self, treeNode, variable_dict):
        if treeNode.token_type in (
            TOKENS.INTEGER,
            TOKENS.FLOAT,
            TOKENS.LITERAL,
            TOKENS.BOOLEAN,
            TOKENS.NULL,
            TOKENS.DATE,
        ):
            return treeNode.value
        if treeNode.token_type == TOKENS.VARIABLE:
            if treeNode.value[0] == treeNode.value[-1] == "`":
                treeNode.value = treeNode.value[1:-1]
            if treeNode.value in variable_dict:
                value = variable_dict[treeNode.value]
                return self.interpret_value(value)
            return None

        left = self.evaluate_recursive(treeNode.left, variable_dict)

        if treeNode.token_type == TOKENS.NOT:
            return not left

        right = self.evaluate_recursive(treeNode.right, variable_dict)

        if treeNode.token_type == TOKENS.OPERATOR:
            try:
                return OPERATORS[treeNode.value](left, right)
            except (TypeError, ValueError):
                return None
        if treeNode.token_type == TOKENS.AND:
            return left and right
        if treeNode.token_type == TOKENS.OR:
            return left or right

        raise InvalidExpression(
            f"Unexpected value of type `{str(treeNode.token_type)}`"
        )

    def to_dnf(self):
        """
        Converting to DNF as sometimes it's easier to deal with DNF than an
        expression tree.
        """
        return self._inner_to_dnf(self.root)

    def _inner_to_dnf(self, treeNode):
        if treeNode.token_type in (
            TOKENS.INTEGER,
            TOKENS.FLOAT,
            TOKENS.BOOLEAN,
            TOKENS.NULL,
            TOKENS.VARIABLE,
        ):
            return treeNode.value

        if treeNode.token_type in (TOKENS.DATE, TOKENS.LITERAL):
            return f'"{treeNode.value}"'

        left = self._inner_to_dnf(treeNode.left)
        right = None
        if treeNode.right:
            right = self._inner_to_dnf(treeNode.right)

        if treeNode.token_type == TOKENS.AND:
            return [left, right]

        if treeNode.token_type == TOKENS.OR:
            return [[left], [right]]

        if treeNode.token_type == TOKENS.OPERATOR:
            return (left, treeNode.value, right)

        if treeNode.token_type == TOKENS.NOT:
            # this isn't strict DNF
            return ("NOT", left)
