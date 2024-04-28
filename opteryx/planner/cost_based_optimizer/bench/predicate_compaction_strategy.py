from dataclasses import dataclass
from typing import Optional


@dataclass
class Limit:
    value: Optional[int]  # None indicates unbounded
    inclusive: bool


@dataclass
class ValueRange:
    lower: Limit = None  # Lower limit of the range
    upper: Limit = None  # Upper limit of the range
    untrackable: bool = False

    def update_with_predicate(self, operator: str, value: int):
        if self.untrackable or not operator in ("=", ">=", "<=", ">", "<"):
            self.untrackable = True
            return

        new_limit = Limit(value, inclusive=operator in ("=", ">=", "<="))

        if operator in ("=", ">=", ">"):
            if self.lower is None:
                self.lower = new_limit
            # Update lower limit if the new one is more restrictive
            elif new_limit.value > self.lower.value or (
                new_limit.value == self.lower.value and new_limit.inclusive
            ):
                self.lower = new_limit
        if operator in ("=", "<=", "<"):
            if self.upper is None:
                self.upper = new_limit
            # Update upper limit if the new one is more restrictive
            elif new_limit.value < self.upper.value or (
                new_limit.value == self.upper.value and new_limit.inclusive
            ):
                self.upper = new_limit

    def __bool__(self):
        if self.upper is None or self.lower is None:
            return True
        return self.upper.value >= self.lower.value

    def __str__(self) -> str:
        _range = ""
        if self.untrackable:
            return "Unsupported Conditions"
        if not (self):
            return "Invalid Range"
        if self.lower and self.upper and self.lower.value == self.upper.value:
            return f" = {self.lower.value}"
        if self.lower is not None:
            _range += f" >{'=' if self.lower.inclusive else ''} {self.lower.value}"
        if self.upper is not None:
            _range += f" <{'=' if self.upper.inclusive else ''} {self.upper.value}"
        return _range


# Example usage
initial_range = ValueRange()
initial_range.update_with_predicate(">", 3)
initial_range.update_with_predicate("<", 10)
initial_range.update_with_predicate("=", 7)
print("Range after applying predicates:", initial_range)


def test_initialization():
    vr = ValueRange()
    assert vr.lower is None
    assert vr.upper is None
    assert not vr.untrackable


def test_updates_with_various_predicates():
    vr = ValueRange()
    vr.update_with_predicate(">", 3)
    assert vr.lower == Limit(3, False)
    vr.update_with_predicate("<", 10)
    assert vr.upper == Limit(10, False)


def test_equality_predicate():
    vr = ValueRange()
    vr.update_with_predicate("=", 5)
    assert vr.lower == vr.upper == Limit(5, True)


def test_untrackable_condition():
    vr = ValueRange()
    vr.update_with_predicate("=", 5)
    vr.update_with_predicate("LIKE", "%name")  # This should make the range untrackable
    assert vr.untrackable


def test_predicate_compaction():
    vr = ValueRange()
    # Apply a series of less-than predicates
    vr.update_with_predicate("<", 10)
    vr.update_with_predicate("<", 7)  # Most restrictive
    vr.update_with_predicate("<", 8)
    vr.update_with_predicate("<", 9)

    # After compaction, the upper limit should be '< 7'
    expected_upper_limit = Limit(7, False)  # Assuming exclusive bounds for '<'
    assert (
        vr.upper == expected_upper_limit
    ), f"Expected upper limit to be {expected_upper_limit}, got {vr.upper}"


test_initialization()
test_updates_with_various_predicates()
test_equality_predicate()
test_untrackable_condition()
test_predicate_compaction()
print("okay")
