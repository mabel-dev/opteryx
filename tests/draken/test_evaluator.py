"""
Tests for compiled expression evaluators.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pytest


class TestExpressionNodes:
    """Test expression tree node classes."""
    
    def test_literal_expression(self):
        """Test literal expression creation and equality."""
        from opteryx.draken.evaluators.expression import LiteralExpression
        
        expr1 = LiteralExpression(42)
        expr2 = LiteralExpression(42)
        expr3 = LiteralExpression(43)
        
        assert expr1 == expr2
        assert expr1 != expr3
        assert expr1.value == 42
        assert repr(expr1) == "LiteralExpression(42)"
    
    def test_column_expression(self):
        """Test column expression creation and equality."""
        from opteryx.draken.evaluators.expression import ColumnExpression
        
        expr1 = ColumnExpression('x')
        expr2 = ColumnExpression('x')
        expr3 = ColumnExpression('y')
        
        assert expr1 == expr2
        assert expr1 != expr3
        assert expr1.column_name == 'x'
        assert repr(expr1) == "ColumnExpression('x')"
    
    def test_binary_expression(self):
        """Test binary expression creation and equality."""
        from opteryx.draken.evaluators.expression import (
            BinaryExpression,
            ColumnExpression,
            LiteralExpression,
        )
        
        left = ColumnExpression('x')
        right = LiteralExpression(1)
        
        expr1 = BinaryExpression('equals', left, right)
        expr2 = BinaryExpression('equals', left, right)
        expr3 = BinaryExpression('greater_than', left, right)
        
        assert expr1 == expr2
        assert expr1 != expr3
        assert expr1.operation == 'equals'
        assert expr1.left == left
        assert expr1.right == right
    
    def test_unary_expression(self):
        """Test unary expression creation and equality."""
        from opteryx.draken.evaluators.expression import (
            UnaryExpression,
            ColumnExpression,
        )
        
        operand = ColumnExpression('x')
        
        expr1 = UnaryExpression('not', operand)
        expr2 = UnaryExpression('not', operand)
        expr3 = UnaryExpression('is_null', operand)
        
        assert expr1 == expr2
        assert expr1 != expr3
        assert expr1.operation == 'not'
        assert expr1.operand == operand


class TestBasicEvaluation:
    """Test basic expression evaluation without PyArrow."""
    
    def test_expression_hashing(self):
        """Test that expressions can be hashed for caching."""
        from opteryx.draken.evaluators.expression import (
            ColumnExpression,
            LiteralExpression,
            BinaryExpression,
        )
        
        expr1 = BinaryExpression('equals', ColumnExpression('x'), LiteralExpression(1))
        expr2 = BinaryExpression('equals', ColumnExpression('x'), LiteralExpression(1))
        expr3 = BinaryExpression('equals', ColumnExpression('y'), LiteralExpression(1))
        
        # Same expressions should have same hash
        assert hash(expr1) == hash(expr2)
        
        # Different expressions should (usually) have different hash
        # Note: hash collisions are possible but unlikely for these simple cases
        assert hash(expr1) != hash(expr3)
    
    def test_cache_clearing(self):
        """Test that cache can be cleared."""
        from opteryx.draken.evaluators.evaluator import clear_cache, _evaluator_cache
        
        # Add something to cache
        _evaluator_cache[123] = "test"
        assert len(_evaluator_cache) > 0
        
        # Clear cache
        clear_cache()
        assert len(_evaluator_cache) == 0


# Tests that require PyArrow are marked to allow skipping if not available
try:
    import pyarrow as pa
    import opteryx.draken as draken
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False


@pytest.mark.skipif(not PYARROW_AVAILABLE, reason="PyArrow not available")
class TestSimpleComparisons:
    """Test simple comparison expressions."""
    
    def test_column_equals_literal(self):
        """Test column == literal pattern."""
        from opteryx.draken.evaluators import (
            evaluate,
            BinaryExpression,
            ColumnExpression,
            LiteralExpression,
        )
        
        # Create test morsel
        table = pa.table({'x': [1, 2, 3, 4, 5]})
        morsel = draken.Morsel.from_arrow(table)
        
        # Create expression: x == 3
        expr = BinaryExpression('equals', ColumnExpression('x'), LiteralExpression(3))
        
        # Evaluate
        result = evaluate(morsel, expr)
        
        # Check result
        result_list = [result[i] for i in range(morsel.num_rows)]
        assert result_list == [False, False, True, False, False]
    
    def test_column_not_equals_literal(self):
        """Test column != literal pattern."""
        from opteryx.draken.evaluators import (
            evaluate,
            BinaryExpression,
            ColumnExpression,
            LiteralExpression,
        )
        
        table = pa.table({'x': [1, 2, 3, 4, 5]})
        morsel = draken.Morsel.from_arrow(table)
        
        expr = BinaryExpression('not_equals', ColumnExpression('x'), LiteralExpression(3))
        result = evaluate(morsel, expr)
        
        result_list = [result[i] for i in range(morsel.num_rows)]
        assert result_list == [True, True, False, True, True]
    
    def test_column_greater_than_literal(self):
        """Test column > literal pattern."""
        from opteryx.draken.evaluators import (
            evaluate,
            BinaryExpression,
            ColumnExpression,
            LiteralExpression,
        )
        
        table = pa.table({'x': [1, 2, 3, 4, 5]})
        morsel = draken.Morsel.from_arrow(table)
        
        expr = BinaryExpression('greater_than', ColumnExpression('x'), LiteralExpression(3))
        result = evaluate(morsel, expr)
        
        result_list = [result[i] for i in range(morsel.num_rows)]
        assert result_list == [False, False, False, True, True]
    
    def test_column_less_than_literal(self):
        """Test column < literal pattern."""
        from opteryx.draken.evaluators import (
            evaluate,
            BinaryExpression,
            ColumnExpression,
            LiteralExpression,
        )
        
        table = pa.table({'x': [1, 2, 3, 4, 5]})
        morsel = draken.Morsel.from_arrow(table)
        
        expr = BinaryExpression('less_than', ColumnExpression('x'), LiteralExpression(3))
        result = evaluate(morsel, expr)
        
        result_list = [result[i] for i in range(morsel.num_rows)]
        assert result_list == [True, True, False, False, False]


@pytest.mark.skipif(not PYARROW_AVAILABLE, reason="PyArrow not available")
class TestVectorVectorComparisons:
    """Test vector-vector comparison expressions."""
    
    def test_column_equals_column(self):
        """Test column1 == column2 pattern."""
        from opteryx.draken.evaluators import (
            evaluate,
            BinaryExpression,
            ColumnExpression,
        )
        
        table = pa.table({
            'x': [1, 2, 3, 4, 5],
            'y': [1, 3, 3, 2, 5]
        })
        morsel = draken.Morsel.from_arrow(table)
        
        expr = BinaryExpression('equals', ColumnExpression('x'), ColumnExpression('y'))
        result = evaluate(morsel, expr)
        
        result_list = [result[i] for i in range(morsel.num_rows)]
        assert result_list == [True, False, True, False, True]
    
    def test_column_greater_than_column(self):
        """Test column1 > column2 pattern."""
        from opteryx.draken.evaluators import (
            evaluate,
            BinaryExpression,
            ColumnExpression,
        )
        
        table = pa.table({
            'x': [1, 2, 3, 4, 5],
            'y': [1, 3, 3, 2, 5]
        })
        morsel = draken.Morsel.from_arrow(table)
        
        expr = BinaryExpression('greater_than', ColumnExpression('x'), ColumnExpression('y'))
        result = evaluate(morsel, expr)
        
        result_list = [result[i] for i in range(morsel.num_rows)]
        assert result_list == [False, False, False, True, False]


@pytest.mark.skipif(not PYARROW_AVAILABLE, reason="PyArrow not available")
class TestCompoundExpressions:
    """Test compound expressions with AND, OR."""
    
    def test_and_expression(self):
        """Test compound AND expression: x == 1 AND y == 'england'."""
        from opteryx.draken.evaluators import (
            evaluate,
            BinaryExpression,
            ColumnExpression,
            LiteralExpression,
        )
        
        table = pa.table({
            'x': [1, 2, 1, 1, 2],
            'y': ['england', 'england', 'france', 'england', 'spain']
        })
        morsel = draken.Morsel.from_arrow(table)
        
        # x == 1
        expr1 = BinaryExpression('equals', ColumnExpression('x'), LiteralExpression(1))
        
        # y == 'england'
        expr2 = BinaryExpression('equals', ColumnExpression('y'), LiteralExpression(b'england'))
        
        # x == 1 AND y == 'england'
        and_expr = BinaryExpression('and', expr1, expr2)
        
        result = evaluate(morsel, and_expr)
        result_list = [result[i] for i in range(morsel.num_rows)]
        
        # Expected: [True, False, False, True, False]
        # Row 0: x=1, y='england' -> True AND True = True
        # Row 1: x=2, y='england' -> False AND True = False
        # Row 2: x=1, y='france' -> True AND False = False
        # Row 3: x=1, y='england' -> True AND True = True
        # Row 4: x=2, y='spain' -> False AND False = False
        assert result_list == [True, False, False, True, False]
    
    def test_or_expression(self):
        """Test compound OR expression."""
        from opteryx.draken.evaluators import (
            evaluate,
            BinaryExpression,
            ColumnExpression,
            LiteralExpression,
        )
        
        table = pa.table({
            'x': [1, 2, 3, 4, 5],
            'y': [10, 20, 30, 40, 50]
        })
        morsel = draken.Morsel.from_arrow(table)
        
        # x == 1
        expr1 = BinaryExpression('equals', ColumnExpression('x'), LiteralExpression(1))
        
        # y == 50
        expr2 = BinaryExpression('equals', ColumnExpression('y'), LiteralExpression(50))
        
        # x == 1 OR y == 50
        or_expr = BinaryExpression('or', expr1, expr2)
        
        result = evaluate(morsel, or_expr)
        result_list = [result[i] for i in range(morsel.num_rows)]
        
        assert result_list == [True, False, False, False, True]
    
    def test_nested_and_or(self):
        """Test nested AND/OR expressions."""
        from opteryx.draken.evaluators import (
            evaluate,
            BinaryExpression,
            ColumnExpression,
            LiteralExpression,
        )
        
        table = pa.table({
            'x': [1, 2, 3, 4, 5],
            'y': [10, 20, 30, 40, 50],
            'z': [100, 200, 300, 400, 500]
        })
        morsel = draken.Morsel.from_arrow(table)
        
        # (x == 1 OR x == 5)
        x_eq_1 = BinaryExpression('equals', ColumnExpression('x'), LiteralExpression(1))
        x_eq_5 = BinaryExpression('equals', ColumnExpression('x'), LiteralExpression(5))
        x_condition = BinaryExpression('or', x_eq_1, x_eq_5)
        
        # y > 20
        y_condition = BinaryExpression('greater_than', ColumnExpression('y'), LiteralExpression(20))
        
        # (x == 1 OR x == 5) AND y > 20
        final_expr = BinaryExpression('and', x_condition, y_condition)
        
        result = evaluate(morsel, final_expr)
        result_list = [result[i] for i in range(morsel.num_rows)]
        
        # Row 0: x=1, y=10 -> True AND False = False
        # Row 1: x=2, y=20 -> False AND False = False
        # Row 2: x=3, y=30 -> False AND True = False
        # Row 3: x=4, y=40 -> False AND True = False
        # Row 4: x=5, y=50 -> True AND True = True
        assert result_list == [False, False, False, False, True]


@pytest.mark.skipif(not PYARROW_AVAILABLE, reason="PyArrow not available")
class TestCaching:
    """Test evaluator caching."""
    
    def test_cache_reuse(self):
        """Test that evaluators are cached and reused."""
        from opteryx.draken.evaluators import (
            evaluate,
            BinaryExpression,
            ColumnExpression,
            LiteralExpression,
        )
        from opteryx.draken.evaluators.evaluator import _evaluator_cache, clear_cache
        
        # Clear cache first
        clear_cache()
        assert len(_evaluator_cache) == 0
        
        table = pa.table({'x': [1, 2, 3, 4, 5]})
        morsel = draken.Morsel.from_arrow(table)
        
        # Create and evaluate expression
        expr = BinaryExpression('equals', ColumnExpression('x'), LiteralExpression(3))
        result1 = evaluate(morsel, expr)
        
        # Cache should have one entry
        assert len(_evaluator_cache) == 1
        
        # Evaluate same expression again
        result2 = evaluate(morsel, expr)
        
        # Cache should still have one entry (reused)
        assert len(_evaluator_cache) == 1
        
        # Results should be the same
        result1_list = [result1[i] for i in range(morsel.num_rows)]
        result2_list = [result2[i] for i in range(morsel.num_rows)]
        assert result1_list == result2_list


@pytest.mark.skipif(not PYARROW_AVAILABLE, reason="PyArrow not available")
class TestFloatComparisons:
    """Test comparisons with float columns."""
    
    def test_float_column_comparison(self):
        """Test float column comparisons."""
        from opteryx.draken.evaluators import (
            evaluate,
            BinaryExpression,
            ColumnExpression,
            LiteralExpression,
        )
        
        table = pa.table({'x': [1.5, 2.7, 3.3, 4.1, 5.9]})
        morsel = draken.Morsel.from_arrow(table)
        
        expr = BinaryExpression('greater_than', ColumnExpression('x'), LiteralExpression(3.0))
        result = evaluate(morsel, expr)
        
        result_list = [result[i] for i in range(morsel.num_rows)]
        assert result_list == [False, False, True, True, True]


@pytest.mark.skipif(not PYARROW_AVAILABLE, reason="PyArrow not available")
class TestColumnRetrieval:
    """Test column expression evaluation."""
    
    def test_column_expression_evaluation(self):
        """Test that column expressions return the column vector."""
        from opteryx.draken.evaluators import evaluate, ColumnExpression
        
        table = pa.table({'x': [1, 2, 3, 4, 5]})
        morsel = draken.Morsel.from_arrow(table)
        
        expr = ColumnExpression('x')
        result = evaluate(morsel, expr)
        
        # Result should be the column vector
        result_list = [result[i] for i in range(morsel.num_rows)]
        assert result_list == [1, 2, 3, 4, 5]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
