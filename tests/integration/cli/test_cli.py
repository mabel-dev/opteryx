import os
import sys
import subprocess

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

def run_cli(args):
    """Helper function to run the CLI and return the result."""
    from tests import find_file

    path = find_file("**/__main__.py")
    result = subprocess.run(
        [sys.executable, path] + args,
        capture_output=True,
        text=True,
        timeout=5
    )

    return result


def test_basic_execution():
    """Test basic SQL execution through the CLI."""
    result = run_cli(["SELECT * FROM $planets"])
    assert result.returncode == 0

def test_save_to_file():
    result = run_cli(["--o", "planets.parquet", "SELECT * FROM $planets"])

    assert result.returncode == 0, result.stderr
    assert "Written result to" in result.stdout

def test_colorized():
    """Test the CLI with colorized output enabled."""
    result = run_cli(["--color", "SELECT * FROM $planets"])
    assert result.returncode == 0

def test_decolorized():
    """Test the CLI with colorized output disabled."""
    result = run_cli(["--no-color", "SELECT * FROM $planets"])
    assert result.returncode == 0

def test_stats():
    """Test the CLI with statistics reporting enabled."""
    result = run_cli(["--stats", "SELECT * FROM $planets"])
    assert result.returncode == 0

def test_no_stats():
    """Test the CLI with statistics reporting disabled."""
    result = run_cli(["--no-stats", "SELECT * FROM $planets"])
    assert result.returncode == 0

def test_cycles():
    """Test the CLI with multiple execution cycles for benchmarking."""
    result = run_cli(["--cycles", "3", "SELECT * FROM $planets"])
    assert result.returncode == 0

def test_table_width():
    """Test the CLI with table width limiting enabled."""
    result = run_cli(["--table_width", "SELECT * FROM $planets"])
    assert result.returncode == 0

def test_column_width():
    """Test the CLI with maximum column width restriction."""
    result = run_cli(["--no-color", "--max_col_width", "4", "SELECT * FROM $planets"])
    output = result.stdout
    assert result.returncode == 0
    assert '│ Merc │' in output, output

def test_unknown_param():
    """Test the CLI with an unrecognized parameter, expecting an error."""
    result = run_cli(["--verbose", "SELECT * FROM $planets"])
    assert result.returncode != 0

if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
