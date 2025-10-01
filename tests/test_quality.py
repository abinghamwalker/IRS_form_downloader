import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parent.parent


def run_command(command, check=True):
    """Run a command and fail the test if it returns a non-zero exit code."""
    process = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if check and process.returncode != 0:
        pytest.fail(
            f"Command `{' '.join(command)}` failed with exit "
            f"code {process.returncode}.\n"
            f"--- STDOUT ---\n{process.stdout}\n"
            f"--- STDERR ---\n{process.stderr}",
            pytrace=False,
        )
    return process


@pytest.mark.quality
def test_black_formatting():
    """Checks that all Python files are formatted correctly with Black."""
    command = [sys.executable, "-m", "black", "--check", "."]
    run_command(command)


@pytest.mark.quality
def test_ruff_linting():
    """Checks that all Python files pass Ruff's linting rules."""
    command = [sys.executable, "-m", "ruff", "check", "."]
    run_command(command)
