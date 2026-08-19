"""Shared pytest configuration."""

import os
from pathlib import Path

import pytest

# Ensure TESTING env var is set for all tests
os.environ["TESTING"] = "1"


def pytest_collection_modifyitems(items):
    """Run process-wide browser tests after isolated unit/integration tests."""
    items.sort(key=lambda item: item.get_closest_marker("e2e") is not None)


@pytest.fixture(scope="session")
def browser_type_launch_args():
    """Allow local E2E runs to use an explicitly configured system browser."""
    executable = os.environ.get("PLAYWRIGHT_EXECUTABLE_PATH")
    if executable and Path(executable).exists():
        return {"executable_path": executable}
    return {}
