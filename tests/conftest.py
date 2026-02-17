"""Shared pytest configuration."""

import os

# Ensure TESTING env var is set for all tests
os.environ["TESTING"] = "1"
