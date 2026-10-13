"""Filename sanitization tests for Unicode titles."""

import unicodedata

from app.services.cart_service import sanitize_filename


def test_sanitize_filename_preserves_composed_accents():
    title = "Megalodón - año"

    assert sanitize_filename(title) == title


def test_sanitize_filename_composes_decomposed_accents():
    title = unicodedata.normalize("NFD", "Megalodón - año")

    assert sanitize_filename(title) == "Megalodón - año"
