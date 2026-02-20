"""Filter service — pure matching functions for include/exclude rules."""
from __future__ import annotations

import re
import unicodedata

from rapidfuzz import fuzz


def matches_filter(value: str, filter_rule: dict) -> bool:
    """Check if a value matches a filter rule."""
    match_type = filter_rule.get("match", "contains")

    if match_type == "all":
        return True

    pattern = filter_rule.get("value", "")
    case_sensitive = filter_rule.get("case_sensitive", False)

    if not pattern:
        return False

    test_value = value if case_sensitive else value.lower()
    test_pattern = pattern if case_sensitive else pattern.lower()

    if match_type == "exact":
        return test_value == test_pattern
    elif match_type == "starts_with":
        return test_value.startswith(test_pattern)
    elif match_type == "ends_with":
        return test_value.endswith(test_pattern)
    elif match_type == "contains":
        return test_pattern in test_value
    elif match_type == "not_contains":
        return test_pattern not in test_value
    elif match_type == "regex":
        try:
            flags = 0 if case_sensitive else re.IGNORECASE
            return bool(re.search(pattern, value, flags))
        except re.error:
            return False

    return False


def should_include(value: str, filter_rules: list[dict]) -> bool:
    """Determine if a value should be included based on filter rules.

    Semantics:
      1. If there are include rules, the value must match at least one of
         them to be retained.  (No include rules ⇒ everything is retained.)
      2. From the retained set, any value matching an exclude rule is removed.

    In other words: **include first, then exclude**.
    """
    include_rules = [r for r in filter_rules if r.get("type") == "include"]
    exclude_rules = [r for r in filter_rules if r.get("type") == "exclude"]

    # Step 1 — include gate: if include rules exist the value must match one.
    if include_rules:
        included = any(matches_filter(value, rule) for rule in include_rules)
        if not included:
            return False

    # Step 2 — exclude gate: reject if any exclude rule matches.
    for rule in exclude_rules:
        if matches_filter(value, rule):
            return False

    return True


def normalize_name(name: str) -> str:
    """Normalize a title for fuzzy comparison.

    Strips common channel/source prefixes, language suffixes/tags, quality markers,
    and accents so that e.g. 'A+ - Hijack (2023) (GB)', 'FR - Hijack (2023) (GB)',
    and 'Hijack_fr' are all compared on their core title.
    """
    n = name.strip().lower()
    # Strip accents
    n = "".join(c for c in unicodedata.normalize("NFD", n) if unicodedata.category(c) != "Mn")
    # Remove common channel/source prefixes
    for _ in range(3):
        m = re.match(r"^(\S{1,4})\s*-\s+", n)
        if not m:
            m = re.match(r"^(\S*[\d+]\S*)\s*-\s+", n)
            if m and len(m.group(1)) > 10:
                m = None
        if not m:
            m = re.match(r"^(\S{1,4})\s+-\S+\s+", n)
        if not m:
            m = re.match(r"^([A-Za-z0-9+]{1,4}):\s+", n)
        if m:
            n = n[m.end():]
        else:
            break
    # Strip trailing language suffixes
    n = re.sub(
        r"[_-](fr|en|de|es|it|pt|nl|pl|ar|tr|jp|kr|gb|us|br|as|vost|vostfr|multi)\s*$",
        "",
        n,
    )
    # Remove parenthesized/bracketed tags
    n = re.sub(r"\s*[\(\[][^)\]]*[\)\]]\s*", " ", n)
    # Remove trailing language/region codes
    n = re.sub(
        r"\s+(?:(?:fr|en|de|es|it|pt|nl|pl)(?:-(?:fr|en|de|es|it|pt|nl|pl))?)\s*$",
        "",
        n,
    )
    # Remove quality / codec tags
    n = re.sub(
        r"\b(4k|uhd|fhd|hd|sd|hdr|hdr10|dolby|atmos|hevc|h\.?265|h\.?264|x264|x265|"
        r"bluray|blu-ray|webrip|web-dl|remux|multi|vf|vo|vost|vostfr|french|english|"
        r"truefrench|cam|ts|md)\b",
        "",
        n,
        flags=re.IGNORECASE,
    )
    # Remove trailing year patterns
    n = re.sub(r"\s*-\s*\d{4}\s*$", "", n)
    n = re.sub(r"\s+\d{4}\s*$", "", n)
    # Collapse whitespace
    n = re.sub(r"\s+", " ", n).strip()
    # Remove leading/trailing punctuation
    n = re.sub(r"^[\s\-\|\.:]+", "", n)
    n = re.sub(r"[\s\-\|\.:]+$", "", n)
    return n


def _normalize_tmdb_id_for_grouping(value) -> str | None:
    """Return a normalised TMDB ID string (digits only) or None."""
    if value is None:
        return None
    raw = str(value).strip().lower()
    if raw.startswith("tmdb:"):
        raw = raw[5:].strip()
    if raw.isdigit() and raw != "0":
        return raw
    return None


def group_similar_items(items: list, threshold: int = 85) -> list:
    """Group items by TMDB ID first, then by fuzzy name similarity as fallback.

    Returns list of group dicts: {name, icon, items, count, rating, added}.
    """
    if not items:
        return []

    groups: list[dict] = []
    # Fast lookup: tmdb_id -> group dict (only for groups that have a TMDB ID)
    tmdb_index: dict[str, dict] = {}

    for item in items:
        item_name = item.get("name", "")
        item_normalized = normalize_name(item_name)
        item_tmdb = _normalize_tmdb_id_for_grouping(
            item.get("tmdb_id") or item.get("tmdb")
        )

        matched_group = None

        # 1) Try TMDB ID grouping (O(1))
        if item_tmdb:
            matched_group = tmdb_index.get(item_tmdb)

        # 2) Fallback: fuzzy name similarity
        if matched_group is None:
            best_score = 0
            for group in groups:
                score = fuzz.token_sort_ratio(item_normalized, group["normalized"])
                if score >= threshold and score > best_score:
                    best_score = score
                    matched_group = group

        if matched_group is not None:
            matched_group["items"].append(item)
            if len(item_name) < len(matched_group["name"]):
                matched_group["name"] = item_name
            if not matched_group["icon"] and item.get("icon"):
                matched_group["icon"] = item["icon"]
            # Track best rating and newest added date
            item_rating = item.get("rating", 0) or 0
            item_added = item.get("added", 0) or 0
            if item_rating > matched_group["rating"]:
                matched_group["rating"] = item_rating
            if item_added > matched_group["added"]:
                matched_group["added"] = item_added
            # Promote TMDB ID to the group index if the group didn't have one yet
            if item_tmdb and matched_group.get("tmdb_id") is None:
                matched_group["tmdb_id"] = item_tmdb
                tmdb_index[item_tmdb] = matched_group
        else:
            new_group: dict = {
                "normalized": item_normalized,
                "name": item_name,
                "icon": item.get("icon", ""),
                "items": [item],
                "rating": item.get("rating", 0) or 0,
                "added": item.get("added", 0) or 0,
                "tmdb_id": item_tmdb,
            }
            groups.append(new_group)
            if item_tmdb:
                tmdb_index[item_tmdb] = new_group

    return [
        {
            "name": g["name"],
            "icon": g["icon"],
            "items": g["items"],
            "count": len(g["items"]),
            "rating": g["rating"],
            "added": g["added"],
        }
        for g in groups
    ]


def build_category_map(categories: list) -> dict:
    """Build a category_id -> category_name map."""
    cat_map = {}
    for cat in categories:
        if isinstance(cat, dict):
            cat_map[str(cat.get("category_id", ""))] = cat.get("category_name", "")
        elif isinstance(cat, str):
            cat_map[cat] = cat
    return cat_map


def safe_get_category_name(cat) -> str:
    if isinstance(cat, dict):
        return cat.get("category_name", "")
    elif isinstance(cat, str):
        return cat
    return ""


def safe_get_category_id(cat) -> str:
    if isinstance(cat, dict):
        return cat.get("category_id", "")
    elif isinstance(cat, str):
        return cat
    return ""


def safe_copy_category(cat) -> dict:
    if isinstance(cat, dict):
        return cat.copy()
    elif isinstance(cat, str):
        return {"category_id": cat, "category_name": cat}
    return {"category_id": "", "category_name": ""}
