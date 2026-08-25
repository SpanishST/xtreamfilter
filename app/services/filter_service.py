"""Filter service — pure matching functions for include/exclude rules."""
from __future__ import annotations

import functools
import re
import unicodedata

from rapidfuzz import fuzz


@functools.lru_cache(maxsize=4096)
def _strip_accents(text: str) -> str:
    """Remove diacritical marks (accents) from *text* (cached)."""
    return "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )


# Small cache for compiled regex patterns
@functools.lru_cache(maxsize=256)
def _compile_regex(pattern: str, flags: int) -> re.Pattern | None:
    try:
        return re.compile(pattern, flags)
    except re.error:
        return None


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

    # Accent-insensitive comparison: strip diacritics so that e.g.
    # "TÉLÉ RÉALITÉ" matches a filter value "tele realite".
    if not case_sensitive:
        test_value = _strip_accents(test_value)
        test_pattern = _strip_accents(test_pattern)

    # Normalize multiple whitespace characters to a single space
    # so that e.g. "TELE  REALITE" (two spaces) matches "tele realite"
    if match_type in ("exact", "starts_with", "ends_with", "contains", "not_contains"):
        test_value = re.sub(r'\s+', ' ', test_value).strip()
        test_pattern = re.sub(r'\s+', ' ', test_pattern).strip()

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
            compiled = _compile_regex(pattern, flags)
            if compiled is None:
                return False
            return bool(compiled.search(value))
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


def group_similar_items(items: list, threshold: int = 85, fuzzy_limit: int = 0) -> list:
    """Group items by TMDB ID first, then by fuzzy name similarity as fallback.

    The fuzzy pass is O(n²) and is skipped when the number of items without a
    TMDB ID exceeds *fuzzy_limit* to avoid blocking the event loop on large
    result sets.  Grouping by TMDB ID is always performed regardless of size.

    Returns list of group dicts: {name, icon, items, count, rating, added}.
    """
    if not items:
        return []

    groups: list[dict] = []
    # Fast lookup: tmdb_id -> group dict (only for groups that have a TMDB ID)
    tmdb_index: dict[str, dict] = {}

    # Count items lacking a TMDB ID to decide whether fuzzy is safe
    without_tmdb = sum(
        1 for it in items
        if not _normalize_tmdb_id_for_grouping(it.get("tmdb_id") or it.get("tmdb"))
    )
    use_fuzzy = without_tmdb <= fuzzy_limit

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

        # 2) Fallback: fuzzy name similarity (only when set is small enough)
        if matched_group is None and use_fuzzy:
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
            "tmdb_id": g["tmdb_id"],
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


# ---------------------------------------------------------------------------
# SQL pushdown for source-filter rules
# ---------------------------------------------------------------------------
#
# Source include/exclude rules are normally applied in Python after fetching
# rows, which forces full-catalog scans.  When every active rule is a
# translatable type (exact/contains/not_contains/starts_with/ends_with/all,
# case-insensitive), the route can instead embed an equivalent SQL predicate.
# The XF_NORM() scalar function mirrors the Python-side normalization
# (lowercase + accent-strip + whitespace-collapse) so results match
# should_include()/matches_filter() exactly, including accented values.

XF_NORM = "xf_filter_norm"

_TRANSLATABLE_MATCH_TYPES = {
    "exact", "contains", "not_contains", "starts_with", "ends_with", "all",
}

_NORM_MEMO_CAP = 20000
# Shared across connections/requests: real catalogs repeat group names
# heavily between rows and between queries, so the memo hit rate is high.
_NORM_MEMO: dict[str, str] = {}


def make_xf_norm() -> callable:
    """Build the deterministic normalization function used by XF_NORM()."""

    def xf_norm(raw) -> str:
        text = raw if isinstance(raw, str) else ("" if raw is None else str(raw))
        cached = _NORM_MEMO.get(text)
        if cached is not None:
            return cached
        if text.isascii() and "  " not in text and "\t" not in text and "\n" not in text:
            # Hot path: already normalized apart from case.
            value = text.lower()
        else:
            value = _strip_accents(text.lower())
            value = re.sub(r"\s+", " ", value).strip()
        if len(_NORM_MEMO) >= _NORM_MEMO_CAP:
            _NORM_MEMO.clear()
        _NORM_MEMO[text] = value
        return value

    return xf_norm


def register_xf_norm(conn) -> None:
    """Attach XF_NORM() to a SQLite connection (idempotent, cheap)."""
    conn.create_function(XF_NORM, 1, make_xf_norm(), deterministic=True)


def _norm_atom(field_expr: str, rule: dict) -> tuple[str, list] | None:
    """Translate one rule into (sql_fragment, params) or None if untranslatable."""
    match_type = rule.get("match", "contains")
    if match_type not in _TRANSLATABLE_MATCH_TYPES or rule.get("case_sensitive"):
        return None
    raw_value = str(rule.get("value", ""))
    if match_type == "all":
        # matches_filter('all') always matches → an exclude-all veto excludes
        # everything; as part of an include list it can never be satisfied.
        return ("0", [])
    if not raw_value:
        # Empty patterns never match in matches_filter().
        return ("0", [])

    norm_expr = f"{XF_NORM}({field_expr})"
    norm_param = f"{XF_NORM}(?)"
    params: list = [raw_value]

    if match_type == "exact":
        return (f"{norm_expr} = {norm_param}", params)
    if match_type == "contains":
        return (f"instr({norm_expr}, {norm_param}) > 0", params)
    if match_type == "not_contains":
        return (f"instr({norm_expr}, {norm_param}) = 0", params)
    if match_type == "starts_with":
        sql = f"substr({norm_expr}, 1, length({norm_param})) = {norm_param}"
        return (sql, [raw_value, raw_value])
    if match_type == "ends_with":
        sql = f"substr({norm_expr}, -length({norm_param})) = {norm_param}"
        return (sql, [raw_value, raw_value])
    return None


def _field_predicate(field_expr: str, rules: list[dict] | None) -> tuple[str, list] | None:
    """Translate one field's rule list; ('1', []) when no rules apply."""
    if not rules:
        return ("1", [])
    includes = [r for r in rules if r.get("type") == "include"]
    excludes = [r for r in rules if r.get("type") == "exclude"]

    parts: list[str] = []
    params: list = []
    for rule in includes:
        atom = _norm_atom(field_expr, rule)
        if atom is None:
            return None
        parts.append(atom[0])
        params.extend(atom[1])
    if includes:
        parts = ["(" + " OR ".join(parts) + ")"]
        for rule in excludes:
            atom = _norm_atom(field_expr, rule)
            if atom is None:
                return None
            parts.append(f"NOT ({atom[0]})")
            params.extend(atom[1])
        return (" AND ".join(parts), params)

    for rule in excludes:
        atom = _norm_atom(field_expr, rule)
        if atom is None:
            return None
        parts.append(f"NOT ({atom[0]})")
        params.extend(atom[1])
    return (" AND ".join(parts) if parts else "1", params)


def source_rules_predicate(
    sources_config: dict,
    content_types: list[str],
) -> tuple[str, list] | None:
    """Build a WHERE fragment applying every source's rules across types.

    Returns None when any active rule cannot be translated to SQL (regex,
    case-sensitive, …); callers then fall back to bounded Python scanning.
    """
    if not sources_config or not content_types:
        return None
    terms: list[str] = []
    params: list = []
    for source_id, source_cfg in sources_config.items():
        all_filters = source_cfg.get("filters") or {}
        for content_type in content_types:
            content_filters = all_filters.get(content_type) or {}
            group_pred = _field_predicate("s.group_name", content_filters.get("groups"))
            name_pred = _field_predicate("s.name", content_filters.get("channels"))
            if group_pred is None or name_pred is None:
                return None
            term = "(s.source_id = ? AND s.content_type = ?"
            term_params: list = [source_id, content_type]
            if group_pred[0] != "1":
                term += f" AND {group_pred[0]}"
                term_params.extend(group_pred[1])
            if name_pred[0] != "1":
                term += f" AND {name_pred[0]}"
                term_params.extend(name_pred[1])
            terms.append(term + ")")
            params.extend(term_params)
    if not terms:
        return None
    return ("(" + " OR ".join(terms) + ")", params)
