from typing import Optional
from typing import Union # db
from typing import List, Tuple

import glob
import csv
import json
import logging
import os
import re
import unicodedata
import math
from azure.core.credentials_async import AsyncTokenCredential
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.aio import SearchClient
from azure.search.documents.indexes.aio import SearchIndexClient
from azure.search.documents.models import VectorizedQuery 
from azure.search.documents.indexes.models import (
    SearchField,
    SearchFieldDataType,  
    SimpleField,
    SearchIndex,
    VectorSearch,
    VectorSearchProfile,
    HnswAlgorithmConfiguration)
from azure.ai.inference.aio import EmbeddingsClient
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError
from .util import ChatRequest
from dataclasses import dataclass
from datetime import date


CredentialType = Union[AsyncTokenCredential, AzureKeyCredential] # db

# SERIAL_PATTERNS = [
#     # # Explicit labels: "serial 5550", "matricola 5550", "s/n 5550", etc.
#     # re.compile(r"(?:serial(?:e)?|matricola|s\/n|sn)\s*[:\-]?\s*([A-Za-z0-9\/\-]+)", re.IGNORECASE),

#     # # Common formats: 49/050, 50/050
#     # re.compile(r"\b(\d{1,4}\/\d{2,4})\b"),

#     # Prefer labeled serials first
#     re.compile(r"(?:serial(?:e)?|matricola|s\/n|sn)\s*[:\-]?\s*(\d{4})",re.IGNORECASE,),
# ]

# Prefer labeled serials first
LABELED_SERIAL_RE = re.compile(
    r"(?:serial(?:e)?|matricola|s\/n|sn)\s*[:\-]?\s*(\d{4})",
    re.IGNORECASE,
)

# Fallback: any standalone 4-digit number (word boundary)
FOUR_DIGIT_RE = re.compile(r"\b(\d{4})\b")

# words that join multiple quoted phrases
_AND_RE = re.compile(r"\b(e|and|with|con)\b", re.I)
_OR_RE = re.compile(r"\b(o|or)\b", re.I)

# what counts as "phrase boundary" right after a closing quote (ignoring spaces)
# - end of string
# - punctuation/separators
# - connectors (so: "a" e "b")
_BOUNDARY_AFTER_QUOTE_RE = re.compile(
    r"""^\s*(?:$|[,;.:?!()\]\}]|\b(?:e|and|with|con|o|or)\b)""",
    re.I,
)

_WS_RE = re.compile(r"\s+")
# keep letters/digits, turn everything else into spaces
_NON_ALNUM_RE = re.compile(r"[^\w]+", flags=re.UNICODE)
_ALNUM_ONLY_RE = re.compile(r"[^a-z0-9]+", re.I)



MACHINE_HINTS = {
    "MODELLO", "MACCHINA", "MACCHINE", "COMMESSA", "MATRICOLA", "VERSIONE"
}

COMPONENT_HINTS = {
    "TESTA", "COMPONENTE", "ASSI", "RIDUTTORE", "MOTORE", "ROTANTE", "ROTAZIONE",
    "MANDRINO", "VALVOLA", "SENSORE", "ENCODER"
}

# "code-like" = contains at least one digit and at least one letter
# and is mostly made of letters/digits/separators.
_CODELIKE_RE = re.compile(r"^(?=.*[A-Za-z])(?=.*\d)[A-Za-z0-9][A-Za-z0-9\s\-/_.]*[A-Za-z0-9]$")


# Lucene special characters that should be escaped in query_type=full
_LUCENE_SPECIAL_RE = re.compile(r'(\+|-|!|\(|\)|\{|}|\[|\]|\^|"|~|\*|\?|:|\\|/|&&|\|\|)')

TITLE_MODE_RE = re.compile(r"^\s*titolo\s*(?:(all|any)\s*)?:\s*(.+)$", re.I)

# Matches: "commessa 20204", "commesse 20204 e 19048", "commesse: 20204,19048,12345",
# also "commessa 20204 e commessa 19048" (it will catch both).
_COMMESSA_WORD_RE = re.compile(r"\bcommess[ae]\b", re.I)

# After the word commessa/commesse, capture a run that may include numbers and separators
_COMMESSE_SPAN_RE = re.compile(
    r"\bcommess[ae]\b\s*[:\-]?\s*"
    r"(?P<span>(?:\d{4,6}(?:\s*(?:,|;|/|\||\be\b|\band\b|\bo\b|\bor\b)\s*)?)+)",
    re.I,
)

def _canon_for_match(s: str) -> str:
    """
    Canonicalize text for matching:
    - normalize unicode
    - convert NBSP and weird spaces to normal space
    - collapse whitespace
    - normalize thousand separators inside numbers (5.200/5,200 -> 5200)
    """
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)

    # normalize all space-like chars (NBSP etc.) to normal spaces
    s = re.sub(r"[\u00A0\u2000-\u200B\u202F\u205F\u3000]", " ", s)

    # normalize thousand separators in integers
    s = normalize_numeric_literals(s)

    # collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_numeric_literals(text: str) -> str:
    """
    Convert thousand-formatted integers to plain digits:
      4.000 -> 4000
      4,000 -> 4000
    Leaves decimal numbers (4.5 or 4,50) untouched because they
    contain a fractional part.
    """
    if not text:
        return text

    # Replace thousands grouped by '.' or ',' but NOT decimal forms like 4.5 or 4,5
    # We handle groups of at least 4 digits (e.g. 4000) and the common grouping patterns.
    def repl(m):
        whole = m.group(0)
        # remove all separators . and ,
        return re.sub(r"[.,]", "", whole)

    # Pattern matches numbers that contain at least one group-sep like 1.000 or 1,000 or 12.500
    return re.sub(r"\b\d{1,3}(?:[.,]\d{3})+\b", repl, text)


def _group_thousands(n_str: str, sep: str = ".") -> str:
    """Group an integer string with given separator from right every 3 digits."""
    if not n_str or not n_str.isdigit():
        return n_str
    parts = []
    i = len(n_str)
    while i > 0:
        start = max(0, i - 3)
        parts.append(n_str[start:i])
        i -= 3
    return sep.join(reversed(parts))


def expand_thousand_variants(phrase: str) -> list[str]:
    """
    Expand integer tokens in a phrase to cover:
      - plain digits (5200)
      - dotted thousands (5.200)
      - comma thousands (5,200)
    Works whether the input already has separators or not.
    """
    if not phrase:
        return [phrase]

    token_re = re.compile(r"\b\d{1,3}(?:[.,]\d{3})+\b|\b\d{4,}\b")
    nums = token_re.findall(phrase)
    if not nums:
        return [phrase]

    variants = [phrase]

    for tok in nums:
        plain = re.sub(r"[.,]", "", tok)
        if not (plain.isdigit() and len(plain) >= 4):
            continue

        dotted = _group_thousands(plain, ".")
        comma = _group_thousands(plain, ",")

        # generate new phrases by replacing this token
        new_variants = []
        for base in variants:
            new_variants.append(base.replace(tok, plain))
            new_variants.append(base.replace(tok, dotted))
            new_variants.append(base.replace(tok, comma))
        variants = new_variants

    # dedupe preserve order
    out = []
    seen = set()
    for v in variants:
        v2 = re.sub(r"\s+", " ", v).strip()
        if v2 and v2 not in seen:
            seen.add(v2)
            out.append(v2)
    return out

def commessa_prefix_to_range_filter(prefix: str) -> str | None:
    """
    Build an OData filter that emulates startswith(commessa, prefix)
    using lexicographic range comparisons on the commessa string field.

    For digit-only prefixes, use:
      commessa ge '25' and commessa lt '26'
    """
    p = (prefix or "").strip()
    if not p or not p.isdigit():
        return None

    width = len(p)
    upper = str(int(p) + 1).zfill(width)

    # Example: prefix=25 => ge '25' and lt '26'
    return f"(commessa ge '{_escape_odata_string(p)}' and commessa lt '{_escape_odata_string(upper)}')"

def commessa_prefixes_to_filter(prefixes: list[str]) -> str | None:
    clauses = []
    for p in prefixes:
        c = commessa_prefix_to_range_filter(p)
        if c:
            clauses.append(c)
    if not clauses:
        return None
    return "(" + " or ".join(clauses) + ")"

def extract_commesse(text: str) -> list[str]:
    """
    Extract all commessa numbers (4-6 digits) from phrases like:
      - "commessa 20204 e commessa 19048"
      - "commesse 20204 e 19048"
      - "commesse 20204, 19048, 12345"
    Returns list of strings, deduped, in appearance order.
    """
    if not text:
        return []

    out: list[str] = []
    seen = set()

    # 1) Span-based extraction after "commessa/e"
    for m in _COMMESSE_SPAN_RE.finditer(text):
        span = m.group("span") or ""
        nums = re.findall(r"\d{4,6}", span)
        for n in nums:
            if n not in seen:
                seen.add(n)
                out.append(n)

    if out:
        return out

    # 2) Fallback: if they mention "commess..." anywhere, grab nearby 4-6 digit numbers
    # (handles odd punctuation or text like "commesse (20204,19048)")
    if _COMMESSA_WORD_RE.search(text):
        nums = re.findall(r"\d{4,6}", text)
        for n in nums:
            if n not in seen:
                seen.add(n)
                out.append(n)

    return out

def parse_title_mode(q: str):
    m = TITLE_MODE_RE.match(q)
    if not m:
        return None, None
    mode = (m.group(1) or "").lower() or None   # None / 'all' / 'any'
    title_query = m.group(2).strip()
    return mode, title_query

def is_code_like(s: str) -> bool:
    s = s.strip()
    if len(s) < 3:
        return False
    return _CODELIKE_RE.match(s) is not None

def code_parts(code: str) -> list[str]:
    # keep only alnum then split into alpha/num groups
    alnum = re.sub(r"[^A-Za-z0-9]", "", code)
    return re.findall(r"[A-Za-z]+|\d+", alnum)

def build_code_token_clause(code: str) -> str:
    """
    Returns a Lucene clause that matches code regardless of spacing/tokenization:
    - full concatenated token
    - AND combinations of group boundaries
    """
    parts = code_parts(code)
    if not parts:
        return f"\"{escape_lucene_phrase(code)}\""

    # canonical concatenated form (covers r1600rc token)
    concat = "".join(parts).upper()

    # if only 2 groups, cover:
    #   AB123 OR (AB AND 123) OR (AB1 AND 23) isn't meaningful,
    # so do just concat OR (A AND B) style for 2 groups
    clauses = [escape_lucene_text(concat)]

    if len(parts) >= 2:
        # all parts separate: (R AND 1600 AND RC)
        clauses.append("(" + " AND ".join(escape_lucene_text(p.upper()) for p in parts) + ")")

    if len(parts) == 3:
        a, b, c = (p.upper() for p in parts)
        # (R AND 1600RC)
        clauses.append(f"({a} AND {b}{c})")
        # (R1600 AND RC)
        clauses.append(f"({a}{b} AND {c})")

    # Dedup preserve order
    out = []
    for cl in clauses:
        if cl not in out:
            out.append(cl)

    return "(" + " OR ".join(out) + ")"


def infer_machine_intent(user_text: str) -> bool:
    t = user_text.upper()
    has_machine = any(w in t for w in MACHINE_HINTS)
    has_component = any(w in t for w in COMPONENT_HINTS)
    # If they mention components and NOT machine hints, assume it's not a machine model filter
    if has_component and not has_machine:
        return False
    # Otherwise, allow (you can tune this)
    return has_machine

def split_quoted_phrases(text: str) -> Tuple[str, List[str], str]:
    """
    Returns: (free_text, phrases, op)

    - phrases: list of quoted phrases
    - free_text: text with quoted phrases removed (collapsed spaces)
    - op: "OR" if any connector between quotes is o/or; else "AND" if any is e/and/with/con; else "OR" default.

    Robust quoting rules:
    - Support CSV escaping inside quotes: "" -> literal "
    - A " inside quotes closes ONLY if what follows is a boundary (separator/end/connector).
      Otherwise treat it as a literal inches quote.
    - Lenient: if the last quote is unterminated, accept up to end-of-string.
    """
    t = text.replace("“", '"').replace("”", '"').replace("’", "'")
    n = len(t)

    phrases: List[str] = []
    spans: List[Tuple[int, int]] = []  # ranges [start, end) to remove from free_text

    # def looks_like_boundary_after_quote(pos_quote: int) -> bool:
    #     tail = t[pos_quote + 1 :]
    #     return _BOUNDARY_AFTER_QUOTE_RE.match(tail) is not None

    def looks_like_boundary_after_quote(pos_quote: int) -> bool:
        # If next char is immediately alnum (no space), likely inches (5"dia, 5"6)
        if pos_quote + 1 < n and t[pos_quote + 1].isalnum():
            return False
        # Otherwise, close the quote (space/newline/punct/end are all OK)
        return True

    i = 0
    while i < n:
        if t[i] != '"':
            i += 1
            continue

        start = i
        i += 1
        buf: List[str] = []
        closed = False

        while i < n:
            ch = t[i]
            if ch == '"':
                # CSV escape: "" -> literal "
                if i + 1 < n and t[i + 1] == '"':
                    buf.append('"')
                    i += 2
                    continue

                # Close only if boundary after quote
                if looks_like_boundary_after_quote(i):
                    i += 1
                    closed = True
                    break

                # otherwise inches quote inside the phrase
                buf.append('"')
                i += 1
                continue

            buf.append(ch)
            i += 1

        phrase = "".join(buf).strip()
        if phrase:
            phrases.append(phrase)
            spans.append((start, i if closed else n))

        if not closed:
            # unterminated final phrase -> we consumed to end-of-string
            break

    # infer operator between quoted phrases (same semantics you had)
    op = "OR"
    if len(spans) >= 2:
        between_tokens = []
        for (a0, a1), (b0, b1) in zip(spans, spans[1:]):
            between_tokens.append(t[a1:b0].lower())

        if any(_OR_RE.search(c) for c in between_tokens):
            op = "OR"
        elif any(_AND_RE.search(c) for c in between_tokens):
            op = "AND"

    # build free_text: remove spans, collapse spaces
    out: List[str] = []
    last = 0
    for a0, a1 in spans:
        out.append(t[last:a0])
        out.append(" ")
        last = a1
    out.append(t[last:])
    free_text = re.sub(r"\s+", " ", "".join(out)).strip()

    return free_text, phrases, op

def extract_serial_candidate(text: str) -> str | None:
    if not text:
        return None

    m = LABELED_SERIAL_RE.search(text)
    if m:
        return m.group(1)

    # fallback
    m = FOUR_DIGIT_RE.search(text)
    if m:
        return m.group(1)

    return None

def normalize_serial(serial: str) -> str:
    # since it's digits only, just strip
    return serial.strip()

def _escape_odata_string(value: str) -> str:
    # OData string literal escaping: single quote doubled
    return value.replace("'", "''")

def build_search_in_filter(field: str, values: list[str], delim: str = ",") -> str | None:
    vals = [v for v in (values or []) if v]
    if not vals:
        return None
    # Ensure delimiter not present (or replace it)
    safe_vals = [v.replace(delim, " ") for v in vals]
    joined = delim.join(safe_vals)
    return f"search.in({field}, '{_escape_odata_string(joined)}', '{delim}')"

def _extract_modello_and_matricola(user_text: str) -> tuple[str | None, str | None]:
    t = user_text.upper()

    # Capture patterns like: MC 8, MC8, MC 24HT, MC24HT, etc.
    # Group1: prefix letters, Group2: digits, Group3: optional suffix letters/digits
    modello = None

    # 1) Strong pattern: capture after keyword MODELLO
    m = re.search(
        r"\bMODELLO\s*[:\-]?\s*([A-Z]{2,4}\s*-?\s*\d{1,5}[A-Z0-9]{0,6})\b",
        t
    )
    if m:
        raw = m.group(1)
        raw = re.sub(r"\s+", " ", raw).strip()
        modello = raw

    # 2) Optional fallback: old pattern (but protected), only if not found
    if not modello:
        m = re.search(
            r"\b([A-Z]{2,4})\s*-?\s*(\d{1,5})(?![.,]\d)([A-Z0-9]{0,6})\b",
            t
        )
        if m:
            prefix, num, suffix = m.group(1), m.group(2), m.group(3)
            if suffix:
                modello = f"{prefix}{num}{suffix}"
            else:
                modello = f"{prefix} {num}"

    matricola = None
    m2 = re.search(r"\b(?:MATRICOLA|MATR\.?|NR\.?|N\.|N°|SERIALE|S\/N|SN)\s*[:#]?\s*(\d{2,10})\b", t)
    if m2:
        matricola = m2.group(1)

    return modello, matricola

def escape_lucene_phrase(s: str) -> str:
    # Lucene phrase: escape internal quotes and backslashes
    return s.replace("\\", "\\\\").replace('"', '\\"')

def escape_lucene_text(s: str) -> str:
    # Escape special chars and collapse whitespace
    s = _LUCENE_SPECIAL_RE.sub(r'\\\1', s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def build_lucene_quote_clause_selective(phrases: list[str], op: str) -> str | None:
    if not phrases:
        return None

    groups = []
    for p in phrases:
        if is_code_like(p):
            groups.append(build_code_token_clause(p))
        else:
            # Exact phrase
            groups.append(f"\"{escape_lucene_phrase(p)}\"")

    joiner = f" {op} "
    return "(" + joiner.join(groups) + ")"

def extract_commessa_prefixes(text: str) -> list[str]:
    """
    Extract one or more prefix constraints from phrases like:
    - 'commessa che comincia con 25'
    - 'commesse che iniziano con 25'
    - 'commesse che iniziano con 25 e 26'
    Returns list of numeric prefixes.
    """
    if not text:
        return []

    m = re.search(
        r"\bcommess[ae]\b\s+che\s+"
        r"(?:comincia|cominciano|inizia|iniziano)\s+"
        r"(?:con|per)\s+"
        r"(\d+(?:\s*(?:e|,|and|or|o)\s*\d+)*)",
        text,
        re.IGNORECASE,
    )

    if not m:
        return []

    chunk = m.group(1)
    nums = re.findall(r"\d+", chunk)

    # dedupe preserve order
    out = []
    for n in nums:
        if n not in out:
            out.append(n)

    return out

# --- YEAR -> COMMESSA PREFIX HELPERS -----------------------------------------

# Simple Italian number words (extend if you need more)
_ITALIAN_NUM_WORDS = {
    "uno": 1, "una": 1,
    "due": 2,
    "tre": 3,
    "quattro": 4,
    "cinque": 5,
    "sei": 6,
    "sette": 7,
    "otto": 8,
    "nove": 9,
    "dieci": 10,
    "undici": 11,
    "dodici": 12,
}

_LAST_N_YEARS_RE = re.compile(
    r"\bultim[ioae]\s+(?P<n>\d+|uno|una|due|tre|quattro|cinque|sei|sette|otto|nove|dieci|undici|dodici)\s+anni\b",
    re.IGNORECASE,
)

# Match explicit ranges: "dal 22 al 24", "dal 2022 al 2024", "tra 22 e 24"
_YEAR_RANGE_RE = re.compile(
    r"\b(?:dal|dall'|dalla|da|tra)\s+(?P<a>\d{2}|\d{4})\s+(?:al|a|e)\s+(?P<b>\d{2}|\d{4})\b",
    re.IGNORECASE,
)

# Match single year mentions: "del 25", "nel 2022", "del 2022"
_YEAR_SINGLE_RE = re.compile(
    r"\b(?:del|dell'|della|nel|nell'|nella|anno)\s+(?P<y>\d{2}|\d{4})\b",
    re.IGNORECASE,
)

def _parse_small_int(token: str) -> int | None:
    if not token:
        return None
    t = token.strip().lower()
    if t.isdigit():
        try:
            return int(t)
        except ValueError:
            return None
    return _ITALIAN_NUM_WORDS.get(t)

def _year_to_prefix_2digits(y: int) -> str:
    # commessa starts with last two digits of the year
    return str(y % 100).zfill(2)

def extract_year_prefixes(text: str, today: date | None = None) -> list[str]:
    """
    Extract year constraints from natural language and convert them into
    commessa 2-digit prefixes.

    Supports:
      - "ultimi 5 anni" / "ultimi cinque anni"
      - "dal 22 al 24" / "dal 2022 al 2024" / "tra 22 e 24"
      - "del 25" / "del 2022" / "nel 2022"
    Returns list of 2-digit strings: ["22","23",...]
    """
    if not text:
        return []

    today = today or date.today()
    out: list[str] = []
    seen = set()

    def add_prefix(p: str):
        if p and p.isdigit() and len(p) == 2 and p not in seen:
            seen.add(p)
            out.append(p)

    # 1) "ultimi N anni" => include current year and previous N-1
    m = _LAST_N_YEARS_RE.search(text)
    if m:
        n = _parse_small_int(m.group("n"))
        if n and n > 0:
            start = today.year - (n - 1)
            for yy in range(start, today.year + 1):
                add_prefix(_year_to_prefix_2digits(yy))
            return out  # usually this is the whole intent

    # 2) Explicit ranges "dal 22 al 24", "tra 2022 e 2024"
    for m in _YEAR_RANGE_RE.finditer(text):
        a = int(m.group("a"))
        b = int(m.group("b"))

        # Normalize 2-digit to 2000+ (assumption consistent with your examples)
        if a < 100:
            a = 2000 + a
        if b < 100:
            b = 2000 + b

        if a > b:
            a, b = b, a

        for yy in range(a, b + 1):
            add_prefix(_year_to_prefix_2digits(yy))

    # 3) Single year "del 25" / "del 2022"
    for m in _YEAR_SINGLE_RE.finditer(text):
        y = int(m.group("y"))
        if y < 100:
            y = 2000 + y
        add_prefix(_year_to_prefix_2digits(y))

    return out

def normalize_modello_key(m: str) -> str:
    # Uppercase, remove all non-alnum (spaces, hyphens, etc.)
    m = unicodedata.normalize("NFKC", (m or "")).upper()
    m = re.sub(r"[^A-Z0-9]+", "", m)
    return m

# Common model suffixes that are often glued to the previous letters in normalized keys
_MODEL_SUFFIXES = ("HT", "GT", "EX", "HD", "LR", "ES", "TX", "SK")

def split_suffix_parts(parts: list[str]) -> list[str]:
    """
    Post-process parts extracted by re.findall(r"[A-Z]+|\d+")
    to split known suffixes off the last alpha token.

    Example: ["MC","4","DHT"] -> ["MC","4","D","HT"]
    """
    if not parts:
        return parts

    last = parts[-1]
    if last.isalpha():
        up = last.upper()
        for suf in _MODEL_SUFFIXES:
            if up.endswith(suf) and len(up) > len(suf):
                base = up[: -len(suf)]
                return parts[:-1] + [base, suf]

    return parts

def modello_variants_for_filter(modello: str) -> list[str]:
    """
    Generate common formatting variants that may exist in the index:
    MC4D, MC 4D, MC 4 D, MC-4D, etc.
    """
    key = normalize_modello_key(modello)
    if not key:
        return []

    parts = re.findall(r"[A-Z]+|\d+", key)
    parts = split_suffix_parts(parts)   # <-- ADD THIS LINE
    if not parts:
        return [modello]

    variants = set()

    # Compact: MC4D
    variants.add("".join(parts))

    # Space between groups: "MC 4D" or "MC 4 D" depending on parts
    variants.add(" ".join(parts))

    # Hyphen between first two groups (common): "MC-4D"
    if len(parts) >= 2:
        variants.add(parts[0] + "-" + "".join(parts[1:]))
        # "MC 4D" style: first group + space + rest compact
        variants.add(parts[0] + " " + "".join(parts[1:]))

    # "MC4 D" style: compact up to last + space + last
    if len(parts) >= 3:
        variants.add("".join(parts[:-1]) + " " + parts[-1]) # MC4D HT (for 4 parts this becomes MC4D H? no; with split it’s ok)

    # "MC 4D HT" style: first group + space + compact middle + space + last
    # parts = ["MC","4","D","HT"] -> "MC 4D HT"
    if len(parts) >= 4:
        variants.add(parts[0] + " " + "".join(parts[1:-1]) + " " + parts[-1])   # MC 4D HT

    # return stable order (prefer the one you extracted first)
    ordered = []

    preferred = [
        modello.upper().strip(),                 # original
        "".join(parts),                          # compact
        parts[0] + " " + "".join(parts[1:]),      # "MC 4DHT"
        " ".join(parts),                         # "MC 4 D HT"
    ]

    # If 4+ parts, also prefer "MC 4D HT" style explicitly
    if len(parts) >= 4:
        preferred.append(parts[0] + " " + "".join(parts[1:-1]) + " " + parts[-1])  # "MC 4D HT"

    for v in preferred:
        if v and v in variants and v not in ordered:
            ordered.append(v)

    for v in sorted(variants):
        if v not in ordered:
            ordered.append(v)

    return ordered


def normalize_key(s: str) -> str:
    # keep only letters/digits, uppercase
    return re.sub(r"[^A-Z0-9]", "", s.upper())


def modello_to_ismatch_query(modello: str) -> str | None:
    key = normalize_key(modello)
    if not key:
        return None

    parts = re.findall(r"[A-Z]+|\d+", key)  # ["MC","8","DTH","2"]
    if not parts:
        return None

    # Heuristic: if user ended on digits (or gave short input), treat as a family/prefix
    # and allow suffix continuation by wildcarding the last part.
    last = parts[-1]
    wildcard_last = (len(parts) <= 2) or last.isdigit()

    if wildcard_last:
        parts[-1] = parts[-1] + "*"

    # Simple query syntax: space = AND when searchMode='all'
    return " ".join(parts)

def modello_to_full_query(modello: str) -> Optional[str]:
    """
    Robust Lucene 'full' query for search.ismatch(..., 'modello','full','any').

    Builds ONE query with OR branches:
      1) token AND clause (supports spaced/hyphen tokenization)
      2) compact prefix (supports compact tokens)
      3) hyphen-joined prefix (supports hyphen-as-part-of-token cases)
      4) common mixed hyphen style: first part + '-' + rest compact

    Examples:
      "MC 8" -> (MC AND 8*) OR (MC8*) OR (MC\-8*) OR (MC\-8*)
      "MC 8HT" -> (MC AND 8 AND HT*) OR (MC8HT*) OR (MC\-8\-HT*) OR (MC\-8HT*)
      "GEO 601 W" -> (GEO AND 601 AND W*) OR (GEO601W*) OR (GEO\-601\-W*) OR (GEO\-601W*)
    """
    key = normalize_modello_key(modello)
    if not key:
        return None

    parts = re.findall(r"[A-Z]+|\d+", key)
    if not parts:
        return None

    # Heuristic: family/prefix input -> wildcard last term
    wildcard_last = (len(parts) <= 2) or parts[-1].isdigit()

    def term(i: int) -> str:
        t = escape_lucene_text(parts[i])
        if i == len(parts) - 1 and wildcard_last:
            return t + "*"
        return t

    # 1) Token clause: (MC AND 8* AND HT)
    must_clause = " AND ".join(term(i) for i in range(len(parts)))

    # 2) Compact: (MC8HT*)
    compact = escape_lucene_text("".join(parts)) + "*"

    # 3) Fully hyphen-joined: (MC\-8\-HT*)
    hyphen_all = r"\-".join(escape_lucene_text(p) for p in parts) + "*"

    # 4) Mixed hyphen common: first group + '-' + rest compact, e.g. MC\-8HT*
    if len(parts) >= 2:
        hyphen_first = escape_lucene_text(parts[0]) + r"\-" + escape_lucene_text("".join(parts[1:])) + "*"
    else:
        hyphen_first = None

    clauses = [f"({must_clause})", f"({compact})", f"({hyphen_all})"]
    if hyphen_first:
        clauses.append(f"({hyphen_first})")

    # Dedup preserve order
    out = []
    for c in clauses:
        if c not in out:
            out.append(c)

    return " OR ".join(out)

@dataclass(frozen=True)
class ModelFamily:
    name: str                   # canonical family name
    models: tuple[str, ...]     # the concrete modello values belonging to the family

FAMILIES = (
    ModelFamily(
        name="GEO 205",
        models=("GEO 205", "GEO 205 E"),
    ),
    ModelFamily(
        name="GEO 300",
        models=("GEO 300", "GEO 300 HT", "MC 300"),
    ),
    ModelFamily(
        name="GEO 305",
        models=("GEO 305", "GEO 305 HT", "GEO 305 EX"),
    ),
    ModelFamily(
        name="GEO 405",
        models=("GEO 405", "GEO 405 HT", "CA 0707", "MC 405", "MC 405 A"),
    ),
    ModelFamily(
        name="GEO 500",
        models=("GEO 500", "GEO 500 HT", "MC 500", "MC 500 N"),
    ),
    ModelFamily(
        name="GEO 501",
        models=("GEO 501", "GEO 501 HT", "MC 500 P"),
    ),
    ModelFamily(
        name="GEO 600",
        models=("GEO 600", "GEO 600 HD", "GEO 600 HT", "MC 450"),
    ),
    ModelFamily(
        name="GEO 600 R",
        models=("GEO 600 R", "MC 450 R"),
    ),
    ModelFamily(
        name="GEO 601",
        models=("GEO 601", "GEO 601 A", "GEO 601 HT", "MC 450 P"),
    ),
    ModelFamily(
        name="GEO 601 W",
        models=("GEO 601 W", "GEO 601 W HT"),
    ),
    ModelFamily(
        name="GEO 602",
        models=("GEO 602", "GEO 602 EX", "GEO 602 GT", "GEO 602 HT", "CA 1010", "CA 1012", "CA 1015", "CX 1010", "CX 1012", "CX 1015", "MC 450 P.1"),
    ),
    ModelFamily(
        name="GEO 655",
        models=("GEO 655", "MC 455 GT"),
    ),
    ModelFamily(
        name="GEO 700",
        models=("GEO 700", "GEO 700 A", "GEO 700 GT", "GEO 700 HT"),
    ),
    ModelFamily(
        name="GEO 900",
        models=("GEO 900", "GEO 900 A", "GEO 900 EX", "GEO 900 GT", "GEO 900 GT HT", "GEO 900 HT GT", "GEO 900 HT", "CA 1515", "CA 1520", "CX 1515", "CX 1520", "MC 450 P.2", "MC 900", "MC 900 GT", "MC 900 P"),
    ),
    ModelFamily(
        name="GEO 901",
        models=("GEO 901", "GEO 901 A", "GEO 901 EX", "GEO 901 GT", "GEO 901 GT HT", "GEO 901 HT GT", "GEO 901 HT", "CA 2015", "CA 2020", "CX 2020", "CX 2030", "MC 900 P.1"),
    ),
    ModelFamily(
        name="GEO 903",
        models=("GEO 903", "CA 3030"),
    ),
    ModelFamily(
        name="GEO 905",
        models=("GEO 905", "CA 3550"),
    ),
    ModelFamily(
        name="GEO 909",
        models=("GEO 909", "GEO 909 EX", "GEO 909 GT", "CX 2520"),
    ),
    ModelFamily(
        name="GEO PC",
        models=("GEO PC", "PC 64/75"),
    ),
    ModelFamily(
        name="MC 10",
        models=("MC 10", "MC 10 ES"),
    ),
    ModelFamily(
        name="MC 10 F",
        models=("MC 10 F", "MC 10 F HT"),
    ),
    ModelFamily(
        name="MC 12",
        models=("MC 12", "MC 12 HT", "MC 800", "MC 800 K", "MC 800 S"),
    ),
    ModelFamily(
        name="MC 14",
        models=("MC 14", "MC 14 HT"),
    ),
    ModelFamily(
        name="MC 14 GT",
        models=("MC 14 GT", "MC 14 GT HT", "MC 14 HT GT"),
    ),
    ModelFamily(
        name="MC 15",
        models=("MC 15", "MC 15 HT"),
    ),
    ModelFamily(
        name="MC 15 P",
        models=("MC 15 P", "MC 15 P HT", "MC 15 P GT"),
    ),
    ModelFamily(
        name="MC 16",
        models=("MC 16", "MC 13", "MC 13 F HT"),
    ),
    ModelFamily(
        name="MC 2 D",
        models=("MC 200", "MC 200 P", "MC 200 P SK"),
    ),
    ModelFamily(
        name="MC 20",
        models=("MC 20", "MC 20 A"),
    ),
    ModelFamily(
        name="MC 21",
        models=("MC 21", "MC 1400"),
    ),
    ModelFamily(
        name="MC 22",
        models=("MC 1200", "MC 1200 A", "MC 22 A", "MC 22 A HT", "MC 22 HT"),
    ),
    ModelFamily(
        name="MC 24",
        models=("MC 24", "MC 24 HT"),
    ),
    ModelFamily(
        name="MC 28",
        models=("MC 1200 P", "MC 28", "MC 28 A", "MC 28 A HD", "MC 28 A HT", "MC 28 HD", "MC 28 HT"),
    ),
    ModelFamily(
        name="MC 3 D",
        models=("MC 235", "MC 3 D", "MC 3 D HT"),
    ),
    ModelFamily(
        name="MC 30",
        models=("MC 1500", "MC 30", "MC 30 HT"),
    ),
    ModelFamily(
        name="MC 30 TX",
        models=("MC 1200-2", "MC 30 TX"),
    ),
    ModelFamily(
        name="MC 4 D",
        models=("MC 4 D", "MC 4 D HT", "MC 400", "MC 400 E", "MC 400 P", "MC 400 R P"),
    ),
    ModelFamily(
        name="MC 4 F",
        models=("MC 4 F", "MC 4 F HT"),
    ),
    ModelFamily(
        name="MC 40",
        models=("MC 40", "MC 3000", "MC 3000 S"),
    ),
    ModelFamily(
        name="MC 5 D",
        models=("MC 5 D", "MC 5 D HT"),
    ),
    ModelFamily(
        name="MC 6",
        models=("MC 6", "MC 6 F", "MC 6 HT", "MC 6 F HT", "MC 6 N", "MC 6 S", "MC 6 V"),
    ),
    ModelFamily(
        name="MC 8 D",
        models=("MC 8 D", "MC 8 D HT", "MC 602"),
    ),
    ModelFamily(
        name="MC 8",
        models=("MC 8", "MC 8 HT"),
    ),
    ModelFamily(
        name="MC 9",
        models=("MC 9", "MC 9 N", "MC 9 HT", "MC 9 LR", "MC 9 N HT"),
    ),
    ModelFamily(
        name="MC F 04",
        models=("MC F 04", "MC F 04 L"),
    ),
    ModelFamily(
        name="MC T 10",
        models=("MC T 10", "MC T 10 HT", "MC T 450 P", "CA T 10", "CX T 10", "GEO T 10"),
    ),
    ModelFamily(
        name="MC T 15",
        models=("MC T 15", "CA T 15", "CX T 15", "GEO T 15"),
    ),
    ModelFamily(
        name="MC T 20",
        models=("MC T 20", "MC T 20 EX", "CA T 20", "CX T 20", "GEO T 20", "GEO T 20 GT"),
    ),
    ModelFamily(
        name="MC T 3",
        models=("MC T 3", "MC T 205", "GEO T 3"),
    ),
    ModelFamily(
        name="MC T 30",
        models=("MC T 30", "CA T 30", "CX T 30"),
    ),
    ModelFamily(
        name="MC T 5",
        models=("MC T 5", "MC T 405", "MC T 405 A", "GEO T 5"),
    ),
    ModelFamily(
        name="MC T 50",
        models=("MC T 50", "CA T 50"),
    ),
    ModelFamily(
        name="MC T 7",
        models=("MC T 7", "MC T 450", "GEO T 7"),
    )
)


# Build a fast lookup: normalized modello -> family
_FAMILY_BY_MODEL: dict[str, ModelFamily] = {}
for fam in FAMILIES:
    for m in fam.models:
        _FAMILY_BY_MODEL[normalize_modello_key(m)] = fam

def find_family(user_modello: str) -> Optional[ModelFamily]:
    u = normalize_modello_key(user_modello)
    if not u:
        return None
    return _FAMILY_BY_MODEL.get(u)

def build_modello_family_filter_clause(user_modello: str, logger=None) -> Optional[str]:
    fam = find_family(user_modello)
    if not fam:
        return None

    values: list[str] = []
    seen = set()

    for m in fam.models:
        for v in modello_variants_for_filter(m):
            if v and v not in seen:
                seen.add(v)
                values.append(v)

    if logger:
        logger.warning(
            "family=%s models=%d variants=%d",
            fam.name,
            len(fam.models),
            len(values),
        )

    if not values:
        return None

    # Exact membership filter against the modello field
    or_clause = " or ".join(f"modello eq '{_escape_odata_string(v)}'" for v in values)
    return f"({or_clause})"


def build_phrase_filter(phrases: list[str], op: str, field: str) -> str | None:
    if not phrases:
        return None

    clauses = []
    for p in phrases:
        p = (p or "").strip()
        if not p:
            continue

        # escape for Lucene phrase
        p_lucene = p.replace("\\", "\\\\").replace('"', '\\"')

        clauses.append(
            f"search.ismatch('\"{_escape_odata_string(p_lucene)}\"', '{field}', 'full', 'any')"
        )

    if not clauses:
        return None

    joiner = " and " if op == "AND" else " or "
    return "(" + joiner.join(clauses) + ")"


@dataclass
class SpecsSearchResult:
    context: str
    raw_hits: int
    unique_machines: int

class SearchIndexManager:
    """
    The class for searching of context for user queries.

    :param endpoint: The search endpoint to be used.
    :param credential: The credential to be used for the search.
    :param index_name: The name of an index to get or to create.
    :param dimensions: The number of dimensions in the embedding. Set this parameter only if
                       embedding model accepts dimensions parameter.
    :param model: The embedding model to be used,
                  must be the same as one use to build the file with embeddings.
    :param embeddings_client: The embedding client.
    """
    
    MIN_DIFF_CHARACTERS_IN_LINE = 5
    MIN_LINE_LENGTH = 5

    
    def __init__(
            self,
            endpoint: str,
            # credential: AsyncTokenCredential, # db
            credential: CredentialType, 
            index_name: str,
            dimensions: Optional[int],
            model: str,
            embeddings_client: EmbeddingsClient,
        ) -> None:
        """Constructor."""
        self._dimensions = dimensions
        self._index_name = index_name
        self._embeddings_client = embeddings_client
        self._endpoint = endpoint
        self._credential = credential
        self._index = None
        self._model = model
        self._client = None

    def _get_client(self):
        """Get search client if it is absent."""
        if self._client is None:
            self._client = SearchClient(
                endpoint=self._endpoint, index_name=self._index.name, credential=self._credential)
        return self._client

    async def _collect_all_hits(self, response, max_pages: int = 50) -> list[dict]:
        hits = []
        page_count = 0

        async for page in response.by_page():
            page_count += 1
            async for r in page:
                hits.append(r)

            if page_count >= max_pages:
                break

        return hits

    async def ensure_index_loaded(self) -> None:
        """Load an existing index from Azure AI Search (do not create schema)."""
        async with SearchIndexClient(endpoint=self._endpoint, credential=self._credential) as ix_client:
            self._index = await ix_client.get_index(self._index_name)

    async def search_specs_machines(self, message: ChatRequest) -> SpecsSearchResult:
        """
        Search in rag-specs for matches and return unique machines (latest only).
        Requires fields: commessa, modello, versione, isLatest, section, titolo, text, text_vector
        """

        logger = logging.getLogger("COMMY.search_specs")
        logger.warning(
            "Azure Search Specs call starting. endpoint=%s index=%s credential=%s",
            self._endpoint,
            self._index_name,
            type(self._credential).__name__,
        )

        query_text = (message.messages[-1].content or "").strip()
        if not query_text:
             return SpecsSearchResult(context="", raw_hits=0, unique_machines=0)
        
        title_mode, title_query = parse_title_mode(query_text)
        title_only = title_query is not None

        # If title-only, run the rest of the pipeline on title_query (not the whole prefix)
        _query_text = title_query if title_only else query_text

        # preserve raw (user surface) and normalized (no thousands separators)
        raw_effective_query_text = (_query_text
            .replace("“", '"').replace("”", '"')
            .replace("’", "'")
        )

        # Normalized version used for model/commessa extraction to avoid DI 4.000 -> DI 4
        norm_effective_query_text = normalize_numeric_literals(raw_effective_query_text)

        # Use raw text to extract quoted_phrases (so we keep the user's surface form)
        if raw_effective_query_text.count('"') % 2 == 1:
            # unbalanced quotes: ignore quoting logic entirely
            free_text, quoted_phrases, quoted_op = raw_effective_query_text.strip(), [], None
            phrase_clause = None
        else:
            free_text, quoted_phrases, quoted_op = split_quoted_phrases(raw_effective_query_text)
            quoted_phrases = [p.strip() for p in quoted_phrases if p and p.strip()]
            free_text = free_text.strip()
            phrase_clause = build_lucene_quote_clause_selective(quoted_phrases, quoted_op or "OR")

        quoted_variants: list[list[str]] = [
            expand_thousand_variants(p) for p in quoted_phrases
        ]

        # If we have quoted phrases, we enforce them at commessa-level (across docs),
        # so do NOT put phrase_clause into search_text.
        if quoted_phrases:
            search_text = free_text if free_text else "*"
            query_type = None
        else:
            # no quoted constraints -> keep your original behavior
            if phrase_clause:
                search_text = phrase_clause
                query_type = "full"
            else:
                search_text = query_text
                query_type = None

        logger.warning("free_text=%r", free_text)
        logger.warning("quoted_phrases=%r op=%r", quoted_phrases, quoted_op)

        # --- helper: find commesse that contain a single quoted phrase in ANY document ---
        async def _commesse_for_single_phrase(single_phrase: str, base_filter_expr: str) -> set[str]:
            pf = build_phrase_filter([single_phrase], "OR", "content")
            if not pf:
                return set()

            if base_filter_expr:
                fexpr = f"({base_filter_expr}) and ({pf})"
            else:
                fexpr = pf

            resp = await self._get_client().search(
                search_text="*",           # we drive matching via filter search.ismatch(...)
                query_type="simple",
                filter=fexpr,
                top=1000,                  # tune if needed; usually OK for commessa set building
                select=["commessa"],
            )

            comm = set()
            async for r in resp:
                c = r.get("commessa")
                if c:
                    comm.add(str(c))
            return comm

        async def _commesse_for_phrase_variants(phrase: str, base_filter_expr: str) -> set[str]:
            comm = set()
            for v in expand_thousand_variants(phrase):
                comm |= await _commesse_for_single_phrase(v, base_filter_expr)
            return comm

        effective_query_text_for_extraction = norm_effective_query_text

        # 1) Extract hard constraints from the user's message
        modello, matricola = _extract_modello_and_matricola(effective_query_text_for_extraction)
        commesse = extract_commesse(effective_query_text_for_extraction)
        # Prefix constraints explicitly asked as "commesse che iniziano con 25"
        commessa_prefixes = extract_commessa_prefixes(effective_query_text_for_extraction)
        # Year constraints like "del 2022", "del 25", "ultimi 5 anni", "dal 22 al 24"
        year_prefixes = extract_year_prefixes(effective_query_text_for_extraction)
        machine_intent = infer_machine_intent(effective_query_text_for_extraction)


        # 2) Build embedding for vector search
        embedded_question = (await self._embeddings_client.embed(
            input=effective_query_text_for_extraction,
            dimensions=self._dimensions,
            model=self._model
        ))["data"][0]["embedding"]

        k = 200 if quoted_phrases else 20
        vector_query = VectorizedQuery(
            vector=embedded_question,
            k_nearest_neighbors=k,
            fields="text_vector"
        )

        # 3) Build filter (hard constraints)
        base_filters = ["isLatest eq true"]
        modello_filters = []
        other_filters = []

        fam = None
        family_values: list[str] = []   # keep for debugging/possible reuse
        if modello and machine_intent:
            # If model belongs to a known family, expand to family values
            fam = build_modello_family_filter_clause(modello, logger)

        if fam:
            # fam is an OData OR string today; better: rebuild membership list and use search.in
            seen = set()
            fam_obj = find_family(modello)
            if fam_obj:
                for m in fam_obj.models:
                    for v in modello_variants_for_filter(m):
                        if v and v not in seen:
                            seen.add(v)
                            family_values.append(v)

            in_clause = build_search_in_filter("modello", family_values)
            if in_clause:
                modello_filters.append(in_clause)
            else:
                modello = None

        else:
            if modello:
                # Non-family: variants just from extracted modello
                vars_ = modello_variants_for_filter(modello)
                in_clause = build_search_in_filter("modello", vars_)
                if in_clause:
                    modello_filters.append(in_clause)
                else:
                    modello = None
            else:
                modello = None
        # NOTE: this requires you to have a dedicated field in the index, e.g. `matricola` filterable
        if matricola:
            other_filters.append(f"matricola eq '{_escape_odata_string(matricola)}'")
        # Exact commesse have priority
        if commesse:
            if len(commesse) == 1:
                other_filters.append(f"commessa eq '{_escape_odata_string(commesse[0])}'")
            else:
                or_clause = " or ".join(
                    f"commessa eq '{_escape_odata_string(c)}'" for c in commesse
                )
                other_filters.append(f"({or_clause})")

        else:
            # Combine explicit prefix constraints + year-derived prefixes
            combined_prefixes = []
            for p in (commessa_prefixes + year_prefixes):
                p = (p or "").strip()
                if p.isdigit() and p not in combined_prefixes:
                    combined_prefixes.append(p)

            if combined_prefixes:
                pref_clause = commessa_prefixes_to_filter(combined_prefixes)
                if pref_clause:
                    other_filters.append(pref_clause)

        # # Final filter expressions
        # Final filter expressions (BASE filters, no quoted phrase constraints yet)
        filters_no_phrase = base_filters + modello_filters + other_filters
        filter_expr_no_phrase = " and ".join(filters_no_phrase)

        # relaxed base filters (used in your retry logic)
        filters_relaxed_no_phrase = base_filters + other_filters
        filter_expr_relaxed = " and ".join(filters_relaxed_no_phrase)

        # --- quoted phrases across multiple documents with same commessa ---
        commessa_scope: list[str] | None = None     # <-- SPOSTATO QUI, PRIMA DI OGNI USO
        phrase_sets: list[set[str]] = []            # <-- SPOSTATO QUI, PRIMA DI OGNI US

        if quoted_phrases:
            for p in quoted_phrases:
                s = await _commesse_for_phrase_variants(p, filter_expr_no_phrase)
                phrase_sets.append(s)

            op = quoted_op or "OR"
            if op == "AND":
                comm_set = set.intersection(*phrase_sets) if phrase_sets else set()
            else:
                comm_set = set.union(*phrase_sets) if phrase_sets else set()

            if not comm_set:
                return SpecsSearchResult(context="", raw_hits=0, unique_machines=0)

            commessa_scope = sorted(comm_set)

        logger.debug("phrase_sets sizes: %s", [len(s) for s in phrase_sets])
        logger.debug("commessa_scope (len=%d): %s", len(commessa_scope or []), commessa_scope)

        # now it's safe to use commessa_scope
        filter_expr_relaxed_final = filter_expr_relaxed
        if commessa_scope:
            if len(commessa_scope) == 1:
                filter_expr_relaxed_final = f"({filter_expr_relaxed}) and (commessa eq '{_escape_odata_string(commessa_scope[0])}')"
            else:
                or_clause = " or ".join(f"commessa eq '{_escape_odata_string(c)}'" for c in commessa_scope)
                filter_expr_relaxed_final = f"({filter_expr_relaxed}) and ({or_clause})"

        search_fields = ["titolo"] if title_only else ["content"]
        search_mode = "all" if title_mode == "all" else "any"

        # 4) Run hybrid search but restrict BM25 to content only
        # Build final filter: base filters + (optional) commessa_scope
        final_filters = filters_no_phrase[:]  # start from base constraints

        if commessa_scope:
            if len(commessa_scope) == 1:
                final_filters.append(f"commessa eq '{_escape_odata_string(commessa_scope[0])}'")
            else:
                or_clause = " or ".join(
                    f"commessa eq '{_escape_odata_string(c)}'" for c in commessa_scope
                )
                final_filters.append(f"({or_clause})")

        filter_expr = " and ".join(final_filters)

        logger.warning(
            ">>> extracted modello=%r matricola=%r commesse=%r commessa_prefixes=%r year_prefixes=%r commessa_scope=%r filter=%s <<<",
            modello, matricola, commesse, commessa_prefixes, year_prefixes, commessa_scope, filter_expr
        )

        def _build_codelike_regex(phrase: str) -> re.Pattern:
            # Build a forgiving regex that matches code with optional separators between parts
            parts = code_parts(phrase)
            if not parts:
                return re.compile(re.escape(phrase), re.IGNORECASE)

            sep = r"[\s\-_\/\.\(\)]*"  # separators you often have in docs
            pat = sep.join(re.escape(p) for p in parts)
            return re.compile(pat, re.IGNORECASE)

        def _tokenize_for_fuzzy_phrase(p: str) -> list[str]:
            # Keep digits/letters as tokens; collapse the rest.
            p = (p or "").strip()
            if not p:
                return []
            # normalize quotes and spaces
            p = p.replace("“", '"').replace("”", '"').replace("\u00A0", " ")
            # split on whitespace first, but keep tokens like 5.200, 5,200, 5200 together
            return [t for t in re.split(r"\s+", p) if t]

        def _build_phrase_fuzzy_regex(phrase: str) -> re.Pattern:
            """
            Build a forgiving regex that matches a phrase even if:
            - whitespace differs (incl NBSP)
            - punctuation/separators appear between tokens
            - thousand separators in integers differ: 5200 / 5.200 / 5,200
            """
            toks = _tokenize_for_fuzzy_phrase(phrase)
            if not toks:
                return re.compile(r"(?!x)x")  # never matches

            sep = r"(?:[\s\u00A0\-_\/\.,;:\(\)\[\]]+)*"  # allow lots of separators between tokens

            def tok_to_pat(tok: str) -> str:
                # If token is an integer (possibly with thousands separators), allow all variants
                plain = re.sub(r"[.,]", "", tok)
                if plain.isdigit() and len(plain) >= 4:
                    # allow: 5200, 5.200, 5,200, 52 00 (rare but seen), etc.
                    # We'll accept separators between digit groups.
                    # Build from the plain number: 5200 -> 5[sep]?200 style
                    # Simpler: allow [.,\s] between any digits but not required.
                    digits = list(plain)
                    return r"".join(re.escape(d) + r"(?:[.,\s\u00A0]?)*" for d in digits).rstrip(r"(?:[.,\s\u00A0]?)*")
                else:
                    return re.escape(tok)

            pat = sep.join(tok_to_pat(t) for t in toks)
            return re.compile(pat, re.IGNORECASE)

        def _snip_around(text: str, start: int, end: int, window: int = 140) -> str:
            a = max(0, start - window)
            b = min(len(text), end + window)
            left = "…" if a > 0 else ""
            right = "…" if b < len(text) else ""
            return (left + text[a:b].strip() + right).replace("\n", " ")

        def extract_match_snippets(
            titolo: str,
            content: str,
            phrases: list[str],
            max_snips: int = 3
        ) -> list[str]:
            """
            Return up to max_snips snippets from content around phrase matches.

            Improvements:
            - tolerant to NBSP/weird spaces
            - tolerant to thousand separators (5200 / 5.200 / 5,200)
            - tolerant to punctuation between tokens
            - uses best span found in raw content to center snippet
            """
            raw = (content or "").strip()
            if not raw or not phrases:
                return []

            snippets: list[str] = []
            seen = set()

            # small optimization: work on raw once
            raw_for_exact = raw
            raw_for_exact_low = raw_for_exact.lower()

            for phrase in phrases:
                p = (phrase or "").strip()
                if not p:
                    continue

                # 1) code-like: keep your existing tolerant regex
                if is_code_like(p):
                    rx = _build_codelike_regex(p)
                    m = rx.search(raw)
                    if not m:
                        continue

                    sn = _snip_around(raw, m.start(), m.end(), window=140)
                    key = sn.lower()
                    if key not in seen:
                        seen.add(key)
                        snippets.append(sn)
                    if len(snippets) >= max_snips:
                        break
                    continue

                # 2) exact substring (case-insensitive) – fast path
                idx = raw_for_exact_low.find(p.lower())
                if idx >= 0:
                    start = idx
                    end = idx + len(p)
                    sn = _snip_around(raw, start, end, window=140)
                    key = sn.lower()
                    if key not in seen:
                        seen.add(key)
                        snippets.append(sn)
                    if len(snippets) >= max_snips:
                        break
                    continue

                # 3) fuzzy regex (spacing, separators, thousands)
                rx = _build_phrase_fuzzy_regex(p)
                m = rx.search(raw)
                if not m:
                    # last-chance: canonical compare without span mapping
                    # (keeps your previous behavior of skipping if we can't locate span)
                    continue

                sn = _snip_around(raw, m.start(), m.end(), window=140)
                key = sn.lower()
                if key not in seen:
                    seen.add(key)
                    snippets.append(sn)

                if len(snippets) >= max_snips:
                    break

            return snippets

        try:
            response = await self._get_client().search(
                search_text=search_text,                # BM25 query
                query_type=query_type,                  # None => simple syntax; "full" => Lucene
                search_fields=search_fields,
                search_mode=search_mode,                # 👈 required
                vector_queries=[vector_query],          # vector query
                filter=filter_expr,                     # hard constraints
                top=200,
                select=["id", "commessa", "modello", "matricola", "versione", "section", "titolo", "content"],
                highlight_fields="content",
                highlight_pre_tag="⟦",
                highlight_post_tag="⟧",
            )
        except Exception:
            logger.exception(">>> Azure Search call failed <<<")
            return SpecsSearchResult(context="", raw_hits=0, unique_machines=0)

        hits = []
        try:
            # async for r in response:
            #     hits.append(r)
            hits = await self._collect_all_hits(response, max_pages=50)
        except Exception:
            logger.exception(">>> Iterating search results failed <<<")
            return SpecsSearchResult(context="", raw_hits=0, unique_machines=0)

        # ✅ ADD THIS HERE: raw retrieval diagnostics 
        logger.warning(
            "raw hits=%d distinct_modelli=%d sample_modelli=%s",
            len(hits),
            len({(h.get("modello") or "") for h in hits}),
            sorted({(h.get("modello") or "") for h in hits})[:50],
        )

        if quoted_phrases:
            # Keep only docs that actually contain at least one quoted phrase,
            # so evidenza is always meaningful.
            def _doc_has_any_phrase(h) -> bool:
                titolo = (h.get("titolo") or "")
                content = (h.get("content") or "")
                raw = f"{titolo}\n{content}"

                raw_c = _canon_for_match(raw)

                for variants in quoted_variants:
                    for p in variants:
                        # canonicalize the phrase too
                        p_c = _canon_for_match(p)
                        if not p_c:
                            continue
                        if p_c.lower() in raw_c.lower():
                            return True
                return False

            hits = [h for h in hits if _doc_has_any_phrase(h)]

        if not hits and modello:
            # retry without filter
            response1 = await self._get_client().search(
                search_text=search_text,
                query_type=query_type,
                search_fields=search_fields,
                search_mode=search_mode,   # 👈 required
                vector_queries=[vector_query],
                filter=filter_expr_relaxed_final,                     # weaker constraints
                top=200,
                select=["id", "commessa", "modello", "matricola", "versione", "section", "titolo", "content"],
                highlight_fields="content",
                highlight_pre_tag="⟦",
                highlight_post_tag="⟧",
            )

            async for r in response1:
                hits.append(r)

        # 5) Deduplicate by machine (commessa, modello, matricola, versione)
        machines: dict[tuple[str, str, str, int], list[dict]] = {}
        for h in hits:
            commessa = h.get("commessa")
            modello_h = h.get("modello")
            matricola_h = h.get("matricola")
            versione = h.get("versione")
            if commessa is None or modello_h is None or versione is None:
                continue

            key = (str(commessa), str(modello_h), str(matricola_h), int(versione))

            section = (h.get("section") or "").strip()
            titolo = (h.get("titolo") or "").strip()
            content = (h.get("content") or "").strip()

            # Build evidence snippets around the quoted phrases (if any)
            flat_phrase_variants = [v for variants in quoted_variants for v in variants]
            snips = extract_match_snippets(titolo, content, flat_phrase_variants, max_snips=3)

            raw = f"{titolo}\n{content}"
            raw_c = _canon_for_match(raw).lower()

            matched = []
            for variants in quoted_variants:
                hit_any = False
                for p in variants:
                    p_c = _canon_for_match(p).lower()
                    if p_c and p_c in raw_c:
                        hit_any = True
                        break
                if hit_any:
                    matched.append(variants[0])

            if quoted_phrases and not matched:
                logger.warning(
                    "SKIP no matched phrase. titolo=%r content_preview=%r quoted=%r",
                    (titolo or "")[:80],
                    (content or "")[:120].replace("\n", " "),
                    quoted_phrases,
                )
                continue

            evidence_text = "\n    ".join(snips) if snips else content.replace("\n", " ")[:200]

            machines.setdefault(key, [])

            # Avoid duplicates by document id (best) or by titolo if id not selected
            doc_id = h.get("id") or ""   # <-- IMPORTANT: you must include "id" in select=[...] below
            already_ids = {e.get("id") for e in machines[key]}

            if doc_id and doc_id in already_ids:
                continue

            machines[key].append({
                "id": doc_id,
                "section": section,
                "titolo": titolo,
                "text": evidence_text,
                "matched_phrases": matched,
            })
                    
        logger.warning("unique machines after dedupe: %d", len(machines))

        for k, evs in machines.items():
            logger.warning("DEBUG evidences for %r: %s", k, [e.get("titolo") for e in evs])

        if not machines:
            return SpecsSearchResult(context="", raw_hits=len(hits), unique_machines=0)

        def _matricola_to_int(m):
            try:
                return int(m)
            except (TypeError, ValueError):
                return -1

        def _sort_key(kv):
            (commessa, modello_out, matricola_out, versione), _ = kv
            return (
                str(commessa),                      # primary
                _matricola_to_int(matricola_out),   # primary
                str(modello_out),                   # tie-breaker
                int(versione),                      # tie-breaker
            )

        sorted_items = sorted(machines.items(), key=_sort_key, reverse=True)

        # Build compact context for LLM
        parts = []
        for (commessa, modello_out, matricola_out, versione), evidences in sorted_items:
            evidences.sort(key=lambda e: len(e.get("matched_phrases") or []), reverse=True)
            evidences = evidences[:5]
            parts.append(
                "MACHINE\n"
                f"commessa: {commessa}\n"
                f"modello: {modello_out}\n"
                f"matricola: {matricola_out}\n"
                f"versione: {versione}\n"
                "evidenza:\n" +
                "\n".join(
                    f"- section: {e['section'] or 'N/A'}\n"
                    f"  titolo: {e['titolo'] or 'N/A'}\n"
                    f"  text: {e['text']}"
                    for e in evidences
                )
            )

        context = "\n\n---\n\n".join(parts)
        logger.info(
            "Azure Search Specs call: machines=%d hits=%d filter=%s modello=%s matricola=%s",
            len(machines), len(hits), filter_expr, modello, matricola
        )

        return SpecsSearchResult(
            context=context,
            raw_hits=len(hits),
            unique_machines=len(machines),
        )

    async def search(self, message: ChatRequest) -> str:
        """
        Search the message in the vector store.

        :param message: The customer question.
        :return: The context for the question.
        """

        logger = logging.getLogger("COMMY.search")
        logger.warning(
            "Azure Search call starting. endpoint=%s index=%s credential=%s",
            self._endpoint,
            self._index_name,
            type(self._credential).__name__,
        )

        logger.warning(
            "AZURE env vars: TENANT=%s CLIENT_ID=%s MSI=%s",
            bool(os.getenv("AZURE_TENANT_ID")),
            bool(os.getenv("AZURE_CLIENT_ID")),
            bool(os.getenv("MSI_ENDPOINT") or os.getenv("IDENTITY_ENDPOINT")),
        )

        self._raise_if_no_index()

        query_text = (message.messages[-1].content or "").strip()

        # 1) Extract + normalize serial
        serial_candidate = extract_serial_candidate(query_text)
        serial = normalize_serial(serial_candidate) if serial_candidate else None
        filter_expr = None
        if serial:
            serial_escaped = serial.replace("'", "''")
            filter_expr = f"serialNumber eq '{serial_escaped}'"

        logger.info('RAG query="%s" serial_candidate=%r serial=%r filter=%r',
                    query_text[:200].replace("\n", " "),
                    serial_candidate, serial, filter_expr)        

        # 2) Embedding for vector query
        embedded_question = (await self._embeddings_client.embed(
            input=query_text,
            dimensions=self._dimensions,
            model=self._model
        ))['data'][0]['embedding']
        vector_query = VectorizedQuery(vector=embedded_question, k_nearest_neighbors=20, fields="text_vector") # increased from 5 to 20

        try:
            # 3) First attempt: hybrid + optional filter
            response = await self._get_client().search(
                search_text=query_text,             # <-- this is the key change
                vector_queries=[vector_query],
                filter=filter_expr,                 # <-- key line
                top=10,                             # return more candidates
                select=["chunk", "title", "serialNumber", "model", "parent_id"],     # for debugging + citations
            )

            # Collect results (keep docs, not just chunk text)
            docs = []
            async for r in response:
                docs.append(r)

            logger.info("Search returned %d docs (filter=%r)", len(docs), filter_expr)

            # 4) if filter was used and it produced nothing, no fallback
            if filter_expr and len(docs) == 0:
                logger.warning("0 docs with filter %r. Retrying without filter.", filter_expr)
                return ""

            # 5) Build context string
            # results = [result['chunk'] async for result in response]      # commented to allow hybrid search
            context_parts = []
            for i, d in enumerate(docs[:10], start=1):
                title = d.get("title") or "(no title)"
                serial = d.get("serialNumber") or "(no serial)"
                model = d.get("model") or "(no model)"
                chunk = d.get("chunk") or ""

                logger.info(
                    'HIT %d title="%s" serial="%s" chunkLen=%d',
                    i, title, serial, len(chunk)
                )

                if chunk.strip():
                    context_parts.append(
                        f"Manual: {title}\n"
                        f"Serial number: {serial}\n"
                        f"Model: {model}\n"
                        f"Content:\n{chunk}"
                    )

            context = "\n\n---\n\n".join(context_parts)
            logger.info("Built contextParts=%d totalLen=%d", len(context_parts), len(context))

            # return "\n------\n".join(results)                             # commented to allow hybrid search
            return context
        
        except HttpResponseError as e:
            # Basic info
            logger.error("Azure Search request failed")
            logger.error("Status code: %s", getattr(e, "status_code", None))
            logger.error("Exception message: %s", str(e))

            # 🔴 THIS IS THE IMPORTANT PART
            if getattr(e, "response", None) is not None:
                try:
                    logger.error("Azure Search response status: %s", e.response.status_code)
                except Exception:
                    pass

                try:
                    logger.error("Azure Search response headers: %s", dict(e.response.headers))
                except Exception:
                    pass

                try:
                    # Azure SDK response body is NOT async
                    logger.error("Azure Search response body: %s", e.response.text())
                except Exception as ex:
                    logger.error("Could not read response body: %s", ex)

            raise

    async def upload_documents(self, embeddings_file: str) -> None:
        """
        Upload the embeggings file to index search.

        :param embeddings_file: The embeddings file to upload.
        """
        self._raise_if_no_index()
        documents = []
        index = 0
        with open(embeddings_file, newline='') as fp:
            reader = csv.DictReader(fp)
            for row in reader:
                documents.append(
                    {
                        'embedId': str(index),
                        'token': row['token'],
                        'embedding': json.loads(row['embedding'])
                    }
                )
                index += 1
        await self._get_client().upload_documents(documents)

    async def is_index_empty(self) -> bool:
        """
        Return True if the index is empty.

        :return: True f index is empty.
        """
        if self._index is None:
            raise ValueError(
                "Unable to perform the operation as the index is absent. "
                "To create index please call create_index")
        document_count = await self._get_client().get_document_count()
        return document_count == 0

    def _raise_if_no_index(self) -> None:
        """
        Raise the exception if the index was not created.

        :raises: ValueError
        """
        if self._index is None:
            raise ValueError(
                "Unable to perform the operation as the index is absent. "
                "To create index please call create_index")

    async def delete_index(self):
        """Delete the index from vector store."""
        self._raise_if_no_index()
        async with SearchIndexClient(endpoint=self._endpoint, credential=self._credential) as ix_client:
            await ix_client.delete_index(self._index.name)
        self._index = None

    def _check_dimensions(self, vector_index_dimensions: Optional[int] = None) -> int:
        """
        Check that the dimensions are set correctly.

        :return: the correct vector index dimensions.
        :raises: Value error if both dimensions of embedding model and vector_index_dimensions are not set
                 or both of them set and they do not equal each other.
        """
        if vector_index_dimensions is None:
            if self._dimensions is None:
                raise ValueError(
                    "No embedding dimensions were provided in neither dimensions in the constructor nor in vector_index_dimensions"
                    "Dimensions are needed to build the search index, please provide the vector_index_dimensions.")
            vector_index_dimensions = self._dimensions
        if self._dimensions is not None and vector_index_dimensions != self._dimensions:
            raise ValueError("vector_index_dimensions is different from dimensions provided to constructor.")
        return vector_index_dimensions

    async def ensure_index_created(self, vector_index_dimensions: Optional[int] = None) -> None:
        """
        Get the search index. Create the index if it does not exist.

        :param vector_index_dimensions: The number of dimensions in the vector index. This parameter is
               needed if the embedding parameter cannot be set for the given model. It can be
               figured out by loading the embeddings file, generated by build_embeddings_file,
               loading the contents of the first row and 'embedding' column as a JSON and calculating
               the length of the list obtained.
               Also please see the embedding model documentation
               https://platform.openai.com/docs/models#embeddings
        :raises: Value error if both dimensions of embedding model and vector_index_dimensions are not set
                 or both of them set and they do not equal each other.
        """
        vector_index_dimensions = self._check_dimensions(vector_index_dimensions)
        if self._index is None:
            self._index = await SearchIndexManager.get_or_create_index(
                self._endpoint,
                self._credential,
                self._index_name,
                vector_index_dimensions)

    @staticmethod
    async def index_exists(
        endpoint: str,
        credential: AsyncTokenCredential,
        index_name: str) -> bool:
        """
        Check if index exists.

        :param endpoint: The search end point to be used.
        :param credential: The credential to be used for the search.
        :param index_name: The name of an index to get or to create.
        :return: True if index already exists.
        """
        exists = False
        async with SearchIndexClient(endpoint=endpoint, credential=credential) as ix_client:
            try:
                await ix_client.get_index(index_name)
                exists = True
            except ResourceNotFoundError:
                pass
        return exists

    @staticmethod
    async def get_or_create_index(
            endpoint: str,
            credential: AsyncTokenCredential,
            index_name: str,
            dimensions: int,
        ) -> SearchIndex:
        """
        Get o create the search index.

        **Note:** If the search index with index_name exists, the embeddings_file will not be uploaded.
        :param endpoint: The search end point to be used.
        :param credential: The credential to be used for the search.
        :param index_name: The name of an index to get or to create.
        :param dimensions: The number of dimensions in the embedding.
        :return: the search index object.
        """
        index = None
        async with SearchIndexClient(endpoint=endpoint, credential=credential) as ix_client:
            try:
                index = await ix_client.get_index(index_name)
            except ResourceNotFoundError:
                pass
        if index is None:
            index = await SearchIndexManager._index_create(
                endpoint=endpoint,
                credential=credential,
                index_name=index_name,
                dimensions=dimensions
            )
        return index

    async def create_index(
        self,
        vector_index_dimensions: Optional[int] = None) -> bool:
        """
        Create index or return false if it already exists.

        :param vector_index_dimensions: The number of dimensions in the vector index. This parameter is
               needed if the embedding parameter cannot be set for the given model. It can be
               figured out by loading the embeddings file, generated by build_embeddings_file,
               loading the contents of the first row and 'embedding' column as a JSON and calculating
               the length of the list obtained.
               Also please see the embedding model documentation
               https://platform.openai.com/docs/models#embeddings
        :return: True if index was created, False otherwise.
        :raises: Value error if both dimensions of embedding model and vector_index_dimensions are not set
                 or both of them are set and they do not equal each other.
        """
        vector_index_dimensions = self._check_dimensions(vector_index_dimensions)
        try:
            self._index = await SearchIndexManager._index_create(
                endpoint=self._endpoint,
                credential=self._credential,
                index_name=self._index_name,
                dimensions=vector_index_dimensions
            )
            return True
        except HttpResponseError:
            return False
        

    @staticmethod
    async def _index_create(
        endpoint: str,
        credential: AsyncTokenCredential,
        index_name: str,
        dimensions: int) -> SearchIndex:
        """Create the index."""
        async with SearchIndexClient(endpoint=endpoint, credential=credential) as ix_client:
            fields = [
                SimpleField(name="embedId", type=SearchFieldDataType.String, key=True),
                SearchField(
                    name="embedding",
                    type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                    vector_search_dimensions=dimensions,
                    searchable=True,
                    vector_search_profile_name="embedding_config"
                ),
                SimpleField(name="token", type=SearchFieldDataType.String, hidden=False),
            ]
            vector_search = VectorSearch(
                profiles=[VectorSearchProfile(name="embedding_config",
                                              algorithm_configuration_name="embed-algorithms-config")],
                algorithms=[HnswAlgorithmConfiguration(name="embed-algorithms-config")],
            )
            search_index = SearchIndex(name=index_name, fields=fields, vector_search=vector_search)
            new_index = await ix_client.create_index(search_index)
        return new_index
        

    async def build_embeddings_file(
            self,
            input_directory: str,
            output_file: str,
            sentences_per_embedding: int=4
            ) -> None:
        """
        In this method we do lazy loading of nltk and download the needed data set to split

        document into tokens. This operation takes time that is why we hide import nltk under this
        method. We also do not include nltk into requirements because this method is only used
        during rag generation.
        :param dimensions: The number of dimensions in the embeddings. Must be the same as
               the one used for SearchIndexManager creation.
        :param input_directory: The directory with the embedding files.
        :param output_file: The file csv file to store embeddings.
        :param embeddings_client: The embedding client, used to create embeddings. 
                Must be the same as the one used for SearchIndexManager creation.
        :param sentences_per_embedding: The number of sentences used to build embedding.
        :param model: The embedding model to be used.
        """
        import nltk
        nltk.download('punkt')
        
        from nltk.tokenize import sent_tokenize
        # Split the data to sentence tokens.
        sentence_tokens = []
        globs = glob.glob(input_directory + '/*.md', recursive=True)
        index = 0
        for fle in globs:
            with open(fle) as f:
                for line in f:
                    line = line.strip()
                    # Skip non informative lines.
                    if len(line) < SearchIndexManager.MIN_LINE_LENGTH or len(set(line)) < SearchIndexManager.MIN_DIFF_CHARACTERS_IN_LINE:
                        continue
                    for sentence in sent_tokenize(line):
                        if index % sentences_per_embedding == 0:
                            sentence_tokens.append(sentence)
                        else:
                            sentence_tokens[-1] += ' '
                            sentence_tokens[-1] += sentence
                        index += 1
        
        # For each token build the embedding, which will be used in the search.
        batch_size = 2000
        with open(output_file, 'w') as fp:
            writer = csv.DictWriter(fp, fieldnames=['token', 'embedding'])
            writer.writeheader()
            for i in range(0, len(sentence_tokens), batch_size):
                emedding = (await self._embeddings_client.embed(
                    input=sentence_tokens[i:i+min(batch_size, len(sentence_tokens))],
                    dimensions=self._dimensions,
                    model=self._model
                ))["data"]
                for token, float_data in zip(sentence_tokens, emedding):
                    writer.writerow({'token': token, 'embedding': json.dumps(float_data['embedding'])})


    async def close(self) -> None:
        client, self._client = self._client, None
        if client is not None:
            try:
                await client.close()
            except Exception:
                logging.getLogger("COMMY").exception("Failed closing SearchClient for index=%s", self._index_name)
