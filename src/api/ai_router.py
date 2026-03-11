# api/ai_router.py
import json
import re
from typing import Any, Dict, Optional, List, Tuple

from azure.ai.inference.models import SystemMessage, UserMessage, JsonSchemaFormat
from azure.ai.inference.aio import ChatCompletionsClient
from azure.core.exceptions import HttpResponseError


ROUTER_SYSTEM = """
You are a ROUTER for a database-backed assistant.
Your job is ONLY to output a JSON object that matches the schema.
Never output SQL. Never invent new intents.

Pick exactly one intent from:
- nuova_ricerca
- search
- macchine
- commesse
- peso
- alternativa
- kit

Rules:
- If user asks reset/restart/new search -> intent "nuova_ricerca".
- If user asks for machines -> "macchine" and extract field + id.
- If user asks commesse -> "commesse" and extract field + id.
- If user asks peso -> "peso" and extract field + id (usually modello/macchina).
- If user asks alternativa per ConfigurazioneID -> "alternativa" and extract id + optional flow_rate (l/min) + volume (cc).
- If user asks kit -> "kit" and extract field+id + optional keywords list.
- Otherwise -> "search" and extract rpm/torque/flow/rotary_type/motor_type/machine_model when present.
- If uncertain, choose best intent but set low confidence.

Normalization rules:
- Recognize misspellings of key words: rotary, configurazioneid, rotaryid, portata, coppia, giri, rpm, commesse, macchine, alternativa, kit.
- Rotary type:
  - If user mentions "R" followed by digits (e.g. R1400) -> rotary_type="R1400".
  - If user mentions "rotary 1400" or "testa 1400" -> treat as "R1400".
- Units:
  - Flow: interpret l/min, lmin, lpm as flow (l/min).
  - Torque: interpret Nm, daNm, kNm; output as daNm (convert if needed) OR output as Nm (pick one and be consistent).
  - RPM: interpret rpm, giri/min, giri as rpm.
- Extract numbers even if punctuation is Italian: "150,5" -> 150.5

For intent "kit":
- field must be "rotaryid" or "configurazioneid"
- Accept user forms like:
  - "kit per rotary 150, 'x', 'y'"
  - "kit rotaryid 150 keywords: x, y"
  - "kit per configurazioneid 12 con albero flottante"
- Extract keywords from:
  - quoted strings "..."
  - comma/semicolon separated tokens after the id
  - phrases after "con", "keywords", "keyword", "accessori"
- If no explicit keywords are provided, set keywords=[].
""".strip()


# JSON schema (strict) for the router output
INTENT_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["intent", "confidence"],
    "properties": {
        "intent": {
            "type": "string",
            "enum": ["nuova_ricerca", "search", "macchine", "commesse", "peso", "alternativa", "kit"],
        },
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},

        # common for macchine/commesse/peso/kit/alternativa
        "field": {
            "type": ["string", "null"],
            "enum": ["rotaryid", "configurazioneid", "rotary", "configurazione", "modello", "macchina", "testa", "matricola", None]
        },
        "id": {"type": ["string", "null"]},

        # for search
        "rpm": {"type": ["number", "null"]},
        "torque": {"type": ["number", "null"]},
        "flow": {"type": ["number", "null"]},
        "rotary_type": {"type": ["string", "null"]},
        "motor_type": {"type": ["string", "null"]},
        "machine_model": {"type": ["string", "null"]},

        # for alternativa
        "flow_rate": {"type": ["number", "null"]},
        "volume": {"type": ["number", "null"]},

        # for kit
        "keywords": {
            "type": ["array", "null"],
            "items": {"type": "string"},
        },
    },
}


# -----------------------------
# Deterministic KIT parsing
# -----------------------------

_KIT_HEAD_RE = re.compile(
    r"\bkit\b.*?\b(per\s+)?(?P<field>rotaryid|configurazioneid)\s+(?P<id>\d+)\b",
    re.I,
)

def _extract_kit_quoted_phrases(text: str, seps={",", ";"}) -> Tuple[str, List[str]]:
    """
    Returns (text_without_quoted_spans, phrases).

    Kit-specific quoting rules:
    - Quotes delimit keyword phrases.
    - Inside a quoted phrase:
        - ""  -> literal "
        - a " closes ONLY if next non-space is a separator (comma/semicolon) or end-of-string
        - otherwise " is treated as literal (inches), e.g. "3"1/2" -> 3"1/2
    - Lenient: if input ends while still in quotes, accept phrase up to end-of-string.
    """
    t = text.replace("“", '"').replace("”", '"').replace("’", "'")
    n = len(t)
    phrases: List[str] = []
    spans: List[Tuple[int, int]] = []

    def next_nonspace_idx(j: int) -> int:
        j += 1
        while j < n and t[j].isspace():
            j += 1
        return j

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

                # boundary close only if next non-space is sep or end
                nxt = next_nonspace_idx(i)
                if nxt >= n or t[nxt] in seps:
                    closed = True
                    i += 1
                    break

                # otherwise it's a literal inches quote inside the keyword
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
            break  # unterminated: we consumed to end-of-string

    # remove quoted spans from free text
    out: List[str] = []
    last = 0
    for a0, a1 in spans:
        out.append(t[last:a0])
        out.append(" ")
        last = a1
    out.append(t[last:])
    free = re.sub(r"\s+", " ", "".join(out)).strip()
    return free, phrases


def parse_kit_request(text: str) -> Optional[Dict[str, Any]]:
    """
    Deterministically parse kit requests so that inches inside quotes work reliably.
    Supports CSV-style escaping: "" inside a quoted phrase -> literal "
    """
    t = text.replace("“", '"').replace("”", '"').replace("’", "'")

    m = _KIT_HEAD_RE.search(t)
    if not m:
        return None

    field = m.group("field").lower()
    rid = m.group("id")

    # tail after "... rotaryid 310" / "... configurazioneid 12"
    tail = t[m.end():].strip(" \t,;")

    # 1) grab quoted keywords (CSV style)
    tail_free, quoted = _extract_kit_quoted_phrases(tail)

    keywords: List[str] = [q.strip() for q in quoted if q.strip()]

    # 2) grab unquoted tokens (comma/semicolon-separated), allowing prefixes
    tail_free = tail_free.strip(" \t,;")
    tail_free = re.sub(
        r"^\s*(?:con|keywords?|accessori)\s*[:\-]?\s*",
        "",
        tail_free,
        flags=re.I,
    )

    for tok in re.split(r"[;,]", tail_free):
        tok = tok.strip()
        if tok:
            keywords.append(tok)

    # de-dup (case-insensitive), preserve order
    seen = set()
    deduped: List[str] = []
    for k in keywords:
        key = k.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(k)

    return {
        "intent": "kit",
        "confidence": 0.95,
        "field": field,
        "id": rid,
        "keywords": deduped,
        # other schema fields filled in by route_intent
    }


# -----------------------------
# Normalization
# -----------------------------

def _pre_normalize(user_text: str) -> str:
    t = user_text.strip()

    # normalize fancy quotes early
    t = t.replace("“", "\"").replace("”", "\"").replace("’", "'")

    # --- common misspellings ---
    t = re.sub(r"\brotery\b|\brotari\b|\brotary\b", "rotary", t, flags=re.I)
    t = re.sub(r"\bconfigurazion(e|i)id\b", "configurazioneid", t, flags=re.I)

    # --- rotary normalization ---
    # "rotary 1400" → "rotary R1400"
    t = re.sub(r"\brotary\s+(\d{3,5})\b", r"rotary R\1", t, flags=re.I)
    t = re.sub(r"\btesta\s+(\d{3,5})\b", r"testa R\1", t, flags=re.I)

    # --- flow units ---
    t = re.sub(r"\bl\s*\/?\s*min\b", "l/min", t, flags=re.I)
    t = re.sub(r"\blpm\b", "l/min", t, flags=re.I)

    # --- rpm ---
    t = re.sub(r"\bgiri\s*\/?\s*min\b", "rpm", t, flags=re.I)
    t = re.sub(r"\bgiri\b", "rpm", t, flags=re.I)

    # --- decimal normalization (ONLY numeric commas) ---
    # "150,5" -> "150.5" but keep commas as separators for lists/keywords
    t = re.sub(r"(\d),(\d)", r"\1.\2", t)

    # kit synonyms + field normalization hints
    t = re.sub(r"\bconfigurazione\s*id\b", "configurazioneid", t, flags=re.I)
    t = re.sub(r"\brotary\s*id\b", "rotaryid", t, flags=re.I)

    # allow "kit per rotary 150" -> "kit per rotaryid 150"
    t = re.sub(r"\bkit\s+per\s+rotary\s+(\d+)\b", r"kit per rotaryid \1", t, flags=re.I)
    t = re.sub(r"\bkit\s+rotary\s+(\d+)\b", r"kit per rotaryid \1", t, flags=re.I)

    # allow "kit per configurazione 150" -> "kit per configurazioneid 150"
    t = re.sub(r"\bkit\s+per\s+configurazione\s+(\d+)\b", r"kit per configurazioneid \1", t, flags=re.I)
    t = re.sub(r"\bkit\s+configurazione\s+(\d+)\b", r"kit per configurazioneid \1", t, flags=re.I)

    return t

# -----------------------------
# Router
# -----------------------------

async def route_intent(
    *,
    chat_client: ChatCompletionsClient,
    model: str,
    user_text: str,
) -> Dict[str, Any]:
    """
    Returns a dict that matches INTENT_SCHEMA (strict).

    Strategy:
    - First, normalize text.
    - Then, deterministically parse KIT requests (for correct quoted keywords like "2"" -> 2").
    - Otherwise, use the LLM router with strict JSON schema.
    """
    normalized_text = _pre_normalize(user_text)

    # Deterministic KIT parsing on normalized text (so "kit per rotary 310" works)
    kit_parsed = parse_kit_request(normalized_text)
    if kit_parsed:
        # Fill missing fields so the result always matches the full schema shape
        kit_parsed.setdefault("rpm", None)
        kit_parsed.setdefault("torque", None)
        kit_parsed.setdefault("flow", None)
        kit_parsed.setdefault("rotary_type", None)
        kit_parsed.setdefault("motor_type", None)
        kit_parsed.setdefault("machine_model", None)
        kit_parsed.setdefault("flow_rate", None)
        kit_parsed.setdefault("volume", None)
        return kit_parsed

    messages = [
        SystemMessage(ROUTER_SYSTEM),
        UserMessage(normalized_text),
    ]

    # 1) Try strict JSON schema mode (best)
    try:
        resp = await chat_client.complete(
            model=model,
            temperature=0,
            response_format=JsonSchemaFormat(
                name="db_intent",
                schema=INTENT_SCHEMA,
                description="Route user requests to DB intents and extract parameters",
                strict=True,
            ),
            messages=messages,
            stream=False,
        )
        content = resp.choices[0].message.content
        data = json.loads(content)
        if "intent" not in data:
            return {"intent": "search", "confidence": 0.0}
        return data

    except HttpResponseError as e:
        # 2) If API version doesn't support json_schema, fall back to "JSON-only"
        msg = str(e)
        if "json_schema" not in msg and "response_format" not in msg:
            raise  # real error, don't hide it

        resp = await chat_client.complete(
            model=model,
            temperature=0,
            messages=[
                SystemMessage(
                    ROUTER_SYSTEM
                    + "\n\nIMPORTANT: Return ONLY a JSON object matching the schema. "
                      "No markdown. No extra text."
                    + "\n\nSchema:\n"
                    + json.dumps(INTENT_SCHEMA, ensure_ascii=False)
                ),
                UserMessage(normalized_text),
            ],
            stream=False,
        )

        content = resp.choices[0].message.content.strip()

        # If model accidentally wraps JSON in text, extract first {...} block
        if not content.startswith("{"):
            m = re.search(r"\{.*\}", content, flags=re.S)
            if m:
                content = m.group(0)

        data = json.loads(content)
        if "intent" not in data:
            return {"intent": "search", "confidence": 0.0}
        return data
