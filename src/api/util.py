from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
from .models import SearchParams

import logging
import os
import pydantic
from pydantic import BaseModel
import sys
import re

_NUM = r"(\d+[.,]?\d*)"

_FALCHETTI_RE = re.compile(r"\bfalchetti\b", re.IGNORECASE)

def is_falchetti_command(text: str) -> bool:
    return bool(_FALCHETTI_RE.search(text or ""))
   
def _to_float(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    try:
        return float(s.replace(",", "."))
    except Exception:
        return None

def extract_model(text: str) -> Optional[str]:
    """
    Cattura un possibile nome modello macchine (stringa alfanumerica con spazi/trattini).
    Es.: Casagrande B175, Soilmec SR-45, Comacchio MC 8D
    """
    m = re.search(r"(modello|macchina|Modello|Macchina)\s+[a-z0-9\- ]+", text, flags=re.I)
    return m.group(0).strip() if m else None

def extract_rotary_type(text: str) -> Optional[str]:
    """
    Estrae un tipo di rotary dalla frase, es:
    'rotary HR-120S' -> 'HR-120S'.
    """
    m = re.search(r"\b(rotary(?:\s+tipo)?|testa(?:\s+tipo)?)\s+(R[0-9A-Z\-]+)", text, flags=re.I)
    return m.group(2).strip() if m else None

def extract_motor_type(text: str) -> Optional[str]:
    """
    Estrae un tipo di motore dalla frase, es:
    'motore 677/451' -> '677/451'.
    """
    m = re.search(r"\b(motore(?:\s+tipo)?|motori(?:\s+tipo)?)\s+([a-z0-9\-\/]+)", text, flags=re.I)
    return m.group(2).strip() if m else None



def parse_intent(user_text: str) -> Tuple[str, Dict[str, Any]]:
    """
    Riconosce: 'search', 'macchine', 'kit', 'nuova_ricerca'.

      - macchine <RotaryID>
      - kit <RotaryID> [keywords: a,b]
      - nuova ricerca / reset
      - altrimenti -> search
    """
    t = user_text.strip().lower()

    # m = re.search(r"^\s*(macchina|macchine)(?:\s+per\s+(RotaryID|ConfigurazioneID))?\s+(\d+)\s*$", t, flags=re.I)
    m = re.search(r"^\s*(macchina|macchine)(?:\s+per\s+(RotaryID|ConfigurazioneID|rotaryid|configurazioneid))\s+(\d+)\s*$", t, flags=re.I)
    if m:
        keyword = m.group(1).lower()
        field = m.group(2).lower()       
        identifier = m.group(3)

        valid_fields = {"rotaryid", "configurazioneid"}

        if field is not None and field not in valid_fields:
            # field exists but is not one of the allowed ones
            return None          # or raise an exception / handle differently

        return "macchine", {
            "field": field,      # None, RotaryID, or ConfigurazioneID
            "id": identifier
        }

    # m = re.search(r"^\s*(kit|kits)(?:\s+per\s+(RotaryID|ConfigurazioneID))?\s+(\d+)(?:\s+keywords?:\s*(.+))?$", t, flags=re.I)
    m = re.search(r"^\s*(kit|kits)(?:\s+per\s+(RotaryID|ConfigurazioneID|rotaryid|configurazioneid))\s+(\d+)(?:\s+keywords?:\s*(.+))?$", t, flags=re.I)
    if m:
        keyword = m.group(1).lower()
        field = m.group(2).lower()       
        identifier = m.group(3)
        kw: List[str] = []
        if m.group(4):
            kw = [x.strip() for x in re.split(r",|;", m.group(4)) if x.strip()]

        valid_fields = {"rotaryid", "configurazioneid"}

        if field is not None and field not in valid_fields:
            # field exists but is not one of the allowed ones
            return None          # or raise an exception / handle differently

        return "kit", {
            "field": field, 
            "id": identifier, 
            "keywords": kw}
    
    m = re.search(r"^\s*(peso)(?:\s+per\s+(Modello|Macchina|modello|macchina))?\s+([A-Za-z0-9 ]+)$", t, flags=re.I)
    if m:
        keyword = m.group(1).lower()
        field = m.group(2).lower()       
        identifier = m.group(3)

        return "peso", {
            "field": field, 
            "id": identifier}
    
    m = re.search(r"^\s*(commesse)(?:\s+per\s+(Modello|RotaryID|Testa|Matricola|modello|rotaryID|testa|matricola))\s+([A-Za-z0-9 ]+)$", t, flags=re.I)
    if m:
        keyword = m.group(1).lower()
        field = m.group(2).lower()       
        identifier = m.group(3)

        valid_fields = {"modello","rotaryid", "testa", "matricola"}

        if field is not None and field not in valid_fields:
            # field exists but is not one of the allowed ones
            return None          # or raise an exception / handle differently

        return "commesse", {
            "field": field, 
            "id": identifier}
    
    m = re.search(
        r"""^\s*
            (alternativa)                             # 1: keyword
            (?:\s+per)?\s+                            # optional 'per' + required space
            (ConfigurazioneID|configurazioneid)       # 2: field
            \s+(\d+)                                  # 3: ID
            (?:\s*,?\s*(\d+(?:[.,]\d+)?)\s*l\s*/?\s*min)?  # 4: optional flow rate
            (?:\s*,?\s*(\d+(?:[.,]\d+)?)\s*cc)?            # 5: optional volume
            \s*$""",
        t,
        flags=re.I | re.X
    )

    if m:
        keyword = m.group(1).lower()          # "alternativa"
        field = "configurazioneid"
        identifier = m.group(3)               # "450"
        flow_rate = m.group(4)                # "150" or None
        volume = m.group(5)                   # "50" or None

        data = {
            "field": field,
            "id": identifier,
        }

        if flow_rate is not None:
            data["flow_rate"] = flow_rate.replace(',', '.')  # normalize if needed
        if volume is not None:
            data["volume"] = volume.replace(',', '.')

        return "alternativa", data
    
    if "nuova ricerca" in t or t in ("nuovo", "ricomincia", "reset"):
        return "nuova_ricerca", {}

    return "search",  {}


def build_params_from_payload(payload: dict) -> SearchParams:
    return SearchParams(
        rpm=payload.get("rpm"),
        torque=payload.get("torque"),
        flow=payload.get("flow"),
        rotary_type=payload.get("rotary_type"),
        motor_type=payload.get("motor_type"),
        machine_model=payload.get("machine_model"),
    )


def build_params(user_text: str) -> SearchParams:
    rpm, nm, flow = extract_core_params(user_text)
    model = extract_model(user_text)
    rotary = extract_rotary_type(user_text)
    motor = extract_motor_type(user_text)
    return SearchParams(
        rpm=rpm,
        torque=nm,
        flow=flow,
        rotary_type=rotary,
        motor_type=motor,
        machine_model=model,
    )

def extract_core_params(text: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Estrae rpm (giri), Nm (coppia) e portata (l/min) supportando unità alla destra.
    """
    t = text.lower().replace("ø", " ")
    rpm: Optional[float] = None
    nm: Optional[float] = None
    q: Optional[float] = None

    # ... 800 rpm / 800 giri/min / 800 giri
    for m in re.finditer(rf"{_NUM}\s*(rpm|giri/min|giri)\b", t):
        rpm = _to_float(m.group(1))

    # ... 1200 nm
    for m in re.finditer(rf"{_NUM}\s*(nm|danm|coppia|knm)\b", t):
        nm = _to_float(m.group(1))

    # ... 120 l/min / 120 lpm
    for m in re.finditer(rf"{_NUM}\s*(l/min|l\/min|lpm)\b", t):
        if q is None:
            q = _to_float(m.group(1))

    # ... portata 120
    m = re.search(rf"portata[^\d]*{_NUM}", t)
    if m:
        q = _to_float(m.group(1)) or q

    return rpm, nm, q

def get_logger(
    name: str,
    log_level: int = logging.INFO,
    log_file_name: Optional[str] = None,
    log_to_console: bool = True
) -> logging.Logger:
    """
    Return the logger, capable to log into file and/or to console.

    :param name: the name of the logger.
    :param log_level: The logging verbosity level.
    :param log_file_name: The file to be sed to write logs if any.
    :param log_to_console: Boolean showing if we want to log into the console.
    :returns: The logger object.
    """
    # 1) Configure ROOT LOGGER ONCE
    root = logging.getLogger()
    root.setLevel(log_level)

    if not root.handlers:
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        )

        if log_to_console:
            stream_handler = logging.StreamHandler(sys.stdout)
            stream_handler.setLevel(log_level)
            stream_handler.setFormatter(formatter)
            root.addHandler(stream_handler)

        if log_file_name:
            file_handler = logging.FileHandler(log_file_name)
            file_handler.setLevel(log_level)
            file_handler.setFormatter(formatter)
            root.addHandler(file_handler)

    # 2) Create NAMED LOGGER (no handlers!)
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    logger.propagate = True   # ← CRITICAL

    is_prod = os.getenv("RUNNING_IN_PRODUCTION", "").strip().lower() == "true"
    if is_prod:
        logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
        logging.getLogger("azure").setLevel(logging.WARNING)
    return logger


class Message(pydantic.BaseModel):
    content: str
    role: str = "user"


class ChatRequest(pydantic.BaseModel):
    messages: list[Message]
    topic: Optional[str] = None # to clearly state the topic without doubt!
