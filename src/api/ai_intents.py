from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Literal, Optional

IntentName = Literal[
    "nuova_ricerca",
    "search",
    "macchine",
    "commesse",
    "peso",
    "alternativa",
    "kit",
]

class AIIntent(BaseModel):
    intent: IntentName

    # common
    confidence: float = Field(ge=0.0, le=1.0, default=0.7)

    # for macchine/commesse/peso/kit
    field: Optional[Literal[
        "rotaryid", "configurazioneid", "modello", "testa", "matricola", "macchina", "rotary", "configurazione"
    ]] = None
    id: Optional[str] = None

    # for search
    rpm: Optional[float] = None
    torque: Optional[float] = None          # Nm or daNm (decide your convention)
    flow: Optional[float] = None            # l/min
    rotary_type: Optional[str] = None       # e.g. R2000
    motor_type: Optional[str] = None        # e.g. 677/451
    machine_model: Optional[str] = None     # e.g. "CH 350"

    # for alternativa
    flow_rate: Optional[float] = None
    volume: Optional[float] = None

    # for kit
    keywords: Optional[list[str]] = None
