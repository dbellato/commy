from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class SearchParams:
    rpm: Optional[float] = None
    torque: Optional[float] = None
    flow: Optional[float] = None
    rotary_type: Optional[str] = None
    motor_type: Optional[str] = None
    machine_model: Optional[str] = None

    def missing(self) -> List[str]:
        out: List[str] = []
        # Non serve più perché i risultati tornano comunque
        # if self.rpm is None:
        #     out.append("giri nominali max (rpm)")
        if self.torque is None:
            out.append("coppia nominale max (Nm)")
        # almeno uno tra portata e tipo rotary
        if self.flow is None and self.rotary_type is None:
            out.append("portata (l/min) o tipo di rotary")
        return out
