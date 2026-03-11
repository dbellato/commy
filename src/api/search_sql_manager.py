from __future__ import annotations
import logging
from typing import Any, Dict, List, Optional, Tuple
from starlette.concurrency import run_in_threadpool

from .util import ChatRequest
from .util import parse_intent, build_params, is_falchetti_command
from .models import SearchParams

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from azure.ai.inference.aio import ChatCompletionsClient
from .ai_router import route_intent
from .ai_intents import AIIntent
from .falchetti_store import get_state

# your existing utilities (adjust imports to your project structure)
from .db_utils import (
    search_configurazioni,
    search_torque,
    machines_for_rotary,
    machines_for_configuration,
    check_rotaryID,
    analyis_of_machine_data,
    config_for_rotary,
    kits_for_configuration,
    kits_for_rotary
)

logger = logging.getLogger("commy.sql")


def _md_table(rows: List[Dict[str, Any]], cols: List[str], max_rows: int = 40) -> str:
    if not rows:
        return ""

    rows = rows[:max_rows]
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join(["---"] * len(cols)) + " |"

    def fmt(v: Any) -> str:
        if v is None:
            return ""
        s = str(v).replace("\n", " ").strip()
        # keep markdown safe-ish
        return s.replace("|", "\\|")

    body = "\n".join(
        "| " + " | ".join(fmt(r.get(c)) for c in cols) + " |"
        for r in rows
    )
    return "\n".join([header, sep, body])


def _md_kv(d: Dict[str, Any]) -> str:
    lines = []
    for k, v in d.items():
        if v is None:
            continue
        lines.append(f"- **{k}**: {v}")
    return "\n".join(lines)

class SqlSearchManager:
    """
    Intent-based DB backend.
    Returns CONTEXT as plain text/markdown (LLM-friendly).
    """

    def _next_actions_footer_search(self, intent: str, payload: dict) -> str:
        cid = payload.get("rotaryid") or payload.get("id")
        rid = payload.get("configurazioneid") or payload.get("id")

        base = [
            "- nuova ricerca teste `rotary tipo R1400, 1350 daNm, 155 rpm`",
            "- `commesse per modello CH 350` o `commesse per testa R2000` o `commesse per matricola T5454`",
            "- `peso per modello CH 350`",
        ]

        # optionally customize based on intent/id
        if intent == "search":
            base[:0] = [
                f"- `commesse per RotaryID <id>`",
                f"- `kit per RotaryID <id>; keywords: xxxxx, yyyyy`",
                f"- `macchine per RotaryID <id>`",
                f"- `macchine per ConfigurazioneID <id>`",
                f"- `alternativa per ConfigurazioneID <id> 150 l/min 50 cc`",
            ]

        if intent == "kit" and rid:
            base[:0] = [
                f"- `commesse per RotaryID {rid}`",
                f"- `kit per RotaryID {rid}; keywords: xxxxx, yyyyy`",
                f"- `macchine per RotaryID {rid}`",
                f"- `macchine per ConfigurazioneID <id>`",
                f"- `alternativa per ConfigurazioneID <id> 150 l/min e/o 50 cc`",
            ]

        if intent == "macchine" and payload.get("field") == "rotaryid" and rid:
            base[:0] = [
                f"- `commesse per RotaryID {rid}`",
                f"- `kit per RotaryID {rid}; keywords: xxxxx, yyyyy`",
            ]

        if intent == "commesse" and payload.get("field") == "rotaryid" and rid:
            base[:0] = [
                f"- `kit per RotaryID {rid}; keywords: xxxxx, yyyyy`",
            ]

        if intent == "macchine" and payload.get("field") == "configurazioneid" and cid:
            base[:0] = [
                f"- `kit per ConfigurazioneID {cid}`",
            ]

        if intent == "alternativa" and cid:
            base[:0] = [
                f"- `macchine per ConfigurazioneID {cid}`",
                f"- `macchine per RotaryID <id>`",
                f"- `macchine per ConfigurazioneID <id>`",
                f"- `kit per RotaryID <id>; keywords: xxxxx, yyyyy`",
            ]

        return "PROSSIMI COMANDI (opzionali):\n" + "\n".join(base)
    
    async def _resolve_intent(self, user_text: str) -> tuple[str, dict]:
        intent, payload = parse_intent(user_text)

        if intent in {"macchine", "commesse", "peso", "alternativa", "kit", "nuova_ricerca"}:
            return intent, payload

        data = await route_intent(
            chat_client=self._chat_client,
            model=self._router_model,
            user_text=user_text,
        )

        parsed = AIIntent.model_validate(data)
        payload2 = parsed.model_dump(exclude={"intent", "confidence"}, exclude_none=True)
        return parsed.intent, payload2

    _BLOB_BASE = "https://digistorageaccount.blob.core.windows.net/rotaries"

    def _render_kits_markdown(self, title: str, result: Dict[str, Any]) -> str:
        immagine_rotary: Optional[str] = result.get("immagine_rotary")
        scheda_rotary: Optional[str] = result.get("scheda_rotary")
        kits: Dict[str, Any] = result.get("kits", {})

        # Title — bold, hyperlinked to PDF if available
        if scheda_rotary:
            rotary_url = f"{self._BLOB_BASE}/{scheda_rotary}"
            title_md = f"[{title}]({rotary_url})"
        else:
            title_md = title

        out = [f"RISULTATO DB (kits) — **{title_md}**\n"]

        # Rotary image block (350 px height, width auto)
        if immagine_rotary:
            img_url = f"{self._BLOB_BASE}/{immagine_rotary}"
            out.append(
                f'<img src="{img_url}" alt="Immagine Rotary"'
                f' style="height:350px;width:auto;display:block;margin-bottom:12px;">\n'
            )

        for kit_name, kit_data in kits.items():
            items: list[tuple[str, str]] = kit_data.get("items", [])
            immagine_kit: Optional[str] = kit_data.get("immagine_kit")
            scheda_kit: Optional[str] = kit_data.get("scheda_kit")

            # Kit heading — hyperlinked to kit PDF if available
            if scheda_kit:
                kit_url = f"{self._BLOB_BASE}/{scheda_kit}"
                kit_title = f"[{kit_name}]({kit_url})"
            else:
                kit_title = kit_name

            out.append(f"### {kit_title}")

            if immagine_kit:
                kit_img_url = f"{self._BLOB_BASE}/{immagine_kit}"
                # Float image right; table flows to the left; clear resets for next kit
                out.append(
                    f'<img src="{kit_img_url}" alt="Immagine Kit"'
                    f' style="height:350px;width:auto;float:right;margin-left:16px;">'
                )

            out.append("| Accessorio | CodiceAccessorio |")
            out.append("| --- | --- |")
            for acc, code in items:
                out.append(f"| {str(acc).replace('|','\\|')} | {str(code).replace('|','\\|')} |")

            if immagine_kit:
                out.append('<div style="clear:both;"></div>')

            out.append("")  # blank line between kits

        return "\n".join(out)

    def __init__(
        self,
        connection_string: str,
        *,
        chat_client: ChatCompletionsClient,
        router_model: str,
        use_llm_router: bool = True,
    ):
        self._cs = connection_string
        self._chat_client = chat_client
        self._router_model = router_model
        self._use_llm_router = use_llm_router

    async def close(self) -> None:
        # Nothing to clean up here (resources are owned elsewhere)
        pass

    async def search(self, message: ChatRequest) -> str:
        
        user_text = (message.messages[-1].content or "").strip()


        # ✅ deterministic, no AI
        if is_falchetti_command(user_text):
            state = get_state(self._cs)

            return {
                "type": "falchetti",
                "title": "Falchetti",
                "data": {
                    "all_cols": state["all_cols"],
                    "filterable_cols": state["filterable_cols"],
                    "distincts": state["distincts"],
                    "rows": state["rows"],
                    "max_rows": 30,
                },
            }

        if self._use_llm_router:
            intent, payload = await self._resolve_intent(user_text)
        else:
            intent, payload = parse_intent(user_text)

        logger.info("DB intent=%s payload=%s user=%r", intent, payload, user_text[:120])

        if intent == "nuova_ricerca":
            return (
                "RISULTATO DB:\n"
                "Ok, nuova ricerca.\n"
                "Indicami almeno coppia (daNm o Nm) e portata (l/min) oppure tipo rotary.\n"
                "Esempio: `portata 150 l/min, coppia 1200 daNm, 800 rpm, rotary R2000`."
            )

        if intent == "search":
            if payload:
                # build SearchParams from router payload (pseudo)
                params = SearchParams(**payload)
            else:
                params = build_params(user_text)
            return await self._intent_search(user_text, params=params)

        if intent == "macchine":
            return await self._intent_machines(payload)

        if intent == "commesse":
            return await self._intent_analysis(intent="commesse", payload=payload)

        if intent == "peso":
            return await self._intent_analysis(intent="peso", payload=payload)

        if intent == "alternativa":
            return await self._intent_alternativa(payload)

        if intent == "kit":
            return await self._intent_kit(payload)


    # search_sql_manager.py (add)
    async def _intent_search(self, user_text: str, params: SearchParams | None = None) -> str:
        
        params = params or build_params(user_text)
        miss = params.missing()
        if miss:
            return (
                "RISULTATO DB:\n"
                "Mi mancano:\n"
                + "\n".join(f"- {m}" for m in miss)
                + "\n\nEsempio: `150 l/min, 1200 daNm, 800 rpm, rotary R2000`."
            )

        rows = await run_in_threadpool(
            search_configurazioni,
            self._cs,
            rpm=params.rpm,
            torque=params.torque,
            flow=params.flow,
            rotary_type=params.rotary_type,
            motor_type=params.motor_type,
            machine_model=params.machine_model,
        )
        logger.info("Number of retrieved rows: %i", len(rows))

        # then keep your existing logic/table formatting exactly the same...
        if not rows:
            if params.rotary_type:
                torques = await run_in_threadpool(search_torque, self._cs, params.rotary_type)
                torques_txt = ", ".join(str(int(t)) for t in torques) if torques else "(nessun dato)"
                return (
                    "RISULTATO DB:\n"
                    "Nessuna configurazione trovata.\n"
                    f"Per rotary tipo `{params.rotary_type}` le coppie massime presenti: {torques_txt} daNm."
                )
            return "RISULTATO DB:\nNessuna configurazione trovata. Prova con valori leggermente diversi."

        cols = [
            "InfoRotary","Rotary","RotaryID","Descrizione","Codice","ConfigurazioneID",
            "Motore","Mode","Portata","Pressione","VgMotore","CoppiaNetta",
            "RapportoTesta","RapportoRiduttore","RapportoCambio","CoppiaNom","GiriNom"
        ]
        table = _md_table(rows, cols)

        result_text = (
            "RISULTATO DB (vista: ConfigurazioniRotariesModelli)\n\n"
            f"{table}"
        )

        return result_text + "\n\n" + self._next_actions_footer_search(
            intent="search",
            payload={}
        )


    async def _intent_machines(self, payload: Dict[str, Any]) -> str:
        field = (payload.get("field") or "").lower()
        id_ = payload.get("id")

        if not id_:
            return "RISULTATO DB:\nDevi indicare un ID. Esempio: `macchine per RotaryID 12`."

        if field == "rotaryid":
            # validate rotary id
            ok = await run_in_threadpool(check_rotaryID, self._cs, id_)
            if not ok:
                return (
                    "RISULTATO DB:\n"
                    "Il RotaryID indicato non è associato a nessuna Rotary. Controlla l'ID e ritenta."
                )

            models = await run_in_threadpool(machines_for_rotary, self._cs, id_)
            if not models:
                return f"RISULTATO DB:\nNessuna macchina trovata per RotaryID {id_}."
            result_text = (
                f"RISULTATO DB:\n**RotaryID {id_}** può essere montata su:\n"
                + "\n".join(f"- {m}" for m in models)
            )
        
            return result_text + "\n\n" + self._next_actions_footer_search(
                intent="search",
                payload={"field": "rotaryid", "id": id_}
            )


        if field == "configurazioneid":
            models = await run_in_threadpool(machines_for_configuration, self._cs, id_)
            if not models:
                return f"RISULTATO DB:\nNessuna macchina trovata per ConfigurazioneID {id_}."
            result_text = (
                f"RISULTATO DB:\n**ConfigurazioneID {id_}** può essere allestita su:\n"
                + "\n".join(f"- {m}" for m in models)
            )
        
            return result_text + "\n\n" + self._next_actions_footer_search(
                intent="macchine",
                payload={"field": "configurazioneid", "id": id_}
            )

        return "RISULTATO DB:\nFormato non valido. Usa `macchine per RotaryID <id>` o `macchine per ConfigurazioneID <id>`."

    async def _intent_analysis(self, intent: str, payload: Dict[str, Any]) -> str:
        field = (payload.get("field") or "").lower()
        id_ = payload.get("id")
        if not id_:
            return f"RISULTATO DB:\nDevi indicare un valore. Esempio: `{intent} per modello CH 350`."

        # normalize field names like your code does
        if field == "rotaryid":
            field_for_fn = "rotaryid"
        elif field in ("modello", "testa", "matricola"):
            field_for_fn = field
        else:
            # for 'peso' you sometimes pass field=modello/macchina; keep it simple
            field_for_fn = "modello"

        rows = await run_in_threadpool(analyis_of_machine_data, self._cs, intent, field_for_fn, id_)

        if not rows:
            return f"RISULTATO DB:\nNessun dato trovato per `{intent}` con {field} = {id_}."

        # pick a reasonable set of columns (don’t know your exact schema; we keep dynamic + trimmed)
        cols = list(rows[0].keys())[:18]  # prevent enormous tables
        table = _md_table(rows, cols, max_rows=100)

        result_text = (
            f"RISULTATO DB (vista: CommesseArticoliInventario)\n"
            f"Richiesta: **{intent}** per **{field} = {id_}**\n\n"
            f"{table}\n\n"
        )
    
        return result_text + "\n\n" + self._next_actions_footer_search(
            intent="search",
            payload={}
        )

    async def _intent_kit(self, payload: Dict[str, Any]) -> str:
        field = (payload.get("field") or "").lower()
        id_ = payload.get("id")
        keywords = payload.get("keywords") or []

        # normalize synonyms if router outputs "rotary"
        if field == "rotary":
            field = "rotaryid"
        if field == "configurazione":
            field = "configurazioneid"

        if field not in ("rotaryid", "configurazioneid"):
            return (
                "RISULTATO DB:\n"
                "Formato non valido.\n"
                "Usa ad esempio:\n"
                "- `kit per RotaryID 150 \"albero flottante\" \"sensore\"`\n"
                "- `kit per ConfigurazioneID 12 keywords: albero flottante, sensore`"
            )

        if not id_:
            return (
                "RISULTATO DB:\n"
                "Devi indicare un ID.\n"
                "Esempi:\n"
                "- `kit per RotaryID 150`\n"
                "- `kit per rotary 150, \"albero flottante\", \"sensore\"`"
            )

        if not str(id_).strip().isdigit():
            return "RISULTATO DB:\nL'ID deve essere numerico."

        try:
            if field == "rotaryid":
                ok = await run_in_threadpool(check_rotaryID, self._cs, id_)
                if not ok:
                    return (
                        "RISULTATO DB:\n"
                        "Il RotaryID indicato non è associato a nessuna Rotary. Controlla l'ID e ritenta."
                    )

                result = await run_in_threadpool(kits_for_rotary, self._cs, id_, keywords)
                if not result.get("kits"):
                    return f"RISULTATO DB:\nNessun kit trovato per RotaryID {id_}."

                # render kits as markdown tables with images/links
                text = self._render_kits_markdown(f"RotaryID {id_}", result)

                return text + "\n\n" + self._next_actions_footer_search("kit", {"field": "rotaryid", "id": id_})

            else:
                result = await run_in_threadpool(kits_for_configuration, self._cs, id_, keywords)
                if not result.get("kits"):
                    return f"RISULTATO DB:\nNessun kit trovato per ConfigurazioneID {id_}."

                text = self._render_kits_markdown(f"ConfigurazioneID {id_}", result)

                return text + "\n\n" + self._next_actions_footer_search("kit", {"field": "configurazioneid", "id": id_})

        except ValueError as e:
            # your kits_for_* raises ValueError when no kits match keywords
            return (
                "RISULTATO DB:\n"
                "Nessun kit corrisponde alle keywords indicate.\n"
                "Prova con keywords diverse o più generiche."
            )


    async def _intent_alternativa(self, payload: Dict[str, Any]) -> str:
        cid = payload.get("id")
        if not cid:
            return (
                "RISULTATO DB:\n"
                "Devi indicare un ConfigurazioneID.\n"
                "Esempio: `alternativa per ConfigurazioneID 417 con 150 l/min e 50 cc`"
            )

        flow_rate = payload.get("flow_rate")
        volume = payload.get("volume")

        result = await run_in_threadpool(config_for_rotary, self._cs, int(cid), flow_rate, volume)
        if not result:
            return f"RISULTATO DB:\nNessuna configurazione trovata per ConfigurazioneID {cid}."

        # result is a dict (your function returns a dict named "results")
        text = (
            f"RISULTATO DB (alternativa per ConfigurazioneID {cid})\n\n"
            + _md_kv(result)
        )
    
        return text + "\n\n" + self._next_actions_footer_search(intent="search", payload={})
