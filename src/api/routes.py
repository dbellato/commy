# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license.
# See LICENSE file in the project root for full license information.
import json
import logging
import mimetypes
import os
import re
import subprocess

from typing import Dict

import fastapi
from fastapi import Request, Depends
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from azure.identity.aio import DefaultAzureCredential
from azure.storage.blob.aio import BlobServiceClient
from fastapi.templating import Jinja2Templates
from azure.ai.inference.prompts import PromptTemplate
from azure.ai.inference.aio import ChatCompletionsClient

from .util import get_logger, ChatRequest
from .search_index_manager import SearchIndexManager
from .search_sql_manager import SqlSearchManager
from azure.core.exceptions import HttpResponseError
from .search_index_manager import SpecsSearchResult


from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from typing import Optional
import secrets

security = HTTPBasic()


username = os.getenv("WEB_APP_USERNAME")
password = os.getenv("WEB_APP_PASSWORD")
basic_auth = username and password

def authenticate(credentials: Optional[HTTPBasicCredentials] = Depends(security)) -> None:

    if not basic_auth:
        logger.info("Skipping authentication: WEB_APP_USERNAME or WEB_APP_PASSWORD not set.")
        return
    
    correct_username = secrets.compare_digest(credentials.username, username)
    correct_password = secrets.compare_digest(credentials.password, password)
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return

auth_dependency = Depends(authenticate) if basic_auth else None

logger = get_logger(
    name="COMMY_routes",
    log_level=logging.INFO,
    log_file_name=os.getenv("APP_LOG_FILE"),
    log_to_console=True
)

router = fastapi.APIRouter()
templates = Jinja2Templates(directory="api/templates")

_BLOB_STORAGE_ACCOUNT = "digistorageaccount"
_BLOB_ROTARIES_CONTAINER = "rotaries"


@router.get("/blob/rotaries/{filename}")
async def get_rotaries_blob(filename: str, _ = auth_dependency):
    """Proxy for Azure Blob Storage rotaries container (no public access)."""
    credential = DefaultAzureCredential()
    try:
        blob_service = BlobServiceClient(
            account_url=f"https://{_BLOB_STORAGE_ACCOUNT}.blob.core.windows.net",
            credential=credential,
        )
        blob_client = blob_service.get_blob_client(
            container=_BLOB_ROTARIES_CONTAINER, blob=filename
        )
        stream = await blob_client.download_blob()
        content = await stream.readall()
    except Exception:
        raise fastapi.HTTPException(status_code=404, detail="Blob not found")
    finally:
        await credential.close()

    mime_type, _ = mimetypes.guess_type(filename)
    return Response(content=content, media_type=mime_type or "application/octet-stream")


def parse_specs_context(context: str) -> list[dict]:
    """
    Parse the context produced by search_specs_machines().

    Supports multiple evidence bullets per MACHINE block:

    MACHINE
    commessa: ...
    modello: ...
    matricola: ...
    versione: ...
    evidenza:
    - section: ...
      titolo: ...
      text: ...
    - section: ...
      titolo: ...
      text: ...
    """
    if not context:
        return []

    blocks = [b.strip() for b in context.split("\n\n---\n\n") if b.strip()]
    out: list[dict] = []

    # Match each evidence bullet (DOTALL to allow newlines in text)
    ev_re = re.compile(
        r"(?ms)^\s*-\s*section:\s*(?P<section>.*?)\s*\n"
        r"\s*titolo:\s*(?P<titolo>.*?)\s*\n"
        r"\s*text:\s*(?P<text>.*?)(?=\n\s*-\s*section:|\Z)"
    )

    for b in blocks:
        if not b.startswith("MACHINE"):
            continue

        def pick_field(label: str) -> str:
            m = re.search(rf"(?mi)^\s*{re.escape(label)}\s*:\s*(.*)\s*$", b)
            return (m.group(1).strip() if m else "")

        commessa = pick_field("commessa")
        modello = pick_field("modello")
        matricola = pick_field("matricola")
        versione = pick_field("versione")

        evidenze = []
        for m in ev_re.finditer(b):
            evidenze.append({
                "section": (m.group("section") or "").strip() or "N/A",
                "titolo": (m.group("titolo") or "").strip() or "N/A",
                "text": (m.group("text") or "").strip(),
            })

        # Backward compatibility / safety: if no bullets parsed, fall back to single pick
        if not evidenze:
            section = pick_field("- section") or "N/A"
            titolo = pick_field("titolo") or "N/A"
            text = pick_field("text") or ""
            evidenze = [{"section": section, "titolo": titolo, "text": text}]

        out.append({
            "commessa": commessa,
            "modello": modello,
            "matricola": matricola,
            "versione": versione,
            "evidenze": evidenze,
        })

    return out


def render_specs_list(machines: list[dict]) -> str:
    parts = []

    def _cap120(s: str) -> str:
        s = (s or "").strip().replace("\n", " ")
        return (s[:117] + "...") if len(s) > 120 else s

    for i, m in enumerate(machines, 1):
        evidenze = m.get("evidenze") or []

        # backward compatibility: if old parser returns single evidence fields
        if not evidenze:
            evidenze = [{
                "section": m.get("section", "N/A"),
                "titolo": m.get("titolo", "N/A"),
                "text": m.get("text", ""),
            }]

        evidenza_lines = []
        for e in evidenze:
            evidenza_lines.append(
                f"- **section:** {e.get('section','N/A')}\n"
                f"  **titolo:** {e.get('titolo','N/A')}\n"
                f"  **text:** {_cap120(e.get('text',''))}"
            )

        parts.append(
            f"{i}.\n"
            f"**Commessa:** {m.get('commessa','')}\n"
            f"**Modello:** {m.get('modello','')}\n"
            f"**Matricola:** {m.get('matricola','')}\n"
            f"**Versione:** {m.get('versione','')}\n"
            f"**Evidenza:**\n"
            + "\n".join(evidenza_lines) + "\n"
            f"---"
        )

    if parts:
        parts.append("Puoi indicarmi una delle commesse qui sopra se vuoi che approfondisca una macchina specifica.")
    return "\n".join(parts)

# Accessors to get app state
def get_chat_client(request: Request) -> ChatCompletionsClient:
    return request.app.state.chat


def get_chat_model(request: Request) -> str:
    return request.app.state.chat_model


# Manuals (existing)
def get_manuals_search_index_manager(request: Request) -> SearchIndexManager:
    return request.app.state.search_index_manager


# Specs (new)
def get_specs_search_index_manager(request: Request) -> SearchIndexManager:
    return request.app.state.specs_search_index_manager

# DB (new)
def get_sql_search_manager(request: Request) -> Optional[SqlSearchManager]:
    return getattr(request.app.state, "sql_search_manager", None)

def serialize_sse_event(data: Dict) -> str:
    return f"data: {json.dumps(data)}\n\n"

def is_widget_payload(x) -> bool:
    return (
        isinstance(x, dict)
        and x.get("type") == "falchetti"
        and isinstance(x.get("data"), dict)
    )

@router.get("/", response_class=HTMLResponse)
async def index_name(request: Request, _ = auth_dependency):
    return templates.TemplateResponse(
        "index.html", 
        {
            "request": request,
        }
    )


@router.get("/_debug/odbc")
def debug_odbc():
    def run(cmd):
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)

    out = {}
    out["ODBCSYSINI"] = os.getenv("ODBCSYSINI")
    out["ODBCINSTINI"] = os.getenv("ODBCINSTINI")

    try:
        out["odbcinst_j"] = run(["odbcinst", "-j"])
    except Exception as e:
        out["odbcinst_j_error"] = str(e)

    try:
        out["odbcinst_q_d"] = run(["odbcinst", "-q", "-d"])
    except Exception as e:
        out["odbcinst_q_d_error"] = str(e)

    # show where it thinks odbcinst.ini is
    try:
        out["etc_odbcinst_ini"] = open("/etc/odbcinst.ini", "r").read()
    except Exception as e:
        out["etc_odbcinst_ini_error"] = str(e)

    try:
        import pyodbc
        out["pyodbc_drivers"] = list(pyodbc.drivers())
    except Exception as e:
        out["pyodbc_error"] = str(e)

    return out

@router.post("/chat")
async def chat_stream_handler(
    chat_request: ChatRequest,
    chat_client: ChatCompletionsClient = Depends(get_chat_client),
    model_deployment_name: str = Depends(get_chat_model),
    manuals_search_index_manager: SearchIndexManager = Depends(get_manuals_search_index_manager),
    specs_search_index_manager: SearchIndexManager = Depends(get_specs_search_index_manager),
    search_sql_manager: SqlSearchManager | None = Depends(get_sql_search_manager),
    _ = auth_dependency
) -> fastapi.responses.StreamingResponse:
    
    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Type": "text/event-stream"
    }    
    if chat_client is None:
        raise Exception("Chat client not initialized")

    def is_meaningful(x) -> bool:
        if x is None:
            return False
        if isinstance(x, dict):
            # structured payload (widgets, etc.) – not “text context”
            return False
        if not isinstance(x, str):
            x = str(x)
        cleaned = x.replace("_", "").replace("-", "").strip()
        return len(cleaned) > 50

    def is_specs_question(user_text: str) -> bool:
        q = (user_text or "").lower()
        # Heuristic routing to rag-specs
        return any(k in q for k in [
            "in which machines", "which machines",
            "in quali macchine", "su quali macchine",
            "in che macchine", "su che macchine",
            "dove è montato", "dove e montato", "montato",
            "dove sono montati", "montati",
            "installato", "utilizzato", "used", "mounted",
            "installati", "utilizzati", 
            "in quali commesse", "su quali commesse", "commesse",
            "in quale commessa", "su quale commessa", "commessa",
            "specifiche", "specifica", "specs"
        ])

    def is_db_question(user_text: str) -> bool:
        q = (user_text or "").lower()
        return any(k in q for k in [
            "configurazione", "configurazioni", "rotary", "rotaryid",
            "rpm", "giri", "portata", "l/min", "lmin",
            "coppia", "danm", "motore", "mode",
            "commesse per rotaryid", "macchine per rotaryid",
            "kit per rotaryid", "alternativa per configurazioneid"
        ])
    
    def count_listed_machines_from_answer(answer: str) -> int:
    # Count occurrences of "Commessa:" allowing optional Markdown bold.
    # We do NOT anchor to line start because the model may format blocks on one line.
        return len(re.findall(r"(?i)\*{0,2}commessa\*{0,2}\s*:\s*\S+", answer))

    async def response_stream():
        messages = [{"role": message.role, "content": message.content} for message in chat_request.messages]

        prompt_messages = PromptTemplate.from_string('You are a helpful assistant').create_messages()

        # --- last user message (for routing + logging) ---
        last_user_msg = ""
        for m in reversed(chat_request.messages):
            if m.role == "user":
                last_user_msg = (m.content or "")
                break


        topic = (chat_request.topic or "").strip().lower()
        chosen_index = "none"
        rag_context = ""
        db_context = ""
        context = "" 
        retrieved_raw_hits: int | None = None
        retrieved_unique_machines: int | None = None

        # Decide which index manager to use
        # Primary routing based on explicit topic
        if topic == "specifications":
            if specs_search_index_manager is not None:
                chosen_index = os.getenv("AZURE_AI_SEARCH_INDEX_NAME_SPECS") or "rag-specs"
                logger.info('TOPIC route: specifications index=%s userMsg="%s"',
                            chosen_index, last_user_msg[:200].replace("\n", " "))
                specs_result = await specs_search_index_manager.search_specs_machines(chat_request)
                if isinstance(specs_result, SpecsSearchResult):
                    rag_context = specs_result.context
                    retrieved_raw_hits = specs_result.raw_hits
                    retrieved_unique_machines = specs_result.unique_machines
                else:
                    # backward compatibility if something returns raw context as string
                    rag_context = str(specs_result or "")
                    retrieved_raw_hits = None
                    retrieved_unique_machines = 0
            else:
                logger.warning("Specs manager not configured.")

        elif topic == "manuals":
            if manuals_search_index_manager is not None:
                chosen_index = os.getenv("AZURE_AI_SEARCH_INDEX_NAME") or "rag-manuals"
                logger.info('TOPIC route: manuals index=%s userMsg="%s"',
                            chosen_index, last_user_msg[:200].replace("\n", " "))
                rag_context = await manuals_search_index_manager.search(chat_request)
            else:
                logger.warning("Manuals manager not configured.")

        elif topic == "components":
            # components -> SQL (as per your existing DB integration)
            if search_sql_manager is not None:
                chosen_index = "sqlserver"
                logger.info('TOPIC route: components(sql) userMsg="%s"',
                            last_user_msg[:200].replace("\n", " "))
                try:
                    db_context = await search_sql_manager.search(chat_request)
                    # ✅ If SQL returned a widget (e.g., Falchetti), stream it and finish
                    if is_widget_payload(db_context):
                        yield serialize_sse_event({"type": "widget", "widget": db_context})
                        yield serialize_sse_event({"type": "stream_end"})
                        return

                except Exception:
                    logger.exception("SQL search failed")
                    db_context = ""
            else:
                logger.warning("SQL manager not configured.")

        else:
            # Fallback: keep your old heuristics if topic missing
            specs_q = is_specs_question(last_user_msg)
            db_q = is_db_question(last_user_msg)
            if specs_q and specs_search_index_manager is not None:
                chosen_index = os.getenv("AZURE_AI_SEARCH_INDEX_NAME_SPECS") or "rag-specs"
                logger.info('RAG start: index=%s userMsg="%s"',
                            chosen_index, last_user_msg[:200].replace("\n", " "))
                rag_context = await specs_search_index_manager.search_specs_machines(chat_request)

            elif manuals_search_index_manager is not None:
                chosen_index = os.getenv("AZURE_AI_SEARCH_INDEX_NAME") or "rag-manuals"
                logger.info('RAG start: index=%s userMsg="%s"',
                            chosen_index, last_user_msg[:200].replace("\n", " "))
                rag_context = await manuals_search_index_manager.search(chat_request)

            # DB can be ADDITIVE (not elif) if you want DB + RAG together:
            if db_q and search_sql_manager is not None:
                try:
                    chosen_index = "sqlserver"
                    logger.info('DB start: userMsg="%s"', last_user_msg[:200].replace("\n", " "))
                    db_context = await search_sql_manager.search(chat_request)
                    if is_widget_payload(db_context):
                        yield serialize_sse_event({"type": "widget", "widget": db_context})
                        yield serialize_sse_event({"type": "stream_end"})
                        return
                except Exception:
                    logger.exception("SQL search failed")
                    db_context = ""
            else:
                logger.info("DB not used for this message (db_q=%s, sql_manager=%s)", db_q, bool(search_sql_manager))


        contexts = [c for c in [db_context, rag_context] if c and is_meaningful(c)]
        context = "\n\n---\n\n".join(contexts)
        logger.info(
            "RAG context returned: index=%s isNone=%s length=%s preview=%r",
            chosen_index,
            context is None,
            0 if context is None else len(context),
            "" if not context else context[:200].replace("\n", " "),
        )

        # If no context, answer politely
        if not context or not is_meaningful(context):
            yield serialize_sse_event({
                "content": "Mi dispiace. Non sono riuscito a trovare le informazioni richieste.",
                "type": "completed_message",
            })
            yield serialize_sse_event({"type": "stream_end"})
            return


        # ✅ SHORT-CIRCUIT FOR SQL: return DB markdown as-is (table + footer, all rows)
        if chosen_index == "sqlserver":
            if not context:
                yield serialize_sse_event({
                    "content": "RISULTATO DB:\nNessun dato trovato.",
                    "type": "completed_message",
                })
                yield serialize_sse_event({"type": "stream_end"})
                return

            # Stream it like normal text (so the UI renders markdown tables)
            yield serialize_sse_event({"content": context, "type": "completed_message"})
            yield serialize_sse_event({"type": "stream_end"})
            return
        
        results_found = "yes" if (retrieved_unique_machines or 0) > 0 else "no"

        # ✅ FOR OTHER INDEXES: go to AI replies
        if chosen_index == os.getenv("AZURE_AI_SEARCH_INDEX_NAME_SPECS"):
            prompt_messages = PromptTemplate.from_string(
                "You are an assistant that answers questions about machine specifications and replies in the language of the user.\n"
                "Use ONLY the information contained in the CONTEXT.\n"
                "\n"
                "RESULTS_FOUND: {{results_found}}\n"
                "TOTAL_MACHINES: {{retrieved_unique_machines}}\n"
                "\n"
                "If RESULTS_FOUND is 'yes', you MUST produce a numbered list of EXACTLY {{retrieved_unique_machines}} machines.\n"
                "If RESULTS_FOUND is 'no', write that you could not find the proper information in the specifications.\n"
                "\n"
                "You must return a UNIQUE numbered list of machines.\n"
                "Do not cut results.\n"
                "The list must show ALL machines found.\n"
                "\n"
                "Mandatory format (use exactly these labels):\n"
                "**Commessa:** <value>\n"
                "**Modello:** <value>\n"
                "**Matricola:** <value>\n"
                "**Versione:** <value>\n"
                "**Evidenza:**\n"
                "- **section:** <value or N/A>\n"
                "  **titolo:** <value or N/A>\n"
                "  **text:** <excerpt, MAX 120 characters>\n"
                "\n"
                "Rules:\n"
                "- DO NOT invent Commessa/Modello/Matricola/Versione. COPY them exactly from CONTEXT.\n"
                "- DO NOT use the word 'Evidence'. Use only 'Evidenza'.\n"
                "- The evidenza MUST always include section and titolo (even if N/A).\n"
                "- Use bold formatting exactly as shown above (with **...**).\n"
                "- After each machine block, output a line containing exactly: ---\n"
                "- If RESULTS_FOUND is 'yes', you MUST NOT say you found no information.\n"
                "- At the end of the list, add ONE sentence suggesting the user to further investigate the results.\n"
                "\n"
                "CONTEXT:\n{{context}}"
            ).create_messages(data={
                "context": context,
                "results_found": results_found,
                "retrieved_unique_machines": retrieved_unique_machines or 0,
            })
        elif chosen_index == os.getenv("AZURE_AI_SEARCH_INDEX_NAME"):
            prompt_messages = PromptTemplate.from_string(
                "You are a helpful assistant that answers questions using excerpts from technical manuals and replies in the language of the user.\n"
                "Use the information in the context to answer the user's question.\n"
                "You may summarize, interpret, and combine information from different parts of the context.\n"
                "When available, mention the manual and section the information comes from.\n"
                "If the context contains only partial information, provide the best possible answer and explain any limitations.\n"
                "Do not use information that is not present in the context.\n\n"
                "Context:\n{{context}}"
            ).create_messages(data={"context": context})

        logger.warning(
            "Prompt vars: results_found=%r retrieved_unique_machines=%r context_len=%d",
            results_found, retrieved_unique_machines, len(context or "")
        )


        # ✅ SPECS: deterministic formatting (no LLM)
        if chosen_index == os.getenv("AZURE_AI_SEARCH_INDEX_NAME_SPECS"):
            machines_parsed = parse_specs_context(context)
            content_out = render_specs_list(machines_parsed)

            logger.info("Deterministic SPECS output: machines_parsed=%d", len(machines_parsed))
            yield serialize_sse_event({"content": content_out, "type": "completed_message"})
            yield serialize_sse_event({"type": "stream_end"})
            return
        
        try:
            accumulated_message = ""
            final_messages = prompt_messages + [{"role": "user", "content": last_user_msg}]
            chat_coroutine = await chat_client.complete(
                model=model_deployment_name,
                # messages=prompt_messages + messages,
                messages=final_messages,
                temperature=0,
                top_p=1,
                stream=True
            )
            async for event in chat_coroutine:
                if event.choices:
                    first_choice = event.choices[0]
                    if first_choice.delta.content:
                        piece = first_choice.delta.content
                        accumulated_message += piece
                        yield serialize_sse_event({"content": piece, "type": "message"})

            # 👇 👇 👇 THIS IS "AFTER STREAMING"
            listed = count_listed_machines_from_answer(accumulated_message)
            logger.info("LLM answer preview: %r", accumulated_message[:300].replace("\n", " "))

            if retrieved_raw_hits is not None and retrieved_unique_machines is not None:
                logger.info(
                    "SPECS completeness: raw_hits=%d unique_machines=%d listed_by_llm=%d",
                    retrieved_raw_hits,
                    retrieved_unique_machines,
                    listed,
                )

            yield serialize_sse_event({"content": accumulated_message, "type": "completed_message"})

        except Exception as e:
            # Keep your existing content-filter / error handling logic if you prefer.
            error_text = str(e)
            logger.error("Chat error: %s", error_text)
            yield serialize_sse_event({"content": error_text, "type": "completed_message"})

        yield serialize_sse_event({"type": "stream_end"})

    return StreamingResponse(response_stream(), headers=headers)

