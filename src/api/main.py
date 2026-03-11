# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license.
# See LICENSE file in the project root for full license information.
import contextlib
import logging
import os, subprocess, pathlib
from typing import Union
from urllib.parse import urlparse

import fastapi
from azure.core.credentials import AzureKeyCredential 
from azure.ai.projects.aio import AIProjectClient
from azure.ai.inference.aio import ChatCompletionsClient, EmbeddingsClient
from azure.identity import AzureDeveloperCliCredential, ManagedIdentityCredential
from pathlib import Path
from dotenv import load_dotenv
from fastapi.staticfiles import StaticFiles

from .search_index_manager import SearchIndexManager
from .search_sql_manager import SqlSearchManager
from .util import get_logger

logger = None
enable_trace = False

@contextlib.asynccontextmanager
async def lifespan(app: fastapi.FastAPI):
    azure_credential: Union[AzureDeveloperCliCredential, ManagedIdentityCredential, AzureKeyCredential]
    is_prod = os.getenv("RUNNING_IN_PRODUCTION", "").strip().lower() == "true"
    if not is_prod:
        if tenant_id := os.getenv("AZURE_TENANT_ID"):
            logger.info("Using AzureDeveloperCliCredential with tenant_id %s", tenant_id)
            azure_credential = AzureDeveloperCliCredential(tenant_id=tenant_id)
            azure_search_credential = azure_credential
        else:
            # logger.info("Using AzureDeveloperCliCredential")
            # azure_credential = AzureDeveloperCliCredential()
            logger.info("Using AzureKeyCredential")
            azure_credential = AzureKeyCredential(os.environ["AZURE_EXISTING_AIPROJECT_API_KEY"])
            azure_search_credential = AzureKeyCredential(os.environ["AZURE_AI_SEARCH_API_KEY"])
            logger.info(
                "AZURE_AI_SEARCH_API_KEY loaded (length=%d, suffix=%s)",
                len(azure_search_credential.key),
                azure_search_credential.key[-4:],
            )
    else:
        # User-assigned identity was created and set in api.bicep
        user_identity_client_id = os.getenv("AZURE_CLIENT_ID")
        logger.info("Using ManagedIdentityCredential with client_id %s", user_identity_client_id)
        azure_credential = ManagedIdentityCredential(client_id=user_identity_client_id)
        azure_search_credential = azure_credential
        logger.info("SEARCH auth mode: ManagedIdentityCredential")


    endpoint = os.environ["AZURE_EXISTING_AIPROJECT_ENDPOINT"]
    project = AIProjectClient(
        credential=azure_credential,
        endpoint=endpoint,
    )

    if enable_trace:
        application_insights_connection_string = ""
        try:
            application_insights_connection_string = await project.telemetry.get_application_insights_connection_string()
        except Exception as e:
            e_string = str(e)
            logger.error("Failed to get Application Insights connection string, error: %s", e_string)
        if not application_insights_connection_string:
            logger.error("Application Insights was not enabled for this project.")
            logger.error("Enable it via the 'Tracing' tab in your AI Foundry project page.")
            exit()
        else:
            from azure.monitor.opentelemetry import configure_azure_monitor
            configure_azure_monitor(connection_string=application_insights_connection_string)


    # Project endpoint has the form:   https://your-ai-services-account-name.services.ai.azure.com/api/projects/your-project-name
    # Inference endpoint has the form: https://your-ai-services-account-name.services.ai.azure.com/models
    # Strip the "/api/projects/your-project-name" part and replace with "/models":
    inference_endpoint = f"https://{urlparse(endpoint).netloc}/models"

    logger.warning("inference_endpoint=%s", inference_endpoint)
    logger.warning("chat api_version=%s", "2024-05-01-preview")

    chat =  ChatCompletionsClient(
        endpoint=inference_endpoint,
        credential=azure_credential,
        credential_scopes=["https://cognitiveservices.azure.com/.default"],
        api_version="2024-05-01-preview",
    )
    embed =  EmbeddingsClient(
        endpoint=inference_endpoint,
        credential=azure_credential,
        credential_scopes=["https://cognitiveservices.azure.com/.default"],
    )

    search_endpoint = os.environ.get('AZURE_AI_SEARCH_ENDPOINT')

    manuals_search_index_manager = None
    specs_search_index_manager = None

    embed_dimensions = None
    if os.getenv('AZURE_AI_EMBED_DIMENSIONS'):
        embed_dimensions = int(os.getenv('AZURE_AI_EMBED_DIMENSIONS'))

    logger.info(
        "CONFIG searchEndpoint=%s manualsIndex=%s specsIndex=%s embedDeployment=%s runningInProd=%s",
        os.getenv("AZURE_AI_SEARCH_ENDPOINT"),
        os.getenv("AZURE_AI_SEARCH_INDEX_NAME"),    # manuals index name
        os.getenv("AZURE_AI_SEARCH_INDEX_NAME_SPECS"),  # specs index name
        os.getenv("AZURE_AI_EMBED_DEPLOYMENT_NAME"),
        os.getenv("RUNNING_IN_PRODUCTION"),
        os.getenv("DB-AUTH"),
        os.getenv("DB-SERVER"),
        os.getenv("DB-USER"),
        os.getenv("DB-NAME"),
        os.getenv("DB-PASSWORD")
    )

    # ---- Manuals manager (existing index created in portal/wizard) ----
    if search_endpoint and os.getenv('AZURE_AI_SEARCH_INDEX_NAME') and os.getenv('AZURE_AI_EMBED_DEPLOYMENT_NAME'):
        manuals_search_index_manager = SearchIndexManager(
            endpoint=search_endpoint,
            credential=azure_search_credential,
            index_name=os.getenv('AZURE_AI_SEARCH_INDEX_NAME'),
            dimensions=embed_dimensions,
            model=os.getenv('AZURE_AI_EMBED_DEPLOYMENT_NAME'),
            embeddings_client=embed
        )
        logger.info("Loading manuals index %s", os.getenv('AZURE_AI_SEARCH_INDEX_NAME'))
        await manuals_search_index_manager.ensure_index_loaded()
        logger.info(
            "Manuals SEARCH manager ready. endpoint=%s index=%s",
            search_endpoint,
            os.getenv("AZURE_AI_SEARCH_INDEX_NAME"),
        )
    else:
        logger.info("Manuals RAG search will not be used (missing config).")


    # ---- Specs manager (new index rag-specs created in portal) ----
    if search_endpoint and os.getenv('AZURE_AI_SEARCH_INDEX_NAME_SPECS') and os.getenv('AZURE_AI_EMBED_DEPLOYMENT_NAME'):
        specs_search_index_manager = SearchIndexManager(
            endpoint=search_endpoint,
            credential=azure_search_credential,
            index_name=os.getenv('AZURE_AI_SEARCH_INDEX_NAME_SPECS'),
            dimensions=embed_dimensions,
            model=os.getenv('AZURE_AI_EMBED_DEPLOYMENT_NAME'),
            embeddings_client=embed
        )
        logger.info("Loading specs index %s", os.getenv("AZURE_AI_SEARCH_INDEX_NAME_SPECS"))
        await specs_search_index_manager.ensure_index_loaded()
        logger.info(
            "Specs SEARCH manager ready. endpoint=%s index=%s",
            search_endpoint,
            os.getenv("AZURE_AI_SEARCH_INDEX_NAME_SPECS"),
        )
    else:
        logger.info("Specs search will not be used (missing config).")



    def _build_sql_connection_string() -> str | None:
        server = os.getenv("DB-SERVER")      # e.g. "tcp:myserver.database.windows.net,1433" or "myserver.database.windows.net"
        user = os.getenv("DB-USER")
        db = os.getenv("DB-NAME")
        pwd = os.getenv("DB-PASSWORD")

        if not server or not db:
            return None

        # Normalize server (optional): if user stored without tcp: prefix
        # Azure SQL commonly works with: "tcp:<server>,1433"
        if not server.lower().startswith("tcp:"):
            server = f"tcp:{server}"
        if "," not in server:
            server = f"{server},1433"

        # If you use SQL username/password
        if user and pwd:
            # ODBC Driver name MUST match what exists in your container
            driver = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
            cs = (
                f"DRIVER={{{driver}}};"
                f"SERVER={server};"
                f"DATABASE={db};"
                f"UID={user};"
                f"PWD={pwd};"
                "Encrypt=yes;"
                "TrustServerCertificate=no;"
                "Connection Timeout=30;"
            )
            return cs

        # If you ever move to MSI/AAD later, you’d handle it here based on DB-AUTH
        return None

    sql_search_manager = None
    try:
        sql_cs = _build_sql_connection_string()
        if sql_cs:
            # sql_search_manager = SqlSearchManager(connection_string=sql_cs)
            router_model = os.getenv("AZURE_AI_ROUTER_DEPLOYMENT_NAME") or os.environ["AZURE_AI_CHAT_DEPLOYMENT_NAME"]
            sql_search_manager = SqlSearchManager(
                connection_string=sql_cs,
                chat_client=chat,          # <-- you already created this above
                router_model=router_model, # <-- model/deployment name
                use_llm_router=True,  # or env-flag it
            )
            logger.info("SQL search manager ready. server=%s db=%s",
                        os.getenv("DB-SERVER"), os.getenv("DB-NAME"))
        else:
            logger.info("SQL search will not be used (missing DB-* secrets).")
    except Exception:
        logger.exception("Failed to initialize SQL search manager.")
        sql_search_manager = None


    app.state.chat = chat
    app.state.search_index_manager = manuals_search_index_manager
    app.state.specs_search_index_manager = specs_search_index_manager
    app.state.sql_search_manager = sql_search_manager
    app.state.chat_model = os.environ["AZURE_AI_CHAT_DEPLOYMENT_NAME"]
    yield

    if manuals_search_index_manager is not None:
        try:
            await manuals_search_index_manager.close()
        except Exception:
            logger.exception("Error closing manuals_search_index_manager")
    if specs_search_index_manager is not None:
        try:
            await specs_search_index_manager.close()
        except Exception:
            logger.exception("Error closing specs_search_index_manager")
    if sql_search_manager is not None:
        try:
            await sql_search_manager.close()
        except Exception:
            logger.exception("Error closing sql_search_manager")
    try:
        await embed.close()
    except Exception:
        logger.exception("Error closing embed")
    try:
        await chat.close()
    except Exception:
        logger.exception("Error closing chat")
    try:
        await project.close()
    except Exception:
        logger.exception("Error closing project")



def create_app():
    if not os.getenv("RUNNING_IN_PRODUCTION"):
        #load_dotenv(override=True)
        ROOT = Path(__file__).resolve().parents[1]
        env_path = ROOT / ".env"
        # print("Trying to load:", env_path, "exists:", env_path.exists())
        loaded = load_dotenv(env_path, override=True)
        # print("load_dotenv returned:", loaded)
        # print("AZURE_EXISTING_AIPROJECT_ENDPOINT =", os.getenv("AZURE_EXISTING_AIPROJECT_ENDPOINT"))


    global logger
    logger = get_logger(
        name="COMMY",
        log_level=logging.INFO,
        log_file_name = os.getenv("APP_LOG_FILE"),
        log_to_console=True
    )

    def sh(cmd):
            try:
                return subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
            except subprocess.CalledProcessError as e:
                logger.error("Command failed (%s): %s", e.returncode, e.output)
                raise

    logger.info("ODBCSYSINI=%r ODBCINSTINI=%r", os.getenv("ODBCSYSINI"), os.getenv("ODBCINSTINI"))

    # Show file existence + perms + contents
    for p in ["/etc/odbcinst.ini", "/etc/odbc.ini"]:
        pp = pathlib.Path(p)
        logger.info("%s exists=%s", p, pp.exists())
        try:
            logger.info("%s stat=%s", p, pp.stat())
        except Exception as e:
            logger.error("%s stat error: %s", p, e)
        try:
            logger.info("%s content:\n%s", p, pp.read_text())
        except Exception as e:
            logger.error("%s read error: %s", p, e)

    # Show directory listing (permissions and files)
    try:
        logger.info("ls -la /etc:\n%s", sh(["sh", "-lc", "ls -la /etc | sed -n '1,200p'"]))
    except Exception as e:
        logger.error("ls /etc failed: %s", e)

    # Show odbcinst output
    try:
        logger.info("odbcinst -j:\n%s", sh(["odbcinst", "-j"]))
    except Exception as e:
        logger.error("odbcinst -j failed: %s", e)

    try:
        logger.info("odbcinst -q -d:\n%s", sh(["odbcinst", "-q", "-d"]))
    except Exception as e:
        logger.error("odbcinst -q -d failed: %s", e)

    import pyodbc
    try:
        logger.info("ODBC drivers: %s", pyodbc.drivers())
    except Exception as e:
        logger.error("No ODBC drivers:: %s", e)

    enable_trace_string = os.getenv("ENABLE_AZURE_MONITOR_TRACING", "")
    global enable_trace
    enable_trace = False
    if enable_trace_string == "":
        enable_trace = False
    else:
        enable_trace = str(enable_trace_string).lower() == "true"
    if enable_trace:
        logger.info("Tracing is enabled.")
        try:
            from azure.monitor.opentelemetry import configure_azure_monitor
        except ModuleNotFoundError:
            logger.error("Required libraries for tracing not installed.")
            logger.error("Please make sure azure-monitor-opentelemetry is installed.")
            exit()
    else:
        logger.info("Tracing is not enabled")

    app = fastapi.FastAPI(lifespan=lifespan)
    app.mount("/static", StaticFiles(directory="api/static"), name="static")

    from . import routes  # noqa

    app.include_router(routes.router)

    return app
