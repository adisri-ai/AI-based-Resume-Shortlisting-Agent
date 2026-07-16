import logging
from contextlib import asynccontextmanager
import os
import threading

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from typing import Optional

from openai import AzureOpenAI  

from services.config_service import ConfigService
from services.application_service import ApplicationService

from utils.schemas import OpenAIConfigBody , UploadSasBody

from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas

import io

@asynccontextmanager
async def lifespan(app: FastAPI):
    try : 
        ApplicationService.initialize()
    except Exception as e : 
        logging.warning("Could not seed processed blobs at the start: " , e)
    blob_worker = ApplicationService.worker_thread()
    blob_watcher = ApplicationService.watcher_thread()
    worker_thread = threading.Thread(target=blob_worker, daemon=True)
    worker_thread.start()

    watcher_thread = threading.Thread(target=blob_watcher, daemon=True)
    watcher_thread.start()

    yield

app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

__openai_client = Optional[AzureOpenAI] = None
RESULTS_BLOB_NAME , ACTIVE_JD_BLOB_NAME , OTHERS_STATUS_BLOB_NAME , PENDING_STATUS_BLOB_NAME   = ConfigService.load_config()

@app.post("/api/set-openai-config")
def set_openai_config(body: OpenAIConfigBody):
    """
    HTTP endpoint to update OpenAI / storage config at runtime.
    Mirrors the Azure Function /set-openai-config route exactly.
    """
    global _openai_client

    updated_keys = []
    for key in [
        "OPENAI_ENDPOINT", "OPENAI_API_KEY", "OPENAI_API_VERSION", "OPENAI_DEPLOYMENT_NAME",
        "BLOB_INCOMING_CONTAINER", "BLOB_RESULTS_CONTAINER", "BLOB_CONFIG_CONTAINER",
        "BLOB_STATUS_CONTAINER",
    ]:
        value = getattr(body, key, None)
        if value:
            os.environ[key] = str(value).strip()
            updated_keys.append(key)

    if not updated_keys:
        raise HTTPException(status_code=400, detail="No valid keys to update.")

    _openai_client = None
    logging.info("Updated OpenAI config keys: %s", ", ".join(updated_keys))
    return {"detail": "OpenAI configuration updated."}

@app.post("/api/get-upload-sas")
def get_upload_sas(body: UploadSasBody):
    conn_str           = os.environ["DATA_STORAGE_CONNECTION"]
    incoming_container = os.environ.get("BLOB_INCOMING_CONTAINER", "incoming")
    filename = body.filename
    return ApplicationService.generate_sas(conn_str , incoming_container, filename , type="upload")

@app.get("/api/get-results-sas")
def get_results_sas():
    conn_str          = os.environ["DATA_STORAGE_CONNECTION"]
    results_container = os.environ.get("BLOB_RESULTS_CONTAINER", "results")
    return ApplicationService.generate_sas(conn_str , results_container, RESULTS_BLOB_NAME , type="results")

@app.get("/api/get-results-json")
def get_results_json():
    return ApplicationService.get_results_json()

@app.get("/config/frontend")
def frontend_config():
    conn_str     = os.environ.get("DATA_STORAGE_CONNECTION", "")
    account_name = ""
    try:
        parts        = dict(item.split("=", 1) for item in conn_str.split(";") if "=" in item)
        account_name = parts.get("AccountName", "")
    except Exception:
        pass
    return {"storageAccount": account_name}

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger(
        "azure.core.pipeline.policies.http_logging_policy"
    ).setLevel(logging.WARNING)

    uvicorn.run(app, host="0.0.0.0", port=8000)
