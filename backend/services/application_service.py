from azure.storage.blob import BlobServiceClient
from services.blob_service import BlobService
from services.openai_service import OpenAIService
from services.text_processing_service import TextProcessingService
from services.openai_service import OpenAIService
import os
import logging
from datetime import datetime, timedelta
import time
from fastapi.exceptions import HTTPException
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
class ApplicationService:
    @staticmethod
    def initialize():
        BlobService.initialize()
    @staticmethod
    def handle_jd(
        blob_service_client: BlobServiceClient,
        blob_filename: str,
        full_text: str,
        incoming_container: str,
        status_container: str,
        results_container: str,
        config_container: str,
    ) -> None:
        skills = OpenAIService.extract_skills_from_jd(full_text)
        BlobService.handle_jd_upload(blob_service_client , blob_filename, full_text, incoming_container, 
                                     status_container , results_container , config_container , skills)
        
    @staticmethod
    def handle_cv(blob_service_client: BlobServiceClient,
    blob_filename: str,
    full_text: str,
    status_container: str,
    results_container: str,
    config_container: str)->None: 
        RESULTS_BLOB_NAME = os.environ.get("RESULTS_BLOB_NAME" , "results")
        if(BlobService.cv_already_scored(blob_service_client, results_container, blob_filename,RESULTS_BLOB_NAME)):
            logging.info(
                "CV '%s' is already scored in results.xlsx; skipping re-processing.",
                blob_filename,
            )
            return

        skills= BlobService.read_skills_from_results_header(blob_service_client , results_container, RESULTS_BLOB_NAME)
        scores , total_score = OpenAIService.handle_cv_upload(full_text , skills)
        BlobService.handle_cv_upload(blob_service_client, blob_filename , full_text , status_container,results_container,
                                     config_container,scores,total_score)
        
    @staticmethod
    def handle_other(
        blob_service_client: BlobServiceClient,
        blob_filename: str,
        status_container: str,
        results_container: str,
    ) -> None:
        BlobService.handle_other(blob_service_client, blob_filename, status_container, results_container)
    
    @staticmethod
    def process_blob(blob_name: str) -> None:
        logging.info("Processing blob '%s'.", blob_name)

        counted_for_progress = False

        try:
            blob_service_client = BlobService.get_blob_service_client()

            incoming_container = os.environ.get("BLOB_INCOMING_CONTAINER", "incoming")
            status_container   = os.environ.get("BLOB_STATUS_CONTAINER",   "status")
            results_container  = os.environ.get("BLOB_RESULTS_CONTAINER",  "results")
            config_container   = os.environ.get("BLOB_CONFIG_CONTAINER",   "config")

            blob_bytes = (
                blob_service_client
                .get_container_client(incoming_container)
                .get_blob_client(blob_name)
                .download_blob()
                .readall()
            )

            blob_filename = os.path.basename(blob_name)

            if blob_filename.lower().endswith(".zip"):
                BlobService.handle_zip_upload(
                    blob_service_client,
                    incoming_container,
                    blob_filename,
                    blob_bytes,
                )
                return

            try:
                full_text = TextProcessingService.extract_text_from_pdf(blob_bytes)
            except Exception as e:
                logging.error("Failed to extract text from '%s': %s", blob_filename, e)
                full_text = ""

            if full_text.strip():
                label = OpenAIService.classify_document(full_text)
            else:
                logging.warning(
                    "No text extracted from '%s'; classifying as OTHER by default.",
                    blob_filename,
                )
                label = "OTHER"

            logging.info("Document '%s' classified as '%s'.", blob_filename, label)

            if label == "JD":

                ApplicationService.handle_jd(
                    blob_service_client,
                    blob_filename,
                    full_text,
                    incoming_container,
                    status_container,
                    results_container,
                    config_container,
                )

            elif label == "CV":

                ApplicationService.handle_cv(
                    blob_service_client,
                    blob_filename,
                    full_text,
                    status_container,
                    results_container,
                    config_container,
                )

            else:

                ApplicationService.handle_other(
                    blob_service_client,
                    blob_filename,
                    status_container,
                    results_container,
                )
            BlobService.increment_processed_count()
            counted_for_progress = True

        except Exception as e:

            logging.error("Unhandled error processing blob '%s': %s", blob_name, e)

            # Even failures should advance progress,
            # otherwise frontend hangs forever.
            if (
                blob_name.lower().endswith(".pdf")
                and not counted_for_progress
            ):
                BlobService.increment_processed_count()

    @staticmethod
    def worker_thread():
        while True:
            blob_queue = BlobService.getQueue()
            blob_name = blob_queue.get()
            queue_lock = BlobService.getlock()
            ApplicationService.process_blob(blob_name)
            with queue_lock:
                BlobService.mark_processed(blob_name)
            blob_queue.task_done()
    @staticmethod
    def watcher_thread() -> None:
        while True:
            try:
                blob_service_client = BlobService.get_blob_service_client()
                incoming_container  = os.environ.get("BLOB_INCOMING_CONTAINER", "incoming")
                container_client    = blob_service_client.get_container_client(incoming_container)

                for blob in container_client.list_blobs():
                    blob_name = blob.name
                    queue_lock = BlobService.getlock()
                    blob_queue = BlobService.getQueue()
                    with queue_lock:
                        if BlobService.is_processed_or_processing(blob_name): continue
                        BlobService.mark_processing(blob_name)

                    blob_queue.put(blob_name)
                    logging.info("Queued blob '%s' for processing.", blob_name)

            except Exception as e:
                logging.error("blob_watcher error: %s", e)

            time.sleep(10)
    @staticmethod
    def get_storage_account_info_from_connection_string(conn_str: str):
        return BlobService.get_storage_account_info_from_connection_string(conn_str)
    
    @staticmethod
    def generate_sas(conn_str : str , container  :str , filename : str):
        try:
            account_name, account_key = BlobService.get_storage_account_info_from_connection_string(conn_str)
        except Exception as e:
            logging.error("Failed to parse storage account info: %s", e)
            raise HTTPException(status_code=500, detail="Server storage configuration error")

        sas_token = generate_blob_sas(
            account_name=account_name,
            container_name=container,
            blob_name=filename,
            account_key=account_key,
            permission=BlobSasPermissions(write=True, create=True),
            expiry=datetime.utcnow() + timedelta(minutes=15),
            type = "upload"
        )
        blob_url = (
            f"https://{account_name}.blob.core.windows.net/"
            f"{container}/{filename}?{sas_token}"
        )
        if(type=="upload"):
            return {"uploadUrl": blob_url, "blobName": filename}
        return {"downloadUrl": blob_url}
    
    @staticmethod
    def get_results_json():
        return BlobService.get_results_json()