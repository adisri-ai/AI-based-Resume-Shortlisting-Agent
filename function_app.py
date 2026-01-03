import logging
import os
import os.path
import io
import json
import zipfile
from datetime import datetime
from typing import List, Tuple, Optional, Set

import azure.functions as func
from azure.storage.blob import BlobServiceClient
import PyPDF2
from openpyxl import Workbook, load_workbook
from openai import AzureOpenAI

app = func.FunctionApp()

_openai_client: Optional[AzureOpenAI] = None

RESULTS_BLOB_NAME = "********"
ACTIVE_JD_BLOB_NAME = "********"
OTHERS_STATUS_BLOB = "*********"
PENDING_STATUS_BLOB = "*********"


def get_openai_client() -> AzureOpenAI:
    global _openai_client
    if _openai_client is None:
        endpoint = "*******"
        deployment = "***********"
        subscription_key = "***********"
        api_version = "*******"
        _openai_client = AzureOpenAI(
            api_version=api_version,
            azure_endpoint=endpoint,
            api_key=subscription_key,
        )
    return _openai_client


def get_blob_service_client() -> BlobServiceClient:
    conn_str = os.environ["DATA_STORAGE_CONNECTION"]
    return BlobServiceClient.from_connection_string(conn_str)


def append_filename_to_status_list(
    blob_service_client: BlobServiceClient,
    status_container: str,
    list_blob_name: str,
    filename: str,
) -> None:
    container_client = blob_service_client.get_container_client(status_container)
    blob_client = container_client.get_blob_client(list_blob_name)
    line = f"{filename}\n"
    try:
        try:
            existing = blob_client.download_blob().readall()
        except Exception:
            existing = b""
        new_content = existing + line.encode("utf-8")
        blob_client.upload_blob(new_content, overwrite=True)
        logging.info(
            "Appended '%s' to status list '%s/%s'.",
            filename,
            status_container,
            list_blob_name,
        )
    except Exception as e:
        logging.error(
            "Failed to append '%s' to status list '%s': %s",
            filename,
            list_blob_name,
            e,
        )


def read_filenames_from_status_list(
    blob_service_client: BlobServiceClient,
    status_container: str,
    list_blob_name: str,
) -> Set[str]:
    container_client = blob_service_client.get_container_client(status_container)
    blob_client = container_client.get_blob_client(list_blob_name)
    names: Set[str] = set()
    try:
        data = blob_client.download_blob().readall().decode("utf-8")
        for line in data.splitlines():
            name = line.strip()
            if name:
                names.add(name)
    except Exception:
        pass
    return names


def extract_text_from_pdf(input_stream: func.InputStream) -> str:
    pdf_bytes = input_stream.read()
    reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
    pages_text = []
    for page in reader.pages:
        page_text = page.extract_text() or ""
        pages_text.append(page_text)
    full_text = "\n".join(pages_text)
    return full_text


def classify_document(content: str) -> str:
    client = get_openai_client()
    deployment = "************"
    system_prompt = (
        "<system prompt>"
    )

    user_prompt = (
        "<user prompt>"
    )

    try:
        response = client.chat.completions.create(
            model=deployment,
            messages=[
                "<messages >"
            ],
            max_tokens=5,
            temperature=0.0,
        )
        label = response.choices[0].message.content.strip().upper()
        if label not in ("JD", "CV", "OTHER"):
            logging.warning(
                "Unexpected classification label '%s', defaulting to OTHER.", label
            )
            return "OTHER"
        return label
    except Exception as e:
        logging.error("Exception during OpenAI classification: %s", e)
        return "OTHER"


def extract_skills_from_jd(jd_text: str) -> List[str]:
    client = get_openai_client()
    deployment = os.environ["OPENAI_DEPLOYMENT_NAME"]
    system_prompt = (
        "<system_prompt>"
    )

    user_prompt = (
        "<user_prompt>"
    )

    try:
        response = client.chat.completions.create(
            model=deployment,
            messages=[
                "<messages>"
            ],
            max_tokens=512,
            temperature=0.0,
        )
        raw = response.choices[0].message.content.strip()

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            start = raw.find("{")
            end = raw.rfind("}")
            if start == -1 or end == -1 or end <= start:
                logging.error(
                    "OpenAI skill extraction response is not valid JSON: %s", raw
                )
                return []
            trimmed = raw[start : end + 1]
            try:
                data = json.loads(trimmed)
            except json.JSONDecodeError as e2:
                logging.error(
                    "Failed to parse JSON from OpenAI skill extraction response: %s\n"
                    "Trimmed: %s",
                    e2,
                    trimmed,
                )
                return []

        skills = data.get("skills", [])
        skills = [str(s).strip() for s in skills if s]
        if len(skills) > 10:
            skills = skills[:10]
        return skills
    except Exception as e:
        logging.error("Exception during OpenAI skill extraction: %s", e)
        return []


def init_results_workbook(
    blob_service_client: BlobServiceClient,
    results_container: str,
    skills: List[str],
) -> None:
    container_client = blob_service_client.get_container_client(results_container)
    blob_client = container_client.get_blob_client(RESULTS_BLOB_NAME)
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "Results"

    header = ["CvName"]
    header.extend(skills)
    header.append("TotalScore")
    worksheet.append(header)

    out_mem_file = io.BytesIO()
    workbook.save(out_mem_file)
    out_mem_file.seek(0)
    blob_client.upload_blob(out_mem_file.read(), overwrite=True)
    logging.info(
        "Initialized '%s' in container '%s' with new JD skill columns.",
        RESULTS_BLOB_NAME,
        results_container,
    )


def save_active_jd_metadata(
    blob_service_client: BlobServiceClient,
    config_container: str,
    jd_filename: str,
    skills: List[str],
) -> None:
    container_client = blob_service_client.get_container_client(config_container)
    blob_client = container_client.get_blob_client(ACTIVE_JD_BLOB_NAME)
    meta = {
        "jd_filename": jd_filename,
        "skills": skills,
        "updated_at_utc": datetime.utcnow().isoformat(),
    }

    data = json.dumps(meta, indent=2)
    blob_client.upload_blob(data, overwrite=True)
    logging.info(
        "Updated '%s' in container '%s' with active JD '%s'.",
        ACTIVE_JD_BLOB_NAME,
        config_container,
        jd_filename,
    )


def load_active_jd_metadata(
    blob_service_client: BlobServiceClient,
    config_container: str,
) -> Optional[dict]:
    container_client = blob_service_client.get_container_client(config_container)
    blob_client = container_client.get_blob_client(ACTIVE_JD_BLOB_NAME)
    try:
        download = blob_client.download_blob()
        data = download.readall().decode("utf-8")
        meta = json.loads(data)
        return meta
    except Exception:
        return None


def read_skills_from_results_header(
    blob_service_client: BlobServiceClient,
    results_container: str,
) -> Optional[List[str]]:
    container_client = blob_service_client.get_container_client(results_container)
    blob_client = container_client.get_blob_client(RESULTS_BLOB_NAME)
    try:
        download_stream = blob_client.download_blob()
        data = download_stream.readall()
        in_mem_file = io.BytesIO(data)
        workbook = load_workbook(in_mem_file)
        worksheet = workbook.active
        header_row = next(worksheet.iter_rows(min_row=1, max_row=1, values_only=True))
        header = list(header_row)
        if len(header) < 3:
            logging.error(
                "results.xlsx header has fewer than 3 columns; cannot read skills."
            )
            return None
        skills = header[1:-1]
        skills = [s for s in skills if s]
        return skills
    except Exception as e:
        logging.error("Failed to read skills from results.xlsx header: %s", e)
        return None


def score_cv_against_skills(
    cv_text: str, skills: List[str]
) -> Tuple[List[float], float]:
    client = get_openai_client()
    deployment = os.environ["OPENAI_DEPLOYMENT_NAME"]
    skills_list_text = "\n".join(
        f"{i+1}. {skill}" for i, skill in enumerate(skills)
    )

    system_prompt = (
        "<system prompt>"
    )

    user_prompt = (
       "user prompt>"
    )

    try:
        response = client.chat.completions.create(
            model=deployment,
            messages=[
                "<messages>"
            ],
            max_tokens=512,
            temperature=0.0,
        )
        raw = response.choices[0].message.content.strip()
        start = raw.find("{")
        if start > 0:
            raw = raw[start:]
        data = json.loads(raw)
        skills_data = data.get("skills", [])
        total_score = float(data.get("total_score", 0.0))

        scores_by_name = {}
        for item in skills_data:
            name = str(item.get("name", "")).strip()
            try:
                score_val = float(item.get("score", 0.0))
            except Exception:
                score_val = 0.0
            scores_by_name[name] = max(0.0, min(10.0, score_val))

        scores: List[float] = []
        for s in skills:
            scores.append(scores_by_name.get(s, 0.0))

        if total_score <= 0.0 or total_score > 10.0:
            if scores:
                total_score = sum(scores) / len(scores)
            else:
                total_score = 0.0

        return scores, total_score
    except Exception as e:
        logging.error("Exception during OpenAI CV scoring: %s", e)
        return [0.0] * len(skills), 0.0


def cv_already_scored(
    blob_service_client: BlobServiceClient,
    results_container: str,
    cv_filename: str,
) -> bool:
    """
    Check if a CV has already been scored (i.e., appears in the CvName column
    of results.xlsx). Used to make CV processing idempotent.
    """
    container_client = blob_service_client.get_container_client(results_container)
    blob_client = container_client.get_blob_client(RESULTS_BLOB_NAME)

    try:
        download_stream = blob_client.download_blob()
        data = download_stream.readall()
        in_mem_file = io.BytesIO(data)
        workbook = load_workbook(in_mem_file)
        worksheet = workbook.active
        for row in worksheet.iter_rows(min_row=2, values_only=True):
            existing_name = row[0]
            if existing_name and str(existing_name) == cv_filename:
                return True
    except Exception:
        return False

    return False


def append_cv_scores_to_results(
    blob_service_client: BlobServiceClient,
    results_container: str,
    cv_filename: str,
    scores: List[float],
    total_score: float,
) -> None:
    """
    Append a CV row to results.xlsx in a concurrency-safe way:
      - Download + get ETag
      - If CV already present, skip
      - Append row
      - Upload with if_match=etag; retry on conflict.
    """
    container_client = blob_service_client.get_container_client(results_container)
    blob_client = container_client.get_blob_client(RESULTS_BLOB_NAME)
    max_retries = 5
    for attempt in range(max_retries):
        try:
            downloader = blob_client.download_blob()
            etag = downloader.properties.etag
            data = downloader.readall()
        except Exception as e:
            logging.error(
                "Failed to download '%s' for appending CV '%s': %s",
                RESULTS_BLOB_NAME,
                cv_filename,
                e,
            )
            return
        in_mem_file = io.BytesIO(data)
        workbook = load_workbook(in_mem_file)
        worksheet = workbook.active
        already = False
        for row in worksheet.iter_rows(min_row=2, values_only=True):
            existing_name = row[0]
            if existing_name and str(existing_name) == cv_filename:
                already = True
                break

        if already:
            logging.info(
                "CV '%s' is already present in results.xlsx; skipping append.",
                cv_filename,
            )
            return

        row = [cv_filename]
        row.extend(scores)
        row.append(total_score)
        worksheet.append(row)

        out_mem_file = io.BytesIO()
        workbook.save(out_mem_file)
        out_mem_file.seek(0)
        new_data = out_mem_file.read()

        try:
            blob_client.upload_blob(
                new_data,
                overwrite=True,
                if_match=etag,
            )
            logging.info(
                "Appended CV '%s' scores to results.xlsx (TotalScore=%.2f).",
                cv_filename,
                total_score,
            )
            return
        except Exception as e:
            logging.warning(
                "Concurrency conflict appending '%s' to results.xlsx (attempt %d/%d): %s",
                cv_filename,
                attempt + 1,
                max_retries,
                e,
            )

    logging.error(
        "Failed to append CV '%s' to results.xlsx after %d retries.",
        cv_filename,
        max_retries,
    )


def handle_zip_upload(
    blob_service_client: BlobServiceClient,
    incoming_container: str,
    blob_filename: str,
    myblob: func.InputStream,
) -> None:
    logging.info("Handling zip upload for '%s'.", blob_filename)
    zip_bytes = myblob.read()
    extracted_files = []

    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            for info in zf.infolist():
                if info.is_dir():
                    continue

                inner_name = info.filename
                logging.info(
                    "Extracting '%s' from zip '%s' into container '%s'.",
                    inner_name,
                    blob_filename,
                    incoming_container,
                )

                file_bytes = zf.read(info)
                if not file_bytes:
                    continue

                container_client = blob_service_client.get_container_client(
                    incoming_container
                )
                inner_blob_client = container_client.get_blob_client(inner_name)
                inner_blob_client.upload_blob(file_bytes, overwrite=True)
                extracted_files.append(inner_name)
    except Exception as e:
        logging.error("Failed to extract zip '%s': %s", blob_filename, e)
        return

    logging.info(
        "Finished handling zip upload '%s'; extracted %d files.",
        blob_filename,
        len(extracted_files),
    )


def get_scanned_and_pending_sets(
    blob_service_client: BlobServiceClient,
    status_container: str,
    results_container: str,
    config_container: str,
) -> Tuple[Set[str], Set[str]]:
    scanned: Set[str] = set()
    pending: Set[str] = set()

    try:
        container_client = blob_service_client.get_container_client(results_container)
        blob_client = container_client.get_blob_client(RESULTS_BLOB_NAME)
        download_stream = blob_client.download_blob()
        data = download_stream.readall()
        in_mem_file = io.BytesIO(data)
        workbook = load_workbook(in_mem_file)
        worksheet = workbook.active
        for row in worksheet.iter_rows(min_row=2, values_only=True):
            cv_name = row[0]
            if cv_name:
                scanned.add(str(cv_name))
    except Exception:
        pass

    others = read_filenames_from_status_list(
        blob_service_client, status_container, OTHERS_STATUS_BLOB
    )
    scanned.update(others)

    jd_meta = load_active_jd_metadata(blob_service_client, config_container)
    if jd_meta:
        old_jd = jd_meta.get("jd_filename")
        if old_jd:
            scanned.add(str(old_jd))

    pending = read_filenames_from_status_list(
        blob_service_client, status_container, PENDING_STATUS_BLOB
    )

    return scanned, pending


def handle_jd(
    blob_service_client: BlobServiceClient,
    blob_filename: str,
    full_text: str,
    incoming_container: str,
    status_container: str,
    results_container: str,
    config_container: str,
) -> None:
    logging.info("Handling JD '%s'.", blob_filename)

    scanned, pending = get_scanned_and_pending_sets(
        blob_service_client, status_container, results_container, config_container
    )

    inc_client = blob_service_client.get_container_client(incoming_container)
    for blob in inc_client.list_blobs():
        base_name = os.path.basename(blob.name)
        if base_name == blob_filename:
            continue
        if base_name in pending:
            continue
        if base_name in scanned:
            logging.info(
                "Deleting previously scanned blob '%s' (base '%s') from '%s'.",
                blob.name,
                base_name,
                incoming_container,
            )
            inc_client.delete_blob(blob.name)

    skills = extract_skills_from_jd(full_text)
    if not skills:
        logging.error(
            "No skills extracted from JD '%s'. JD processing will continue, "
            "but results.xlsx may not be correctly initialized.",
            blob_filename,
        )

    init_results_workbook(blob_service_client, results_container, skills)
    save_active_jd_metadata(blob_service_client, config_container, blob_filename, skills)

    logging.info(
        "JD '%s' processed. Skills: %s. results.xlsx re-initialized.",
        blob_filename,
        ", ".join(skills),
    )


def handle_cv(
    blob_service_client: BlobServiceClient,
    blob_filename: str,
    full_text: str,
    status_container: str,
    results_container: str,
    config_container: str,
) -> None:
    logging.info("Handling CV '%s'.", blob_filename)

    # Idempotency: skip if this CV already has a row in results.xlsx
    if cv_already_scored(blob_service_client, results_container, blob_filename):
        logging.info(
            "CV '%s' is already scored in results.xlsx; skipping re-processing.",
            blob_filename,
        )
        return

    jd_meta = load_active_jd_metadata(blob_service_client, config_container)
    jd_name = None
    if jd_meta:
        jd_name = jd_meta.get("jd_filename")
    if not jd_meta or not jd_name:
        logging.warning(
            "No valid active JD found; CV '%s' will be marked as pending.", blob_filename
        )
        append_filename_to_status_list(
            blob_service_client, status_container, PENDING_STATUS_BLOB, blob_filename
        )
        return

    skills = read_skills_from_results_header(blob_service_client, results_container)
    if not skills or len(skills) != 10:
        logging.error(
            "Could not read 10 skills from results.xlsx header; found: %s. "
            "CV '%s' will not be scored.",
            skills,
            blob_filename,
        )
        return

    scores, total_score = score_cv_against_skills(full_text, skills)

    append_cv_scores_to_results(
        blob_service_client, results_container, blob_filename, scores, total_score
    )

    logging.info(
        "CV '%s' scored against active JD '%s'. TotalScore=%.2f.",
        blob_filename,
        jd_name,
        total_score,
    )


def handle_other(
    blob_service_client: BlobServiceClient,
    blob_filename: str,
    status_container: str,
) -> None:
    logging.info(
        "File '%s' was classified as OTHER. Adding to others.txt.", blob_filename
    )
    append_filename_to_status_list(
        blob_service_client, status_container, OTHERS_STATUS_BLOB, blob_filename
    )


@app.function_name(name="******")
@app.blob_trigger(
    arg_name="myblob",
    path="incoming/{name}",
    connection="DATA_STORAGE_CONNECTION",
)
def pdfprocessor(myblob: func.InputStream) -> None:
    logging.info(
        "Python Blob trigger function processed blob\n"
        "Name: %s\n"
        "Size: %d bytes",
        myblob.name,
        myblob.length,
    )
    blob_path = myblob.name
    blob_filename = os.path.basename(blob_path)

    blob_service_client = get_blob_service_client()
    incoming_container = os.environ.get("BLOB_INCOMING_CONTAINER", "incoming")
    status_container = os.environ.get("BLOB_STATUS_CONTAINER", "status")
    results_container = os.environ.get("BLOB_RESULTS_CONTAINER", "results")
    config_container = os.environ.get("BLOB_CONFIG_CONTAINER", "config")

    if blob_filename.lower().endswith(".zip"):
        handle_zip_upload(
            blob_service_client,
            incoming_container,
            blob_filename,
            myblob,
        )
        return

    try:
        full_text = extract_text_from_pdf(myblob)
    except Exception as e:
        logging.error("Failed to extract text from '%s': %s", blob_filename, e)
        full_text = ""

    if full_text.strip():
        label = classify_document(full_text)
    else:
        logging.warning(
            "No text extracted from '%s'; classifying as OTHER by default.",
            blob_filename,
        )
        label = "OTHER"

    logging.info("Document '%s' classified as '%s'.", blob_filename, label)

    if label == "JD":
        handle_jd(
            blob_service_client,
            blob_filename,
            full_text,
            incoming_container,
            status_container,
            results_container,
            config_container,
        )
    elif label == "CV":
        handle_cv(
            blob_service_client,
            blob_filename,
            full_text,
            status_container,
            results_container,
            config_container,
        )
    else:
        handle_other(blob_service_client, blob_filename, status_container)