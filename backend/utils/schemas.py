from pydantic import BaseModel
from typing import Optional
class OpenAIConfigBody(BaseModel):
    OPENAI_ENDPOINT:         Optional[str] = None
    OPENAI_API_KEY:          Optional[str] = None
    OPENAI_API_VERSION:      Optional[str] = None
    OPENAI_DEPLOYMENT_NAME:  Optional[str] = None
    BLOB_INCOMING_CONTAINER: Optional[str] = None
    BLOB_RESULTS_CONTAINER:  Optional[str] = None
    BLOB_CONFIG_CONTAINER:   Optional[str] = None
    BLOB_STATUS_CONTAINER:   Optional[str] = None
class UploadSasBody(BaseModel):
    filename: str