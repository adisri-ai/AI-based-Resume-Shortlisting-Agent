import os
import logging
CONFIG_PATH = os.environ.get("CONFIG_PATH", "/config/config.txt")
class ConfigService: 
    @staticmethod
    def load_config():
        if not os.path.exists(CONFIG_PATH):
            logging.warning("config.txt not found.")
            return
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                os.environ[key.strip()] = value.strip()
        RESULTS_BLOB_NAME       = os.environ.get("RESULTS_BLOB_NAME",       "results.xlsx")
        ACTIVE_JD_BLOB_NAME     = os.environ.get("ACTIVE_JD_BLOB_NAME",     "active-jd.json")
        OTHERS_STATUS_BLOB_NAME = os.environ.get("OTHERS_STATUS_BLOB_NAME", "others.txt")
        PENDING_STATUS_BLOB_NAME = os.environ.get("PENDING_STATUS_BLOB_NAME", "pending.txt")
        return RESULTS_BLOB_NAME , ACTIVE_JD_BLOB_NAME , OTHERS_STATUS_BLOB_NAME , PENDING_STATUS_BLOB_NAME