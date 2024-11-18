import json
import logging
from pathlib import Path

from feature_piepline import settings

def get_logger(name: str) -> logging.Logger:
    # Template for getting logger

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(name)

    return logger

def save_json(data: dict, file_name: str, save_dir: str = settings.OUTPUT_DIR):
    # Save directory as json

    data_path = Path(save_dir) / file_name
    with open(data_path, "w") as f:
        json.dump(data, f)

def load_json(file_name: str, save_dir: str = settings.OUTPUT_DIR) -> dict:
    # Load json file 

    data_path = Path(save_dir) / file_name

    if not data_path.exists():
        raise FileNotFoundError(f"Cached json from {data_path} does not exist.")
    
    with open(data_path, "r") as f:
        return json.load()