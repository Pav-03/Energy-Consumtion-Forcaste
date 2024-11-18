import os
from pathlib import Path
from typing import Union


from dotenv import load_dotenv


def load_env_var(root_dir: Union[str, Path]) -> dict:
    # load environment variables from .env.default and .env files

    if isinstance(root_dir, str):
        root_dir = Path(root_dir)

    load_dotenv(dotenv_path=root_dir / ".env.default")
    load_dotenv(dotenv_path=root_dir / ".env", override=True)

    return dict(os.environ)


def get_root_dir(default_value: str = ".") -> Path:
    # get the root directory of the project 

    return Path(os.getenv("ML_PIPELINE_ROOT_DIR", default_value))

ML_PIPELINE_ROOT_DIR = get_root_dir()
OUTPUT_DIR = ML_PIPELINE_ROOT_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

SETTINGS = load_env_var(root_dir=ML_PIPELINE_ROOT_DIR)