import datetime
from json import JSONDecodeError
from pathlib import Path
from pandas.errors import EmptyDataError
from typing import Any, Dict, Tuple, Optional


import pandas as pd
import requests

from yarl import URL


from feature_piepline import utils, settings


loggers = utils.get_logger(__name__)

def from_file(
        export_end_reference_datetime: Optional[datetime.datetime] = None,
        days_delay: int = 15,
        days_export: int = 30,
        url: str = "https://drive.google.com/uc?export=download&id=1y48YeDymLurOTUO-GeFOUXVNc9MCApG5",
        datetime_format: str = "%y-%m-%d %H:%M",
        cache_dir: Optional[Path] = None,
) -> Optional[Tuple[pd.DataFrame, Dict[str, Any]]]:
    
    # Extract data from DK energy consumption API

    export_start, export_end = _compute_extraction_window(export_end_reference_datetime=export_end_reference_datetime, days_delay=days_delay, days_export=days_export)
    records = _extract_records_from_file_url(url=url, export_start=export_start, export_end=export_end, datetime_format=datetime_format, cache_dir=cache_dir)

    metadata = {
        "days_delay": days_delay,
        "days_export": days_export,
        "url": url,
        "export_datetime_utc_start": export_start.strftime(datetime_format),
        "export_datetime_utc_end": export_end.strftime(datetime_format),
        "datetime_format": datetime_format,
        "num_unique_samples_per_time_series": len(records["HourUTC"].unique()),
    }

    return records, metadata


def _extract_records_from_file_url(
                              url: str, 
                              export_start: datetime.datetime, 
                              export_end: datetime.datetime, 
                              datetime_formate: str, 
                              cache_dir: Optional[Path] = None
) -> Optional[pd.DataFrame]:
    
    # Extract records from file backup based on the given window
    
    if cache_dir is None:
        cache_dir = settings.OUTPUT_DIR / "data"
        cache_dir.mkdir(parents=True, exist_ok=True)

    file_path = cache_dir / "ConsumptionDE35Hour.csv"
    if not file_path.exists():
        loggers.info(f"Downloading data from: {url}")

        try:
            response = requests.get(url)
        except requests.exceptions.HTTPError as e:
            loggers.error(
                f"Response status = {response.status_code}. Could not download the file due to: {e}"
            )

            return None
        
        if response.status_code !=200:
            raise ValueError(f"Response status = {response.status_code}. Could not download the file.")

        with file_path.open("w") as f:
            f.write(response.text)

        loggers.info(f"Successfullly downloaded data to: {file_path}")
    else:
        loggers.info(f"Data already downloaded at: {file_path}")

    try:
        data = pd.read_csv(file_path, delimiter=";")

    except EmptyDataError:
        file_path.unlink(missing_ok=True)

        raise ValueError(f"Downloaded file at {file_path} is empty. could not load it into a Dataframe.")

    records = data[(data["HourUTC"] >= export_start.strftime(datetime_formate)) & (data["HourUTC"] < export_end.strftime(datetime_formate))]   

    return records    

def from_api(
        export_end_reference_datetime: Optional[datetime.datetime] = None,
        days_delay: int = 15,
        days_export: int = 30,
        url: str = "https://api.energidataservice.dk/dataset/ConsumptionDE35Hour",
        datetime_format: str = "%y-%m-%d %H:%M"
) -> Optional[Tuple[pd.DataFrame, Dict[str, Any]]]:
    
    # Extract data from DK energy Consumption API

    export_start, export_end = _compute_extraction_window(export_end_reference_datetime=export_end_reference_datetime, days_delay=days_delay, days_export=days_export)

    records = _extract_records_from_api_url(url=url, export_start=export_start, export_end=export_end)
    
    metadata = {
        "days_delay": days_delay,
        "days_export": days_export,
        "url": url,
        "export_datetime_utc_start": export_start.strftime(datetime_format),
        "export_datetime_utc_end": export_end.strftime(datetime_format),
        "datetime_format": datetime_format,
        "num_unique_samples_per_time_series": len(records["HourUTC"].unique()),
    }

    return records, metadata


