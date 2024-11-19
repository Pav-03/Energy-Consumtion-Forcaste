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
    
    # Extract data from DK energy Consumption API thatbstore in google drive 

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


def _extract_records_from_api_url(url: str, export_start: datetime.datetime, export_end: datetime.datetime):

    # exctract data from official API based o given export window.

    query_params = {
        "offset": 0,
        "sort": "HourUTC",
        "timezone": "utc",
        "start": export_start.strftime("%y-%m-%dT%H:%M"),
        "end": export_end.strftime("%y-%m-%dT%H:%M"),
    }

    url = URL(url) % query_params
    url = str(url)
    loggers.info(f"requesting data from api with URL: {url}")
    response = requests.get(url)
    loggers.info(f"Response received from API with status code: {response.status_code}")

    # Parse the data

    try:
        response = response.json()
    except JSONDecodeError:
        loggers.error(f"response status = {response.status_code}. Could not decode response from API with URL: {url}")

        return None
    
    Records = response["records"]
    Records = pd.DataFrame.from_records(Records)




def _compute_extraction_window(
        exort_end_reference_datetime: datetime.datetime, 
        days_delay: int, 
        days_export: int
) -> Tuple[datetime.datetime, datetime.datetime]:
    
    # """Compute the extraction window relative to 'export_end_reference_datetime' and take into consideration the maximum and minimum data points available in the dataset."""

    if export_end_reference_datetime is None:
        # As the dataset expired in July 2023, we set the export end reference datetime to the last day of June 2023 + the delay.
        export_end_reference_datetime = datetime.datetime(
            2023, 6, 30, 21, 0, 0
        ) + datetime.timedelta(days=days_delay)
        export_end_reference_datetime = export_end_reference_datetime.replace(
            minute=0, second=0, microsecond=0
        )
    else:
        export_end_reference_datetime = export_end_reference_datetime.replace(
            minute=0, second=0, microsecond=0
        )

    # TODO: Change the API source, until then we have to clamp the export_end_reference_datetime to the last day of June 2023 to simulate the same behavior.
    expiring_dataset_datetime = datetime.datetime(2023, 6, 30, 21, 0, 0) + datetime.timedelta(
        days=days_delay
    )
    if export_end_reference_datetime > expiring_dataset_datetime:
        export_end_reference_datetime = expiring_dataset_datetime

        loggers.warning(
            "We clapped 'export_end_reference_datetime' to 'datetime(2023, 6, 30) + datetime.timedelta(days=days_delay)' as \
        the dataset will not be updated starting from July 2023. The dataset will expire during 2023. \
        Check out the following link for more information: https://www.energidataservice.dk/tso-electricity/ConsumptionDE35Hour"
        )

    export_end = export_end_reference_datetime - datetime.timedelta(days=days_delay)
    export_start = export_end_reference_datetime - datetime.timedelta(
        days=days_delay + days_export
    )

    min_export_start = datetime.datetime(2020, 6, 30, 22, 0, 0)
    if export_start < min_export_start:
        export_start = min_export_start
        export_end = export_start + datetime.timedelta(days=days_export)

        loggers.warning(
            "We clapped 'export_start' to 'datetime(2020, 6, 30, 22, 0, 0)' and 'export_end' to 'export_start + datetime.timedelta(days=days_export)' as this is the latest window available in the dataset."
        )

    return export_start, export_end




